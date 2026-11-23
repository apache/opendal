// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context as TaskContext, Poll};
use std::time::SystemTime;

use anyhow::{Context, Result};
use opendal::raw::{self, oio};
use opendal::{OperationContext, Operator, services};
use tempfile::TempDir;
use tokio::io::{AsyncRead, ReadBuf};
use unftp_core::auth::DefaultUser;
use unftp_core::storage::{ErrorKind, Metadata, StorageBackend};
use unftp_sbe_opendal::OpendalStorage;

/// Test layer that injects a fixed [`opendal::ErrorKind`] for `stat` calls on
/// one path and can hide the `list` capability, emulating backends whose
/// `stat` fails for reasons other than a missing path.
#[derive(Debug)]
struct StatErrorLayer {
    injected_path: &'static str,
    injected_kind: opendal::ErrorKind,
    hide_list: bool,
}

impl raw::Layer for StatErrorLayer {
    fn apply_service(&self, srv: raw::Servicer) -> raw::Servicer {
        Arc::new(StatErrorService {
            inner: srv,
            injected_path: self.injected_path,
            injected_kind: self.injected_kind,
            hide_list: self.hide_list,
        })
    }
}

#[derive(Debug)]
struct StatErrorService {
    inner: raw::Servicer,
    injected_path: &'static str,
    injected_kind: opendal::ErrorKind,
    hide_list: bool,
}

impl raw::Service for StatErrorService {
    type Reader = oio::Reader;
    type Writer = oio::Writer;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;
    type Copier = oio::Copier;
    type Composer = oio::Composer;

    fn info(&self) -> raw::ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> opendal::Capability {
        let mut capability = self.inner.capability();
        capability.list &= !self.hide_list;
        capability
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: raw::OpCreateDir,
    ) -> opendal::Result<raw::RpCreateDir> {
        self.inner.create_dir(ctx, path, args).await
    }

    async fn stat(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: raw::OpStat,
    ) -> opendal::Result<raw::RpStat> {
        if path == self.injected_path {
            return Err(opendal::Error::new(
                self.injected_kind,
                "stat error injected by the test",
            ));
        }
        self.inner.stat(ctx, path, args).await
    }

    fn read(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: raw::OpRead,
    ) -> opendal::Result<Self::Reader> {
        self.inner.read(ctx, path, args)
    }

    fn write(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: raw::OpWrite,
    ) -> opendal::Result<Self::Writer> {
        self.inner.write(ctx, path, args)
    }

    fn delete(&self, ctx: &OperationContext) -> opendal::Result<Self::Deleter> {
        self.inner.delete(ctx)
    }

    fn list(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: raw::OpList,
    ) -> opendal::Result<Self::Lister> {
        self.inner.list(ctx, path, args)
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: raw::OpCopy,
    ) -> opendal::Result<Self::Copier> {
        self.inner.copy(ctx, from, to, args)
    }

    fn compose(
        &self,
        ctx: &OperationContext,
        to: &str,
        args: raw::OpCompose,
    ) -> opendal::Result<Self::Composer> {
        self.inner.compose(ctx, to, args)
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: raw::OpRename,
    ) -> opendal::Result<raw::RpRename> {
        self.inner.rename(ctx, from, to, args).await
    }

    async fn restore(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: raw::OpRestore,
    ) -> opendal::Result<raw::RpRestore> {
        self.inner.restore(ctx, path, args).await
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: raw::OpPresign,
    ) -> opendal::Result<raw::RpPresign> {
        self.inner.presign(ctx, path, args).await
    }
}

/// Test layer that counts `stat` calls and can optionally hide the `stat`
/// capability, attach a `last_modified` timestamp to `stat` responses, or
/// strip `last_modified` from listed file entries to simulate backends whose
/// lister returns file mode and size without timestamps.
#[derive(Debug)]
struct StatObserveLayer {
    stat_count: Arc<AtomicUsize>,
    hide_stat: bool,
    stat_last_modified: Option<raw::Timestamp>,
    strip_list_last_modified: bool,
}

impl raw::Layer for StatObserveLayer {
    fn apply_service(&self, srv: raw::Servicer) -> raw::Servicer {
        Arc::new(StatObserveService {
            inner: srv,
            stat_count: self.stat_count.clone(),
            hide_stat: self.hide_stat,
            stat_last_modified: self.stat_last_modified,
            strip_list_last_modified: self.strip_list_last_modified,
        })
    }
}

#[derive(Debug)]
struct StatObserveService {
    inner: raw::Servicer,
    stat_count: Arc<AtomicUsize>,
    hide_stat: bool,
    stat_last_modified: Option<raw::Timestamp>,
    strip_list_last_modified: bool,
}

struct StripLastModifiedLister {
    inner: oio::Lister,
}

impl oio::List for StripLastModifiedLister {
    async fn next(&mut self) -> opendal::Result<Option<oio::Entry>> {
        let Some(entry) = self.inner.next().await? else {
            return Ok(None);
        };
        let (path, meta) = entry.into_parts();
        if meta.is_dir() {
            return Ok(Some(oio::Entry::new(&path, meta)));
        }
        let meta = opendal::MetadataBuilder::file(meta.content_length()).build();
        Ok(Some(oio::Entry::new(&path, meta)))
    }
}

impl raw::Service for StatObserveService {
    type Reader = oio::Reader;
    type Writer = oio::Writer;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;
    type Copier = oio::Copier;
    type Composer = oio::Composer;

    fn info(&self) -> raw::ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> opendal::Capability {
        let mut capability = self.inner.capability();
        capability.stat &= !self.hide_stat;
        capability
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: raw::OpCreateDir,
    ) -> opendal::Result<raw::RpCreateDir> {
        self.inner.create_dir(ctx, path, args).await
    }

    async fn stat(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: raw::OpStat,
    ) -> opendal::Result<raw::RpStat> {
        self.stat_count.fetch_add(1, Ordering::SeqCst);
        let rp = self.inner.stat(ctx, path, args).await?;
        if let Some(ts) = self.stat_last_modified {
            let mut builder = rp.into_metadata().into_builder();
            builder.last_modified(ts);
            Ok(raw::RpStat::new(builder.build()))
        } else {
            Ok(rp)
        }
    }

    fn read(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: raw::OpRead,
    ) -> opendal::Result<Self::Reader> {
        self.inner.read(ctx, path, args)
    }

    fn write(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: raw::OpWrite,
    ) -> opendal::Result<Self::Writer> {
        self.inner.write(ctx, path, args)
    }

    fn delete(&self, ctx: &OperationContext) -> opendal::Result<Self::Deleter> {
        self.inner.delete(ctx)
    }

    fn list(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: raw::OpList,
    ) -> opendal::Result<Self::Lister> {
        let inner = self.inner.list(ctx, path, args)?;
        if self.strip_list_last_modified {
            Ok(Box::new(StripLastModifiedLister { inner }))
        } else {
            Ok(inner)
        }
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: raw::OpCopy,
    ) -> opendal::Result<Self::Copier> {
        self.inner.copy(ctx, from, to, args)
    }

    fn compose(
        &self,
        ctx: &OperationContext,
        to: &str,
        args: raw::OpCompose,
    ) -> opendal::Result<Self::Composer> {
        self.inner.compose(ctx, to, args)
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: raw::OpRename,
    ) -> opendal::Result<raw::RpRename> {
        self.inner.rename(ctx, from, to, args).await
    }

    async fn restore(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: raw::OpRestore,
    ) -> opendal::Result<raw::RpRestore> {
        self.inner.restore(ctx, path, args).await
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: raw::OpPresign,
    ) -> opendal::Result<raw::RpPresign> {
        self.inner.presign(ctx, path, args).await
    }
}

#[derive(Debug)]
struct CloseErrorLayer {
    aborted: Arc<AtomicBool>,
}

impl raw::Layer for CloseErrorLayer {
    fn apply_service(&self, srv: raw::Servicer) -> raw::Servicer {
        Arc::new(CloseErrorService {
            inner: srv,
            aborted: self.aborted.clone(),
        })
    }
}

#[derive(Debug)]
struct CloseErrorService {
    inner: raw::Servicer,
    aborted: Arc<AtomicBool>,
}

impl raw::Service for CloseErrorService {
    type Reader = ();
    type Writer = oio::Writer;
    type Lister = ();
    type Deleter = ();
    type Copier = ();
    type Composer = ();

    fn info(&self) -> raw::ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> opendal::Capability {
        self.inner.capability()
    }

    async fn create_dir(
        &self,
        _: &OperationContext,
        _: &str,
        _: raw::OpCreateDir,
    ) -> opendal::Result<raw::RpCreateDir> {
        Err(opendal::Error::new(
            opendal::ErrorKind::Unsupported,
            "not used by this test",
        ))
    }

    async fn stat(
        &self,
        _: &OperationContext,
        _: &str,
        _: raw::OpStat,
    ) -> opendal::Result<raw::RpStat> {
        Err(opendal::Error::new(
            opendal::ErrorKind::Unsupported,
            "not used by this test",
        ))
    }

    fn read(&self, _: &OperationContext, _: &str, _: raw::OpRead) -> opendal::Result<Self::Reader> {
        Err(opendal::Error::new(
            opendal::ErrorKind::Unsupported,
            "not used by this test",
        ))
    }

    fn write(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: raw::OpWrite,
    ) -> opendal::Result<Self::Writer> {
        let inner = self.inner.write(ctx, path, args)?;
        Ok(Box::new(CloseErrorWriter {
            inner,
            aborted: self.aborted.clone(),
        }))
    }

    fn delete(&self, _: &OperationContext) -> opendal::Result<Self::Deleter> {
        Err(opendal::Error::new(
            opendal::ErrorKind::Unsupported,
            "not used by this test",
        ))
    }

    fn list(&self, _: &OperationContext, _: &str, _: raw::OpList) -> opendal::Result<Self::Lister> {
        Err(opendal::Error::new(
            opendal::ErrorKind::Unsupported,
            "not used by this test",
        ))
    }

    fn copy(
        &self,
        _: &OperationContext,
        _: &str,
        _: &str,
        _: raw::OpCopy,
    ) -> opendal::Result<Self::Copier> {
        Err(opendal::Error::new(
            opendal::ErrorKind::Unsupported,
            "not used by this test",
        ))
    }

    async fn rename(
        &self,
        _: &OperationContext,
        _: &str,
        _: &str,
        _: raw::OpRename,
    ) -> opendal::Result<raw::RpRename> {
        Err(opendal::Error::new(
            opendal::ErrorKind::Unsupported,
            "not used by this test",
        ))
    }

    async fn presign(
        &self,
        _: &OperationContext,
        _: &str,
        _: raw::OpPresign,
    ) -> opendal::Result<raw::RpPresign> {
        Err(opendal::Error::new(
            opendal::ErrorKind::Unsupported,
            "not used by this test",
        ))
    }
}

struct CloseErrorWriter {
    inner: oio::Writer,
    aborted: Arc<AtomicBool>,
}

impl oio::Write for CloseErrorWriter {
    async fn write(&mut self, bs: opendal::Buffer) -> opendal::Result<()> {
        oio::Write::write(&mut self.inner, bs).await
    }

    async fn close(&mut self) -> opendal::Result<opendal::Metadata> {
        Err(opendal::Error::new(
            opendal::ErrorKind::PermissionDenied,
            "injected close failure",
        ))
    }

    async fn abort(&mut self) -> opendal::Result<()> {
        self.aborted.store(true, Ordering::SeqCst);
        let _ = oio::Write::abort(&mut self.inner).await;
        Err(opendal::Error::new(
            opendal::ErrorKind::Unexpected,
            "injected abort failure",
        ))
    }
}

fn fs_storage() -> Result<(TempDir, OpendalStorage)> {
    let root = tempfile::tempdir()?;
    let fs = services::Fs::default().root(root.path().to_str().context("non-UTF-8 test root")?);
    Ok((root, OpendalStorage::new(Operator::new(fs)?)))
}

async fn read_file(storage: &OpendalStorage, path: &str) -> Result<String> {
    let mut reader = storage.get(&DefaultUser, path, 0).await.context("get")?;
    let mut content = String::new();
    tokio::io::AsyncReadExt::read_to_string(&mut reader, &mut content).await?;
    Ok(content)
}

/// An input that yields `remaining` bytes and then fails, to exercise aborted puts.
struct FailingReader {
    remaining: usize,
}

impl AsyncRead for FailingReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.remaining == 0 {
            return Poll::Ready(Err(io::Error::other("injected read failure")));
        }
        let n = self.remaining.min(buf.remaining());
        buf.put_slice(&vec![b'x'; n]);
        self.remaining -= n;
        Poll::Ready(Ok(()))
    }
}

/// `start_pos == 0` stores the input verbatim, whether the file exists or not.
#[tokio::test]
async fn put_overwrites_at_start_position_zero() -> Result<()> {
    let (_root, storage) = fs_storage()?;
    let user = DefaultUser;

    storage
        .put(&user, "abc".as_bytes(), "file", 0)
        .await
        .context("put abc")?;
    storage
        .put(&user, "def".as_bytes(), "file", 0)
        .await
        .context("put def")?;

    assert_eq!(read_file(&storage, "file").await?, "def");
    Ok(())
}

/// `start_pos` equal to the current file size appends on an append-capable
/// backend, which is how libunftp implements `APPE`.
#[tokio::test]
async fn put_appends_at_end_of_file() -> Result<()> {
    let root = tempfile::tempdir()?;
    let fs = services::Fs::default().root(root.path().to_str().context("non-UTF-8 test root")?);
    let op = Operator::new(fs)?;
    assert!(op.info().capability().write_can_append);
    let storage = OpendalStorage::new(op);
    let user = DefaultUser;

    storage
        .put(&user, "abc".as_bytes(), "file", 0)
        .await
        .context("put abc")?;
    storage
        .put(&user, "def".as_bytes(), "file", 3)
        .await
        .context("put def at start_pos 3")?;

    assert_eq!(read_file(&storage, "file").await?, "abcdef");
    Ok(())
}

/// A nonzero `start_pos` that does not match the file size must fail instead
/// of overwriting the file.
#[tokio::test]
async fn put_rejects_offset_past_end_of_file() -> Result<()> {
    let (_root, storage) = fs_storage()?;
    let user = DefaultUser;

    storage
        .put(&user, "abc".as_bytes(), "file", 0)
        .await
        .context("put abc")?;

    for start_pos in [1, 100] {
        let err = storage
            .put(&user, "def".as_bytes(), "file", start_pos)
            .await
            .expect_err("put with mismatched start_pos must fail");
        assert_eq!(err.kind(), ErrorKind::PermanentFileNotAvailable);
        assert_eq!(
            read_file(&storage, "file").await?,
            "abc",
            "file must be preserved after rejected put at start_pos {start_pos}"
        );
    }
    Ok(())
}

/// Without `write_can_append` a nonzero `start_pos` is reported as
/// unimplemented and leaves the file untouched.
#[tokio::test]
async fn put_rejects_offset_on_non_append_backend() -> Result<()> {
    let op = Operator::new(services::Memory::default())?;
    assert!(!op.info().capability().write_can_append);
    let storage = OpendalStorage::new(op);
    let user = DefaultUser;

    storage
        .put(&user, "abc".as_bytes(), "file", 0)
        .await
        .context("put abc")?;

    let err = storage
        .put(&user, "def".as_bytes(), "file", 3)
        .await
        .expect_err("put at start_pos 3 must fail on a non-append backend");
    assert_eq!(err.kind(), ErrorKind::CommandNotImplemented);
    assert_eq!(read_file(&storage, "file").await?, "abc");
    Ok(())
}

/// A read failure after data was written must abort the writer instead of
/// committing a truncated object.
#[tokio::test]
async fn put_read_error_preserves_existing_content() -> Result<()> {
    let storage = OpendalStorage::new(Operator::new(services::Memory::default())?);
    let user = DefaultUser;

    storage
        .put(&user, "old".as_bytes(), "file", 0)
        .await
        .context("put old")?;

    let err = storage
        .put(
            &user,
            FailingReader {
                remaining: 300 * 1024,
            },
            "file",
            0,
        )
        .await
        .expect_err("put must fail when the input read fails");
    assert_eq!(err.kind(), ErrorKind::LocalError);
    assert_eq!(read_file(&storage, "file").await?, "old");
    Ok(())
}

/// A failed put must not leave a partial object behind when none existed.
#[tokio::test]
async fn put_read_error_does_not_create_file() -> Result<()> {
    let storage = OpendalStorage::new(Operator::new(services::Memory::default())?);
    let user = DefaultUser;

    let err = storage
        .put(
            &user,
            FailingReader {
                remaining: 300 * 1024,
            },
            "file",
            0,
        )
        .await
        .expect_err("put must fail when the input read fails");
    assert_eq!(err.kind(), ErrorKind::LocalError);
    assert!(storage.metadata(&user, "file").await.is_err());
    Ok(())
}

/// Filesystem writes can abort cleanly when they are staged for atomic commit.
#[tokio::test]
async fn put_read_error_preserves_existing_content_on_fs() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().join("root");
    let staging = temp.path().join("staging");
    std::fs::create_dir(&root)?;
    std::fs::create_dir(&staging)?;
    let fs = services::Fs::default()
        .root(root.to_str().context("non-UTF-8 test root")?)
        .atomic_write_dir(staging.to_str().context("non-UTF-8 staging dir")?);
    let storage = OpendalStorage::new(Operator::new(fs)?);
    let user = DefaultUser;

    storage
        .put(&user, "old".as_bytes(), "file", 0)
        .await
        .context("put old")?;

    let err = storage
        .put(
            &user,
            FailingReader {
                remaining: 300 * 1024,
            },
            "file",
            0,
        )
        .await
        .expect_err("put must fail when the input read fails");
    assert_eq!(err.kind(), ErrorKind::LocalError);
    assert_eq!(read_file(&storage, "file").await?, "old");
    Ok(())
}

/// `rmd` must delete only the named directory. A prefix-based service would
/// otherwise also remove siblings such as `dd/`.
#[tokio::test]
async fn rmd_deletes_only_the_named_directory() -> Result<()> {
    let storage = OpendalStorage::new(Operator::new(services::Memory::default())?);
    let user = DefaultUser;

    for (path, content) in [("d/a", "a"), ("d/sub/x", "x"), ("dd/b", "b")] {
        storage
            .put(&user, content.as_bytes(), path, 0)
            .await
            .with_context(|| format!("put {path}"))?;
    }

    storage.rmd(&user, "d").await.context("rmd d")?;

    assert!(storage.metadata(&user, "d/a").await.is_err());
    assert!(storage.metadata(&user, "d/sub/x").await.is_err());
    assert_eq!(read_file(&storage, "dd/b").await?, "b");

    // A trailing slash identifies the same directory and remains scoped.
    storage
        .put(&user, "a".as_bytes(), "d/a", 0)
        .await
        .context("restore d/a")?;
    storage.rmd(&user, "d/").await.context("rmd d/")?;
    assert!(storage.metadata(&user, "d/a").await.is_err());
    assert_eq!(read_file(&storage, "dd/b").await?, "b");
    Ok(())
}

#[tokio::test]
async fn rmd_rejects_root_missing_and_file_paths() -> Result<()> {
    let storage = OpendalStorage::new(Operator::new(services::Memory::default())?);
    let user = DefaultUser;

    storage
        .put(&user, "keep".as_bytes(), "file", 0)
        .await
        .context("put file")?;

    for path in ["/", ""] {
        let err = storage
            .rmd(&user, path)
            .await
            .expect_err("rmd of the root must fail");
        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
    }

    for path in ["missing", "file"] {
        let err = storage
            .rmd(&user, path)
            .await
            .expect_err("rmd of a non-directory must fail");
        assert_eq!(err.kind(), ErrorKind::PermanentDirectoryNotAvailable);
    }

    assert_eq!(read_file(&storage, "file").await?, "keep");
    Ok(())
}

#[tokio::test]
async fn cwd_on_filesystem_backend() -> Result<()> {
    let (_root, storage) = fs_storage()?;
    let user = DefaultUser;

    storage.cwd(&user, "/").await.context("cwd /")?;
    storage.cwd(&user, "").await.context("cwd empty path")?;

    storage.mkd(&user, "dir").await.context("mkd dir")?;
    storage.cwd(&user, "dir").await.context("cwd dir")?;
    storage.cwd(&user, "dir/").await.context("cwd dir/")?;

    storage
        .put(&user, "abc".as_bytes(), "file", 0)
        .await
        .context("put file")?;
    let err = storage
        .cwd(&user, "file")
        .await
        .expect_err("cwd file must fail");
    assert_eq!(err.kind(), ErrorKind::PermanentDirectoryNotAvailable);

    let err = storage
        .cwd(&user, "nonexistent")
        .await
        .expect_err("cwd nonexistent must fail");
    assert_eq!(err.kind(), ErrorKind::PermanentDirectoryNotAvailable);
    Ok(())
}

/// Memory keeps no directory entries of its own, so a directory only exists
/// implicitly through its children. `cwd` must still accept it while
/// rejecting files and absent paths, like on object stores.
#[tokio::test]
async fn cwd_on_memory_backend() -> Result<()> {
    let storage = OpendalStorage::new(Operator::new(services::Memory::default())?);
    let user = DefaultUser;

    storage.cwd(&user, "/").await.context("cwd /")?;
    storage.cwd(&user, "").await.context("cwd empty path")?;

    storage
        .put(&user, "abc".as_bytes(), "dir/file", 0)
        .await
        .context("put dir/file")?;
    storage
        .cwd(&user, "dir")
        .await
        .context("cwd into implicit dir")?;
    storage
        .cwd(&user, "dir/")
        .await
        .context("cwd into implicit dir/")?;

    storage
        .put(&user, "abc".as_bytes(), "file", 0)
        .await
        .context("put file")?;
    let err = storage
        .cwd(&user, "file")
        .await
        .expect_err("cwd file must fail");
    assert_eq!(err.kind(), ErrorKind::PermanentDirectoryNotAvailable);

    let err = storage
        .cwd(&user, "nonexistent")
        .await
        .expect_err("cwd nonexistent must fail");
    assert_eq!(err.kind(), ErrorKind::PermanentDirectoryNotAvailable);
    Ok(())
}

/// `stat` failures that do not mean "missing path" must propagate their
/// real error instead of being swallowed into
/// `PermanentDirectoryNotAvailable`, on list-capable and list-less
/// backends alike.
#[tokio::test]
async fn cwd_propagates_stat_errors() -> Result<()> {
    let user = DefaultUser;

    for hide_list in [false, true] {
        for (injected_kind, expected_kind) in [
            (
                opendal::ErrorKind::PermissionDenied,
                ErrorKind::PermissionDenied,
            ),
            (opendal::ErrorKind::Unexpected, ErrorKind::LocalError),
            (
                opendal::ErrorKind::NotFound,
                ErrorKind::PermanentDirectoryNotAvailable,
            ),
        ] {
            let op = Operator::new(services::Memory::default())?.layer(StatErrorLayer {
                injected_path: "forbidden/",
                injected_kind,
                hide_list,
            });
            let storage = OpendalStorage::new(op);

            let err = storage
                .cwd(&user, "forbidden")
                .await
                .expect_err("cwd forbidden must fail");
            assert_eq!(
                err.kind(),
                expected_kind,
                "hide_list={hide_list}, injected stat error: {injected_kind}"
            );
        }
    }
    Ok(())
}

/// Backends without the list capability cannot probe implicit prefix
/// directories; `cwd` must still report `PermanentDirectoryNotAvailable`
/// instead of leaking an internal `LocalError`.
#[tokio::test]
async fn cwd_on_backend_without_list_capability() -> Result<()> {
    let root = tempfile::tempdir()?;
    let cacache =
        services::Cacache::default().datadir(root.path().to_str().context("non-UTF-8 test root")?);
    let op = Operator::new(cacache)?;
    assert!(!op.info().capability().list);
    let storage = OpendalStorage::new(op);
    let user = DefaultUser;

    storage.cwd(&user, "/").await.context("cwd /")?;

    storage
        .put(&user, "abc".as_bytes(), "file", 0)
        .await
        .context("put file")?;
    let err = storage
        .cwd(&user, "file")
        .await
        .expect_err("cwd file must fail");
    assert_eq!(err.kind(), ErrorKind::PermanentDirectoryNotAvailable);

    let err = storage
        .cwd(&user, "nonexistent")
        .await
        .expect_err("cwd nonexistent must fail");
    assert_eq!(err.kind(), ErrorKind::PermanentDirectoryNotAvailable);
    Ok(())
}

/// A directory created with `mkd` must be found by `metadata` even when the
/// backend only reports it for the trailing-slash form.
#[tokio::test]
async fn metadata_returns_directory_after_mkd() -> Result<()> {
    let root = tempfile::tempdir()?;
    let fs = services::Fs::default().root(root.path().to_str().context("non-UTF-8 test root")?);
    let op = Operator::new(fs)?.layer(StatErrorLayer {
        injected_path: "sub",
        injected_kind: opendal::ErrorKind::NotFound,
        hide_list: false,
    });
    let storage = OpendalStorage::new(op);
    let user = DefaultUser;

    storage.mkd(&user, "/sub").await.context("mkd /sub")?;

    let metadata = storage
        .metadata(&user, "/sub")
        .await
        .context("metadata /sub")?;
    assert!(metadata.is_dir());
    Ok(())
}

/// Memory keeps no directory entries of its own, so `metadata` must resolve
/// implicit prefix directories from their children.
#[tokio::test]
async fn metadata_returns_directory_for_implicit_prefix() -> Result<()> {
    let storage = OpendalStorage::new(Operator::new(services::Memory::default())?);
    let user = DefaultUser;

    storage
        .put(&user, "abc".as_bytes(), "implicit/file.txt", 0)
        .await
        .context("put implicit/file.txt")?;

    for path in ["/implicit", "/implicit/"] {
        let metadata = storage
            .metadata(&user, path)
            .await
            .with_context(|| format!("metadata {path}"))?;
        assert!(metadata.is_dir(), "{path} must be a directory");
    }
    Ok(())
}

/// The FTP root always exists, even when a backend cannot stat it.
#[tokio::test]
async fn metadata_returns_directory_for_root() -> Result<()> {
    let op = Operator::new(services::Memory::default())?.layer(StatErrorLayer {
        injected_path: "/",
        injected_kind: opendal::ErrorKind::NotFound,
        hide_list: false,
    });
    let storage = OpendalStorage::new(op);

    let metadata = storage
        .metadata(&DefaultUser, "/")
        .await
        .context("metadata /")?;
    assert!(metadata.is_dir());
    Ok(())
}

#[tokio::test]
async fn metadata_returns_file_metadata() -> Result<()> {
    let storage = OpendalStorage::new(Operator::new(services::Memory::default())?);
    let user = DefaultUser;

    storage
        .put(&user, "hello".as_bytes(), "/file.txt", 0)
        .await
        .context("put /file.txt")?;

    let metadata = storage
        .metadata(&user, "/file.txt")
        .await
        .context("metadata /file.txt")?;
    assert!(metadata.is_file());
    assert!(!metadata.is_dir());
    assert_eq!(metadata.len(), 5);
    Ok(())
}

#[tokio::test]
async fn metadata_missing_returns_file_not_available() -> Result<()> {
    let storage = OpendalStorage::new(Operator::new(services::Memory::default())?);
    let user = DefaultUser;

    storage
        .put(&user, "abc".as_bytes(), "dir/file.txt", 0)
        .await
        .context("put dir/file.txt")?;

    let err = storage
        .metadata(&user, "/missing")
        .await
        .expect_err("metadata /missing must fail");
    assert_eq!(err.kind(), ErrorKind::PermanentFileNotAvailable);
    Ok(())
}

/// A failed close must abort the writer and return the close error, not the
/// abort error.
#[tokio::test]
async fn put_close_error_aborts_writer() -> Result<()> {
    let aborted = Arc::new(AtomicBool::new(false));
    let op = Operator::new(services::Memory::default())?.layer(CloseErrorLayer {
        aborted: aborted.clone(),
    });
    let storage = OpendalStorage::new(op);

    let err = storage
        .put(&DefaultUser, "body".as_bytes(), "file", 0)
        .await
        .expect_err("put must fail when close fails");
    assert_eq!(err.kind(), ErrorKind::PermissionDenied);
    assert!(
        aborted.load(Ordering::SeqCst),
        "close failure must abort the writer"
    );
    Ok(())
}

/// Parent traversal segments (`..`) must resolve lexically and clamp at the
/// FTP root on the filesystem backend instead of escaping or failing with
/// `NotFound`.
#[tokio::test]
async fn path_parent_traversal_on_fs() -> Result<()> {
    let (_root, storage) = fs_storage()?;
    let user = DefaultUser;

    storage.mkd(&user, "/dir").await.context("mkd /dir")?;
    storage
        .mkd(&user, "/dir/sub")
        .await
        .context("mkd /dir/sub")?;
    storage
        .put(&user, "top".as_bytes(), "/top.txt", 0)
        .await
        .context("put /top.txt")?;

    storage.cwd(&user, "/dir/..").await.context("cwd /dir/..")?;
    storage
        .cwd(&user, "/dir/sub/..")
        .await
        .context("cwd /dir/sub/..")?;
    assert_eq!(read_file(&storage, "/dir/../top.txt").await?, "top");

    let meta = storage
        .metadata(&user, "/dir/../top.txt")
        .await
        .context("metadata /dir/../top.txt")?;
    assert!(meta.is_file());

    let entries = storage
        .list(&user, "/dir/..")
        .await
        .context("list /dir/..")?;
    assert!(
        entries
            .iter()
            .any(|entry| entry.path.as_os_str() == "top.txt"),
        "list /dir/.. must include top.txt"
    );

    storage
        .put(&user, "escaped".as_bytes(), "/dir/../escape.txt", 0)
        .await
        .context("put /dir/../escape.txt")?;
    assert_eq!(read_file(&storage, "/escape.txt").await?, "escaped");

    storage
        .put(&user, "clamped".as_bytes(), "/../../clamped.txt", 0)
        .await
        .context("put /../../clamped.txt")?;
    assert_eq!(read_file(&storage, "/clamped.txt").await?, "clamped");

    storage
        .del(&user, "/dir/../clamped.txt")
        .await
        .context("del /dir/../clamped.txt")?;
    let err = storage
        .metadata(&user, "/clamped.txt")
        .await
        .expect_err("clamped.txt should be deleted");
    assert_eq!(err.kind(), ErrorKind::PermanentFileNotAvailable);

    let err = storage
        .rmd(&user, "/dir/..")
        .await
        .expect_err("rmd /dir/.. must reject the FTP root");
    assert_eq!(err.kind(), ErrorKind::PermissionDenied);
    assert_eq!(read_file(&storage, "/top.txt").await?, "top");
    Ok(())
}

/// Parent traversal segments (`..`) must not create literal `..` keys on
/// object-store-like backends such as memory, and `rmd /dir/..` must hit the
/// root guard.
#[tokio::test]
async fn path_parent_traversal_on_memory() -> Result<()> {
    let op = Operator::new(services::Memory::default())?;
    let storage = OpendalStorage::new(op.clone());
    let user = DefaultUser;

    storage
        .put(&user, "top".as_bytes(), "/top.txt", 0)
        .await
        .context("put /top.txt")?;
    storage
        .put(&user, "nested".as_bytes(), "/dir/nested.txt", 0)
        .await
        .context("put /dir/nested.txt")?;

    storage.cwd(&user, "/dir/..").await.context("cwd /dir/..")?;
    assert_eq!(read_file(&storage, "/dir/../top.txt").await?, "top");

    storage
        .put(&user, "escaped".as_bytes(), "/dir/../escape.txt", 0)
        .await
        .context("put /dir/../escape.txt")?;
    assert_eq!(read_file(&storage, "/escape.txt").await?, "escaped");
    assert!(
        !op.exists("dir/../escape.txt").await?,
        "literal dir/../escape.txt key must not be created"
    );

    storage
        .put(&user, "clamped".as_bytes(), "/../../clamped.txt", 0)
        .await
        .context("put /../../clamped.txt")?;
    assert_eq!(read_file(&storage, "/clamped.txt").await?, "clamped");
    assert!(
        !op.exists("../../clamped.txt").await?,
        "literal ../../clamped.txt key must not be created"
    );

    let err = storage
        .rmd(&user, "/dir/..")
        .await
        .expect_err("rmd /dir/.. must reject the FTP root");
    assert_eq!(err.kind(), ErrorKind::PermissionDenied);
    assert_eq!(read_file(&storage, "/top.txt").await?, "top");
    assert_eq!(read_file(&storage, "/dir/nested.txt").await?, "nested");
    Ok(())
}

/// `del` must report missing files as `PermanentFileNotAvailable` and refuse
/// to delete directories on the filesystem backend.
#[tokio::test]
async fn del_missing_and_directory_on_fs() -> Result<()> {
    let (_root, storage) = fs_storage()?;
    let user = DefaultUser;

    let err = storage
        .del(&user, "missing.txt")
        .await
        .expect_err("del missing.txt must fail");
    assert_eq!(err.kind(), ErrorKind::PermanentFileNotAvailable);

    storage.mkd(&user, "/dir").await.context("mkd /dir")?;
    for path in ["/dir", "/dir/", "/", "/dir/.."] {
        let err = storage
            .del(&user, path)
            .await
            .expect_err("del on directory must fail");
        assert_eq!(
            err.kind(),
            ErrorKind::PermanentFileNotAvailable,
            "del {path}"
        );
    }
    assert!(storage.metadata(&user, "/dir").await?.is_dir());

    storage
        .put(&user, "keep".as_bytes(), "/dir/file.txt", 0)
        .await
        .context("put /dir/file.txt")?;
    for path in ["/dir", "/dir/"] {
        let err = storage
            .del(&user, path)
            .await
            .expect_err("del on non-empty directory must fail");
        assert_eq!(
            err.kind(),
            ErrorKind::PermanentFileNotAvailable,
            "del {path}"
        );
    }
    assert_eq!(read_file(&storage, "/dir/file.txt").await?, "keep");

    storage
        .del(&user, "/dir/file.txt")
        .await
        .context("del /dir/file.txt")?;
    let err = storage
        .metadata(&user, "/dir/file.txt")
        .await
        .expect_err("deleted file must not exist");
    assert_eq!(err.kind(), ErrorKind::PermanentFileNotAvailable);
    Ok(())
}

/// `del` must report missing files as `PermanentFileNotAvailable` and leave
/// directories untouched on memory backends.
#[tokio::test]
async fn del_missing_and_directory_on_memory() -> Result<()> {
    let storage = OpendalStorage::new(Operator::new(services::Memory::default())?);
    let user = DefaultUser;

    let err = storage
        .del(&user, "missing.txt")
        .await
        .expect_err("del missing.txt must fail");
    assert_eq!(err.kind(), ErrorKind::PermanentFileNotAvailable);

    storage
        .put(&user, "keep".as_bytes(), "/dir/file.txt", 0)
        .await
        .context("put /dir/file.txt")?;
    for path in ["/dir", "/dir/", "/", "/dir/.."] {
        let err = storage
            .del(&user, path)
            .await
            .expect_err("del on directory must fail");
        assert_eq!(
            err.kind(),
            ErrorKind::PermanentFileNotAvailable,
            "del {path}"
        );
    }
    assert_eq!(read_file(&storage, "/dir/file.txt").await?, "keep");

    storage
        .del(&user, "/dir/file.txt")
        .await
        .context("del /dir/file.txt")?;
    let err = storage
        .metadata(&user, "/dir/file.txt")
        .await
        .expect_err("deleted file must not exist");
    assert_eq!(err.kind(), ErrorKind::PermanentFileNotAvailable);
    Ok(())
}

/// Backends without rename capability must report `CommandNotImplemented`.
#[tokio::test]
async fn rename_unsupported_on_memory() -> Result<()> {
    let storage = OpendalStorage::new(Operator::new(services::Memory::default())?);
    let user = DefaultUser;

    storage
        .put(&user, "hello".as_bytes(), "/file.txt", 0)
        .await
        .context("put /file.txt")?;

    let err = storage
        .rename(&user, "/file.txt", "/file2.txt")
        .await
        .expect_err("rename on memory must fail");
    assert_eq!(err.kind(), ErrorKind::CommandNotImplemented);

    let err = storage
        .rename(&user, "/dir/", "/dir2/")
        .await
        .expect_err("directory rename on memory must fail");
    assert_eq!(err.kind(), ErrorKind::CommandNotImplemented);
    Ok(())
}

/// Directory renames with trailing slashes must work on rename-capable
/// backends, while trailing-slash renames of regular files and root renames
/// must be rejected.
#[tokio::test]
async fn rename_directory_with_trailing_slash_on_fs() -> Result<()> {
    let (_root, storage) = fs_storage()?;
    let user = DefaultUser;

    storage.mkd(&user, "/dir").await.context("mkd /dir")?;
    storage
        .put(&user, "hello".as_bytes(), "/dir/nested.txt", 0)
        .await
        .context("put /dir/nested.txt")?;

    storage
        .rename(&user, "/dir/", "/dir2/")
        .await
        .context("rename /dir/ -> /dir2/")?;
    assert_eq!(read_file(&storage, "/dir2/nested.txt").await?, "hello");
    assert_eq!(
        storage
            .metadata(&user, "/dir")
            .await
            .expect_err("old directory must be gone")
            .kind(),
        ErrorKind::PermanentFileNotAvailable
    );

    storage
        .rename(&user, "/dir2/", "/dir3")
        .await
        .context("rename /dir2/ -> /dir3")?;
    storage
        .rename(&user, "/dir3", "/dir4/")
        .await
        .context("rename /dir3 -> /dir4/")?;
    assert_eq!(read_file(&storage, "/dir4/nested.txt").await?, "hello");

    for (from, to) in [
        ("/dir4/nested.txt/", "/other.txt"),
        ("/dir4/nested.txt", "/other.txt/"),
        ("/dir4/..", "/other"),
        ("/dir4", "/"),
    ] {
        let err = storage
            .rename(&user, from, to)
            .await
            .expect_err("invalid trailing-slash or root rename must fail");
        assert_eq!(
            err.kind(),
            ErrorKind::PermanentFileNotAvailable,
            "rename {from} -> {to}"
        );
    }
    assert_eq!(read_file(&storage, "/dir4/nested.txt").await?, "hello");
    Ok(())
}

/// Backends whose lister omits file metadata (mode, size, or last_modified)
/// must still report real file metadata in `list` and `list_fmt`, matching
/// `metadata`, while keeping directories intact without stat'ing them.
#[tokio::test]
async fn list_populates_file_metadata_on_memory() -> Result<()> {
    let expected_ts = raw::Timestamp::new(1_700_000_000, 0)?;
    let stat_count = Arc::new(AtomicUsize::new(0));
    let op = Operator::new(services::Memory::default())?.layer(StatObserveLayer {
        stat_count: stat_count.clone(),
        hide_stat: false,
        stat_last_modified: Some(expected_ts),
        strip_list_last_modified: false,
    });
    let storage = OpendalStorage::new(op);
    let user = DefaultUser;

    let payload = b"hello world";
    storage
        .put(&user, payload.as_slice(), "/hello.txt", 0)
        .await
        .context("put /hello.txt")?;
    storage
        .put(&user, "nested".as_bytes(), "/sub/nested.txt", 0)
        .await
        .context("put /sub/nested.txt")?;
    stat_count.store(0, Ordering::SeqCst);

    let entries = storage.list(&user, "/").await.context("list /")?;
    assert_eq!(entries.len(), 2);
    assert_eq!(
        stat_count.load(Ordering::SeqCst),
        1,
        "list must stat only the incomplete file entry and skip the directory"
    );

    let file = entries
        .iter()
        .find(|entry| entry.path.as_os_str() == "hello.txt")
        .context("hello.txt missing from list")?;
    assert!(file.metadata.is_file());
    assert!(!file.metadata.is_dir());
    assert_eq!(file.metadata.len(), payload.len() as u64);
    assert_eq!(
        file.metadata.modified().context("file modified")?,
        SystemTime::from(expected_ts)
    );

    let dir = entries
        .iter()
        .find(|entry| entry.path.as_os_str() == "sub/")
        .context("sub/ missing from list")?;
    assert!(dir.metadata.is_dir());
    assert!(!dir.metadata.is_file());

    // Single-file listing must also populate metadata.
    stat_count.store(0, Ordering::SeqCst);
    let single = storage
        .list(&user, "/hello.txt")
        .await
        .context("list /hello.txt")?;
    assert_eq!(single.len(), 1);
    assert!(single[0].metadata.is_file());
    assert_eq!(single[0].metadata.len(), payload.len() as u64);
    assert_eq!(
        single[0].metadata.modified().context("single modified")?,
        SystemTime::from(expected_ts)
    );
    assert_eq!(stat_count.load(Ordering::SeqCst), 1);

    // Formatted LIST output must reflect the real file size, not 0.
    let formatted = String::from_utf8(
        storage
            .list_fmt(&user, "/")
            .await
            .context("list_fmt /")?
            .into_inner(),
    )?;
    let hello_line = formatted
        .lines()
        .find(|line| line.ends_with(" hello.txt"))
        .context("hello.txt line missing from list_fmt")?;
    assert!(
        hello_line.contains(&format!(" {} ", payload.len())),
        "expected formatted size {} in line: {hello_line}",
        payload.len()
    );
    Ok(())
}

/// Backends whose lister already returns complete file metadata (such as fs)
/// must not issue extra `stat` calls, while entries missing `last_modified`
/// must still trigger the `stat` fallback.
#[tokio::test]
async fn list_skips_stat_when_metadata_complete_and_fills_missing_last_modified() -> Result<()> {
    let root = tempfile::tempdir()?;
    let root_path = root.path().to_str().context("non-UTF-8 test root")?;
    let user = DefaultUser;

    let stat_count = Arc::new(AtomicUsize::new(0));
    let complete_op =
        Operator::new(services::Fs::default().root(root_path))?.layer(StatObserveLayer {
            stat_count: stat_count.clone(),
            hide_stat: false,
            stat_last_modified: None,
            strip_list_last_modified: false,
        });
    let complete_storage = OpendalStorage::new(complete_op);

    let payload = b"complete metadata";
    complete_storage
        .put(&user, payload.as_slice(), "/hello.txt", 0)
        .await
        .context("put /hello.txt")?;
    complete_storage
        .mkd(&user, "/sub")
        .await
        .context("mkd /sub")?;
    stat_count.store(0, Ordering::SeqCst);

    let entries = complete_storage.list(&user, "/").await.context("list /")?;
    assert_eq!(entries.len(), 2);
    assert_eq!(
        stat_count.load(Ordering::SeqCst),
        0,
        "list must not stat entries when lister already provides complete metadata"
    );

    let file = entries
        .iter()
        .find(|entry| entry.path.as_os_str() == "hello.txt")
        .context("hello.txt missing")?;
    assert!(file.metadata.is_file());
    assert_eq!(file.metadata.len(), payload.len() as u64);
    assert!(file.metadata.modified().is_ok());

    // When lister reports file mode and size but omits last_modified, stat must
    // fill in modified time for files only (never directories).
    let partial_stat_count = Arc::new(AtomicUsize::new(0));
    let partial_op =
        Operator::new(services::Fs::default().root(root_path))?.layer(StatObserveLayer {
            stat_count: partial_stat_count.clone(),
            hide_stat: false,
            stat_last_modified: None,
            strip_list_last_modified: true,
        });
    let partial_storage = OpendalStorage::new(partial_op);

    let entries = partial_storage.list(&user, "/").await.context("list /")?;
    assert_eq!(entries.len(), 2);
    assert_eq!(
        partial_stat_count.load(Ordering::SeqCst),
        1,
        "list must stat only the file missing last_modified"
    );
    let file = entries
        .iter()
        .find(|entry| entry.path.as_os_str() == "hello.txt")
        .context("hello.txt missing")?;
    assert!(file.metadata.is_file());
    assert_eq!(file.metadata.len(), payload.len() as u64);
    assert!(file.metadata.modified().is_ok());
    Ok(())
}

/// Stat fallback during `list` must skip concurrently vanished entries
/// (`NotFound`), fall back to lister metadata when `stat` is unsupported or
/// unavailable, and propagate other stat errors.
#[tokio::test]
async fn list_handles_stat_not_found_unsupported_and_errors() -> Result<()> {
    let user = DefaultUser;

    // NotFound on one entry skips the vanished entry without failing the list.
    let op = Operator::new(services::Memory::default())?.layer(StatErrorLayer {
        injected_path: "vanished.txt",
        injected_kind: opendal::ErrorKind::NotFound,
        hide_list: false,
    });
    let storage = OpendalStorage::new(op);
    storage
        .put(&user, "gone".as_bytes(), "/vanished.txt", 0)
        .await?;
    storage
        .put(&user, "present".as_bytes(), "/present.txt", 0)
        .await?;

    let entries = storage.list(&user, "/").await.context("list /")?;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].path.as_os_str(), "present.txt");
    assert!(entries[0].metadata.is_file());
    assert_eq!(entries[0].metadata.len(), 7);

    // Unsupported from stat falls back to the lister's metadata cleanly.
    let op = Operator::new(services::Memory::default())?.layer(StatErrorLayer {
        injected_path: "fallback.txt",
        injected_kind: opendal::ErrorKind::Unsupported,
        hide_list: false,
    });
    let storage = OpendalStorage::new(op);
    storage
        .put(&user, "data".as_bytes(), "/fallback.txt", 0)
        .await?;

    let entries = storage.list(&user, "/").await.context("list /")?;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].path.as_os_str(), "fallback.txt");
    assert!(entries[0].metadata.is_file());
    assert_eq!(entries[0].metadata.len(), 4);
    assert!(entries[0].metadata.modified().is_err());

    // Missing stat capability skips stat calls entirely.
    let stat_count = Arc::new(AtomicUsize::new(0));
    let op = Operator::new(services::Memory::default())?.layer(StatObserveLayer {
        stat_count: stat_count.clone(),
        hide_stat: true,
        stat_last_modified: None,
        strip_list_last_modified: false,
    });
    let storage = OpendalStorage::new(op);
    storage
        .put(&user, "data".as_bytes(), "/no-stat.txt", 0)
        .await?;
    stat_count.store(0, Ordering::SeqCst);

    let entries = storage.list(&user, "/").await.context("list /")?;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].path.as_os_str(), "no-stat.txt");
    assert_eq!(stat_count.load(Ordering::SeqCst), 0);

    // Other stat errors are propagated via convert_err.
    let op = Operator::new(services::Memory::default())?.layer(StatErrorLayer {
        injected_path: "secret.txt",
        injected_kind: opendal::ErrorKind::PermissionDenied,
        hide_list: false,
    });
    let storage = OpendalStorage::new(op);
    storage
        .put(&user, "secret".as_bytes(), "/secret.txt", 0)
        .await?;

    let err = storage
        .list(&user, "/")
        .await
        .err()
        .expect("permission denied during stat fallback must fail list");
    assert_eq!(err.kind(), ErrorKind::PermissionDenied);
    Ok(())
}
