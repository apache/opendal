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
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context as TaskContext, Poll};

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
