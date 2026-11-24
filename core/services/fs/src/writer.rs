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

#[cfg(unix)]
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Buf;
#[cfg(target_os = "linux")]
use bytes::Bytes;
use tokio::io::AsyncWriteExt;

use super::core::{
    ErrorContext, FsCore, parse_error, parse_rename_error, publish_staged_file, refine_path_error,
};
#[cfg(target_os = "linux")]
use super::direct_io::DirectIoAlignment;
use opendal_core::raw::*;
use opendal_core::*;

type FsWriters = TwoWays<FsWriter, oio::PositionWriter<FsWriter>>;

pub struct FsLazyWriter {
    core: Arc<FsCore>,
    executor: Executor,
    path: String,
    op: OpWrite,
    inner: Option<FsWriters>,
    #[cfg(target_os = "linux")]
    direct_buffer: Option<DirectWriteBuffer>,
}

impl FsLazyWriter {
    pub(super) fn new(core: Arc<FsCore>, executor: Executor, path: &str, op: OpWrite) -> Self {
        Self {
            core,
            executor,
            path: path.to_string(),
            op,
            inner: None,
            #[cfg(target_os = "linux")]
            direct_buffer: None,
        }
    }

    async fn open(&mut self) -> Result<()> {
        if self.inner.is_some() {
            return Ok(());
        }

        let writer = FsWriter::create(&self.core, &self.path, &self.op).await?;
        #[cfg(target_os = "linux")]
        if let Some(alignment) = writer.direct_io {
            self.direct_buffer = Some(DirectWriteBuffer {
                offset_align: alignment.offset_align,
                tail: Bytes::new(),
            });
        }

        self.inner = Some(if self.op.append() {
            FsWriters::One(writer)
        } else {
            FsWriters::Two(oio::PositionWriter::new(
                self.executor.clone(),
                writer,
                self.op.concurrent(),
            ))
        });
        Ok(())
    }
}

impl oio::Write for FsLazyWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        self.open().await?;
        let inner = self.inner.as_mut().expect("writer must be initialized");
        #[cfg(target_os = "linux")]
        if let Some(direct) = &mut self.direct_buffer {
            return direct.write(inner, bs).await;
        }
        inner.write(bs).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.open().await?;
        let inner = self.inner.as_mut().expect("writer must be initialized");
        #[cfg(target_os = "linux")]
        if let Some(direct) = &mut self.direct_buffer
            && !direct.tail.is_empty()
        {
            // PositionWriter waits for all write tasks before closing with the
            // logical byte count, while write_all_at pads the aligned tail.
            inner.write(direct.tail.clone().into()).await?;
            direct.tail = Bytes::new();
        }
        inner.close().await
    }

    async fn abort(&mut self) -> Result<()> {
        if let Some(inner) = &mut self.inner {
            inner.abort().await?;
        }
        #[cfg(target_os = "linux")]
        if let Some(direct) = &mut self.direct_buffer {
            direct.tail = Bytes::new();
        }
        Ok(())
    }
}

#[cfg(target_os = "linux")]
struct DirectWriteBuffer {
    offset_align: usize,
    tail: Bytes,
}

#[cfg(target_os = "linux")]
impl DirectWriteBuffer {
    async fn write(&mut self, writer: &mut FsWriters, bs: Buffer) -> Result<()> {
        if bs.is_empty() {
            return Ok(());
        }
        let mut pending: Buffer = if self.tail.is_empty() {
            bs
        } else {
            std::iter::once(self.tail.clone()).chain(bs).collect()
        };
        let aligned_len = pending.len() / self.offset_align * self.offset_align;
        if aligned_len > 0 {
            oio::Write::write(writer, pending.split_to(aligned_len)).await?;
        }
        // Own the small tail without retaining the caller's larger allocations.
        self.tail = pending.to_vec().into();
        Ok(())
    }
}

pub struct FsWriter {
    target_path: PathBuf,
    /// The staging path for a write that publishes the file on close.
    temp_path: Option<PathBuf>,
    if_not_exists: bool,
    f: tokio::fs::File,
    #[cfg(target_os = "linux")]
    direct_io: Option<DirectIoAlignment>,
    /// User metadata to be written to xattr on Unix systems.
    #[cfg(unix)]
    user_metadata: Option<HashMap<String, String>>,
    /// Whether close must replace user metadata on an existing inode.
    #[cfg(unix)]
    overwrite_in_place: bool,
}

impl FsWriter {
    async fn create(core: &FsCore, path: &str, op: &OpWrite) -> Result<Self> {
        let target_path = core.ensure_write_abs_path(&core.root, path).await?;

        #[cfg(unix)]
        let user_metadata = op.user_metadata().map(|metadata| {
            metadata
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value.to_owned()))
                .collect()
        });

        let mut temp_path = None;
        if let Some(atomic_write_dir) = &core.atomic_write_dir {
            // Fail fast on an existing destination instead of staging content
            // that the conditional commit must reject.
            if op.if_not_exists()
                && tokio::fs::try_exists(&target_path)
                    .await
                    .map_err(new_std_io_error)?
            {
                return Err(Error::new(
                    ErrorKind::ConditionNotMatch,
                    "file already exists, doesn't match the condition if_not_exists",
                ));
            }
            // Unconditional append always opens the target directly so
            // concurrent writers cannot overwrite each other when closing.
            let append_in_place = op.append() && !op.if_not_exists();
            if !append_in_place {
                temp_path = Some(
                    core.ensure_write_abs_path(atomic_write_dir, &build_tmp_path_of(path))
                        .await?,
                );
            }
        }

        let open_path = temp_path.as_ref().unwrap_or(&target_path);

        // Opening an existing file without appending truncates it in place,
        // keeping the old inode and its user metadata.
        #[cfg(unix)]
        let overwrite_in_place = !op.append() && temp_path.is_none();

        #[cfg(target_os = "linux")]
        if core.direct_io {
            let open_path = open_path.clone();
            let create_new = temp_path.is_some() || op.if_not_exists();
            let operation = if temp_path.is_some() {
                ServiceOperation("OpenTemporaryFile")
            } else {
                ServiceOperation("OpenForWrite")
            };
            let (f, alignment) = tokio::task::spawn_blocking(move || {
                DirectIoAlignment::open_for_write(&open_path, operation, create_new)
            })
            .await
            .map_err(new_task_join_error)??;
            return Ok(Self {
                target_path,
                temp_path,
                if_not_exists: op.if_not_exists(),
                f: tokio::fs::File::from_std(f),
                direct_io: Some(alignment),
                #[cfg(unix)]
                user_metadata,
                #[cfg(unix)]
                overwrite_in_place,
            });
        }

        let mut options = OpenOptions::new();
        options.write(true);
        let operation = if temp_path.is_some() {
            options.create_new(true);
            ServiceOperation("OpenTemporaryFile")
        } else {
            options
                .create_new(op.if_not_exists())
                .create(!op.if_not_exists());
            options.append(op.append());
            ServiceOperation("OpenForWrite")
        };

        options.truncate(!op.append() || temp_path.is_some());
        let f = match tokio::fs::OpenOptions::from(options).open(open_path).await {
            Ok(f) => f,
            Err(err) => {
                let mut err = refine_path_error(err, open_path).await;
                // Unix fails `O_CREAT | O_EXCL` on a directory with `EEXIST`.
                if op.if_not_exists() && err.kind() == std::io::ErrorKind::IsADirectory {
                    err = std::io::Error::new(std::io::ErrorKind::AlreadyExists, err);
                }
                return Err(parse_error(ErrorContext::new(operation), err));
            }
        };
        Ok(Self {
            target_path,
            temp_path,
            if_not_exists: op.if_not_exists(),
            f,
            #[cfg(target_os = "linux")]
            direct_io: None,
            #[cfg(unix)]
            user_metadata,
            #[cfg(unix)]
            overwrite_in_place,
        })
    }

    /// Write the final user metadata for this write.
    ///
    /// A non-append write without staging truncates the existing file in place,
    /// so user metadata from the previous content must be replaced. Staged
    /// writes attach metadata to the staging file before publishing the new
    /// inode, while appends keep the metadata already attached to the file.
    #[cfg(unix)]
    fn update_user_metadata(&self) -> Result<()> {
        if self.overwrite_in_place {
            FsCore::replace_user_metadata(&self.target_path, self.user_metadata.as_ref())
        } else if let Some(user_metadata) = &self.user_metadata {
            let path = self.temp_path.as_deref().unwrap_or(&self.target_path);
            FsCore::set_user_metadata(path, user_metadata)
        } else {
            Ok(())
        }
    }

    /// Discard the staged file on a permanent failure while keeping it intact
    /// for temporary errors so retry layers can retry `close()`.
    async fn cleanup_staged_on_permanent_error(&self, err: Error) -> Error {
        if err.is_temporary() {
            return err;
        }
        let Some(temp_path) = &self.temp_path else {
            return err;
        };

        match tokio::fs::remove_file(temp_path).await {
            Ok(()) => err,
            Err(cleanup_err) if cleanup_err.kind() == std::io::ErrorKind::NotFound => err,
            Err(cleanup_err) => err.with_context("staging_cleanup", cleanup_err),
        }
    }

    async fn commit_staged_file(&self) -> Result<()> {
        let Some(temp_path) = &self.temp_path else {
            return Ok(());
        };

        let result = if self.if_not_exists {
            let temp_path = temp_path.clone();
            let target_path = self.target_path.clone();
            tokio::task::spawn_blocking(move || publish_staged_file(&temp_path, &target_path))
                .await
                .map_err(new_task_join_error)?
        } else {
            match tokio::fs::rename(temp_path, &self.target_path).await {
                Ok(()) => Ok(()),
                Err(err) => Err(parse_rename_error(
                    refine_path_error(err, &self.target_path).await,
                )),
            }
        };

        match result {
            Ok(()) => Ok(()),
            Err(err) => Err(self.cleanup_staged_on_permanent_error(err).await),
        }
    }
}

/// # Safety
///
/// We will only take `&mut Self` reference for FsWriter.
unsafe impl Sync for FsWriter {}

impl oio::Write for FsWriter {
    async fn write(&mut self, mut bs: Buffer) -> Result<()> {
        while bs.has_remaining() {
            let n = self.f.write(bs.chunk()).await.map_err(new_std_io_error)?;
            bs.advance(n);
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        if let Err(err) = self.f.flush().await.map_err(new_std_io_error) {
            return Err(self.cleanup_staged_on_permanent_error(err).await);
        }
        #[cfg(unix)]
        if let Err(err) = self.update_user_metadata() {
            return Err(self.cleanup_staged_on_permanent_error(err).await);
        }
        if let Err(err) = self.f.sync_all().await.map_err(new_std_io_error) {
            return Err(self.cleanup_staged_on_permanent_error(err).await);
        }

        self.commit_staged_file().await?;

        let file_meta = self.f.metadata().await.map_err(new_std_io_error)?;
        let mut meta = MetadataBuilder::file(file_meta.len());
        meta.last_modified(Timestamp::try_from(
            file_meta.modified().map_err(new_std_io_error)?,
        )?);
        Ok(meta.build())
    }

    async fn abort(&mut self) -> Result<()> {
        if let Some(temp_path) = &self.temp_path {
            tokio::fs::remove_file(temp_path)
                .await
                .map_err(new_std_io_error)
        } else {
            Err(Error::new(
                ErrorKind::Unsupported,
                "Fs doesn't support abort for non-staged writes",
            ))
        }
    }
}

impl oio::PositionWrite for FsWriter {
    async fn write_all_at(&self, offset: u64, buf: Buffer) -> Result<()> {
        let f = self
            .f
            .try_clone()
            .await
            .map_err(new_std_io_error)?
            .into_std()
            .await;

        #[cfg(target_os = "linux")]
        let direct_io = self.direct_io;
        tokio::task::spawn_blocking(move || {
            #[cfg(target_os = "linux")]
            if let Some(alignment) = direct_io {
                return alignment.write_all_at(&f, offset, buf);
            }
            let mut buf = buf;
            let mut offset = offset;
            while !buf.is_empty() {
                match write_at(&f, buf.chunk(), offset) {
                    Ok(n) => {
                        buf.advance(n);
                        offset += n as u64
                    }
                    Err(e) => return Err(e),
                }
            }
            Ok(())
        })
        .await
        .map_err(new_task_join_error)?
    }

    async fn close(&self, _size: u64) -> Result<Metadata> {
        #[cfg(target_os = "linux")]
        if self.direct_io.is_some()
            && let Err(err) = self.f.set_len(_size).await.map_err(new_std_io_error)
        {
            return Err(self.cleanup_staged_on_permanent_error(err).await);
        }

        let mut f = self
            .f
            .try_clone()
            .await
            .map_err(new_std_io_error)?
            .into_std()
            .await;

        if let Err(err) = f.flush().map_err(new_std_io_error) {
            return Err(self.cleanup_staged_on_permanent_error(err).await);
        }
        #[cfg(unix)]
        if let Err(err) = self.update_user_metadata() {
            return Err(self.cleanup_staged_on_permanent_error(err).await);
        }
        if let Err(err) = f.sync_all().map_err(new_std_io_error) {
            return Err(self.cleanup_staged_on_permanent_error(err).await);
        }

        self.commit_staged_file().await?;

        let file_meta = f.metadata().map_err(new_std_io_error)?;
        let mode = if file_meta.is_file() {
            EntryMode::FILE
        } else if file_meta.is_dir() {
            EntryMode::DIR
        } else {
            EntryMode::Unknown
        };
        let mut meta = match mode {
            EntryMode::FILE => MetadataBuilder::file(file_meta.len()),
            EntryMode::DIR => MetadataBuilder::dir(),
            EntryMode::Unknown => MetadataBuilder::unknown(),
        };
        meta.last_modified(Timestamp::try_from(
            file_meta.modified().map_err(new_std_io_error)?,
        )?);
        Ok(meta.build())
    }

    async fn abort(&self) -> Result<()> {
        if let Some(temp_path) = &self.temp_path {
            tokio::fs::remove_file(temp_path)
                .await
                .map_err(new_std_io_error)
        } else {
            Err(Error::new(
                ErrorKind::Unsupported,
                "Fs doesn't support abort if atomic_write_dir is not set",
            ))
        }
    }
}

#[cfg(windows)]
fn write_at(f: &File, buf: &[u8], offset: u64) -> Result<usize> {
    use std::os::windows::fs::FileExt;
    f.seek_write(buf, offset).map_err(new_std_io_error)
}

#[cfg(unix)]
fn write_at(f: &File, buf: &[u8], offset: u64) -> Result<usize> {
    use std::os::unix::fs::FileExt;
    f.write_at(buf, offset).map_err(new_std_io_error)
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::collections::HashMap;
    use std::path::Path;

    use opendal_core::Operator;

    use super::*;

    #[cfg(unix)]
    fn xattrs_supported(dir: &Path) -> bool {
        let probe = dir.join(".xattr-probe");
        if std::fs::write(&probe, b"probe").is_err() {
            return false;
        }
        let supported = xattr::set(&probe, "user.opendal_probe", b"1").is_ok();
        let _ = std::fs::remove_file(&probe);
        supported
    }

    fn staging_is_empty(staging: &Path) -> bool {
        std::fs::read_dir(staging).unwrap().next().is_none()
    }

    fn operator(root: &Path, atomic_write_dir: Option<&Path>) -> Operator {
        let builder = crate::Fs::default().root(root.to_str().unwrap());
        let builder = match atomic_write_dir {
            Some(dir) => builder.atomic_write_dir(dir.to_str().unwrap()),
            None => builder,
        };
        Operator::new(builder).unwrap()
    }

    #[cfg(unix)]
    async fn overwrite_replaces_user_metadata(atomic_write: bool) {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let root = temp_dir.path().join("root");
        std::fs::create_dir(&root).unwrap();
        if !xattrs_supported(&root) {
            return;
        }
        let staging = atomic_write.then(|| {
            let dir = temp_dir.path().join("staging");
            std::fs::create_dir(&dir).unwrap();
            dir
        });
        let op = operator(&root, staging.as_deref());

        op.write_with("file", "first")
            .user_metadata([("old".to_string(), "value".to_string())])
            .await
            .unwrap();
        let file = root.join("file");
        assert_eq!(
            FsCore::get_user_metadata(&file).unwrap(),
            HashMap::from([("old".to_string(), "value".to_string())])
        );

        // A non-empty to non-empty overwrite must drop the old key and keep
        // only the new one.
        op.write_with("file", "second")
            .user_metadata([("new".to_string(), "value".to_string())])
            .await
            .unwrap();
        assert_eq!(
            FsCore::get_user_metadata(&file).unwrap(),
            HashMap::from([("new".to_string(), "value".to_string())])
        );

        // A write without user metadata must clear the previous keys.
        op.write("file", "third").await.unwrap();
        assert!(FsCore::get_user_metadata(&file).unwrap().is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_overwrite_replaces_user_metadata() {
        overwrite_replaces_user_metadata(false).await;
        overwrite_replaces_user_metadata(true).await;
    }

    #[cfg(unix)]
    async fn append_preserves_user_metadata(atomic_write: bool) {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let root = temp_dir.path().join("root");
        std::fs::create_dir(&root).unwrap();
        if !xattrs_supported(&root) {
            return;
        }
        let staging = atomic_write.then(|| {
            let dir = temp_dir.path().join("staging");
            std::fs::create_dir(&dir).unwrap();
            dir
        });
        let op = operator(&root, staging.as_deref());

        let metadata = HashMap::from([
            ("first".to_string(), "one".to_string()),
            ("second".to_string(), "two".to_string()),
        ]);
        op.write_with("file", "first")
            .user_metadata(metadata.clone())
            .await
            .unwrap();

        op.write_with("file", "more").append(true).await.unwrap();

        assert_eq!(
            FsCore::get_user_metadata(&root.join("file")).unwrap(),
            metadata
        );
        assert_eq!(op.read("file").await.unwrap().to_vec(), b"firstmore");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_append_preserves_user_metadata() {
        append_preserves_user_metadata(false).await;
        append_preserves_user_metadata(true).await;
    }

    #[tokio::test]
    async fn test_staged_write_cleans_up_on_is_a_directory() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let root = temp_dir.path().join("root");
        let staging = temp_dir.path().join("staging");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&staging).unwrap();
        std::fs::create_dir(root.join("target")).unwrap();

        let op = operator(&root, Some(&staging));
        let err = op.write("target", "data").await.unwrap_err();
        assert!(!err.is_temporary(), "{err:?}");
        assert_eq!(err.kind(), ErrorKind::IsADirectory, "{err:?}");
        assert!(staging_is_empty(&staging));
    }

    #[tokio::test]
    async fn test_staged_write_cleans_up_on_not_a_directory() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let root = temp_dir.path().join("root");
        let staging = temp_dir.path().join("staging");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&staging).unwrap();

        let op = operator(&root, Some(&staging));
        let mut writer = op.writer("sub/file").await.unwrap();
        writer.write("data").await.unwrap();
        assert!(!staging_is_empty(&staging));

        // Replace the parent directory with a regular file before commit so
        // rename fails permanently with ENOTDIR.
        std::fs::remove_dir(root.join("sub")).unwrap();
        std::fs::write(root.join("sub"), b"blocker").unwrap();

        let err = writer.close().await.unwrap_err();
        assert!(!err.is_temporary(), "{err:?}");
        assert_eq!(err.kind(), ErrorKind::NotADirectory, "{err:?}");
        assert!(staging_is_empty(&staging));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_staged_write_cleans_up_on_permission_denied() {
        use std::os::unix::fs::PermissionsExt;

        // Root bypasses directory write permission checks.
        if unsafe { libc::geteuid() } == 0 {
            return;
        }

        let temp_dir = tempfile::TempDir::new().unwrap();
        let root = temp_dir.path().join("root");
        let staging = temp_dir.path().join("staging");
        let ro_dir = root.join("ro");
        std::fs::create_dir_all(&ro_dir).unwrap();
        std::fs::create_dir(&staging).unwrap();
        std::fs::set_permissions(&ro_dir, std::fs::Permissions::from_mode(0o555)).unwrap();

        let op = operator(&root, Some(&staging));
        let result = op.write("ro/file", "data").await;

        std::fs::set_permissions(&ro_dir, std::fs::Permissions::from_mode(0o755)).unwrap();

        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PermissionDenied, "{err:?}");
        assert!(!err.is_temporary(), "{err:?}");
        assert!(staging_is_empty(&staging));
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_staged_write_cleans_up_on_cross_device_rename() {
        use std::os::unix::fs::MetadataExt;

        let shm = Path::new("/dev/shm");
        let Ok(staging_dir) = tempfile::tempdir_in(shm) else {
            return;
        };
        let root_dir = tempfile::tempdir().unwrap();
        if std::fs::metadata(root_dir.path()).unwrap().dev()
            == std::fs::metadata(staging_dir.path()).unwrap().dev()
        {
            return;
        }

        let op = operator(root_dir.path(), Some(staging_dir.path()));
        let err = op.write("file", "data").await.unwrap_err();
        assert!(!err.is_temporary(), "{err:?}");
        assert!(staging_is_empty(staging_dir.path()));
    }

    #[tokio::test]
    async fn test_cleanup_staged_preserves_temp_file_on_temporary_error() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let root = temp_dir.path().join("root");
        let staging = temp_dir.path().join("staging");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&staging).unwrap();

        let core = Arc::new(FsCore {
            info: ServiceInfo::new(crate::FS_SCHEME, root.to_string_lossy(), ""),
            capability: Capability::default(),
            root: root.clone(),
            atomic_write_dir: Some(staging.clone()),
            direct_io: false,
            buf_pool: oio::PooledBuf::new(1),
        });

        let writer = FsWriter::create(&core, "file", &OpWrite::default())
            .await
            .unwrap();
        oio::PositionWrite::write_all_at(&writer, 0, Buffer::from("data"))
            .await
            .unwrap();
        let temp_path = writer.temp_path.clone().unwrap();
        assert!(temp_path.exists());

        // Temporary errors must leave the staging file intact for retry.
        let transient = Error::new(ErrorKind::Unexpected, "transient").set_temporary();
        let returned = writer.cleanup_staged_on_permanent_error(transient).await;
        assert!(returned.is_temporary());
        assert!(temp_path.exists());

        // A retried close must publish the preserved staging file cleanly.
        oio::PositionWrite::close(&writer, 4).await.unwrap();
        assert_eq!(std::fs::read(root.join("file")).unwrap(), b"data");
        assert!(staging_is_empty(&staging));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_staged_write_applies_user_metadata_before_commit() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let root = temp_dir.path().join("root");
        let staging = temp_dir.path().join("staging");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&staging).unwrap();
        if !xattrs_supported(&root) {
            return;
        }

        let core = Arc::new(FsCore {
            info: ServiceInfo::new(crate::FS_SCHEME, root.to_string_lossy(), ""),
            capability: Capability::default(),
            root: root.clone(),
            atomic_write_dir: Some(staging.clone()),
            direct_io: false,
            buf_pool: oio::PooledBuf::new(1),
        });

        let expected = HashMap::from([("key".to_string(), "value".to_string())]);
        let capability = Capability {
            write_with_user_metadata: true,
            ..Default::default()
        };
        let (op_write, _) = OpWrite::from_options(
            &capability,
            options::WriteOptions {
                user_metadata: Some(expected.clone()),
                ..Default::default()
            },
        )
        .unwrap();
        let writer = FsWriter::create(&core, "file", &op_write).await.unwrap();
        oio::PositionWrite::write_all_at(&writer, 0, Buffer::from("data"))
            .await
            .unwrap();

        let temp_path = writer.temp_path.clone().unwrap();
        writer.update_user_metadata().unwrap();
        assert!(!root.join("file").exists());
        assert_eq!(FsCore::get_user_metadata(&temp_path).unwrap(), expected);

        writer.commit_staged_file().await.unwrap();
        assert_eq!(
            FsCore::get_user_metadata(&root.join("file")).unwrap(),
            expected
        );
        assert!(staging_is_empty(&staging));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_staged_conditional_write_loss_preserves_winner_user_metadata() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let root = temp_dir.path().join("root");
        let staging = temp_dir.path().join("staging");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&staging).unwrap();
        if !xattrs_supported(&root) {
            return;
        }

        let op = operator(&root, Some(&staging));
        for append in [false, true] {
            let path = if append { "race_append" } else { "race" };
            let mut loser = op
                .writer_with(path)
                .if_not_exists(true)
                .append(append)
                .user_metadata([("owner".to_string(), "loser".to_string())])
                .await
                .unwrap();
            loser.write("loser").await.unwrap();

            op.write_with(path, "winner")
                .user_metadata([("owner".to_string(), "winner".to_string())])
                .await
                .unwrap();

            let err = loser.close().await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::ConditionNotMatch, "{err:?}");
            assert_eq!(op.read(path).await.unwrap().to_bytes(), "winner");
            assert_eq!(
                FsCore::get_user_metadata(&root.join(path)).unwrap(),
                HashMap::from([("owner".to_string(), "winner".to_string())])
            );
            assert!(staging_is_empty(&staging));
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_staged_write_user_metadata_failure_does_not_publish_or_leak() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let root = temp_dir.path().join("root");
        let staging = temp_dir.path().join("staging");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&staging).unwrap();

        let op = operator(&root, Some(&staging));
        // A NUL byte in the xattr key fails deterministically on all Unix
        // platforms before or at setxattr without depending on filesystem
        // xattr configuration.
        let invalid_metadata = [("bad\0key".to_string(), "v".to_string())];

        // New file must not be published and staged file must be removed for
        // both PositionWrite::close and oio::Write::close (append + if_not_exists).
        for (name, append, if_not_exists) in
            [("new_file", false, false), ("new_append", true, true)]
        {
            let err = op
                .write_with(name, "payload")
                .append(append)
                .if_not_exists(if_not_exists)
                .user_metadata(invalid_metadata.clone())
                .await
                .unwrap_err();
            assert!(!err.is_temporary(), "{err:?}");
            assert!(!root.join(name).exists());
            assert!(staging_is_empty(&staging));
        }

        // Overwriting an existing file must leave it untouched if setting
        // user metadata on the staged file fails.
        std::fs::write(root.join("existing"), b"original").unwrap();
        let err = op
            .write_with("existing", "updated")
            .user_metadata(invalid_metadata)
            .await
            .unwrap_err();
        assert!(!err.is_temporary(), "{err:?}");
        assert_eq!(std::fs::read(root.join("existing")).unwrap(), b"original");
        assert!(staging_is_empty(&staging));
    }
}
