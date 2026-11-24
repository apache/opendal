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

use std::fmt::Debug;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use monoio::fs::OpenOptions;
use opendal_core::raw::*;
use opendal_core::*;

use super::config::MonoiofsConfig;
use super::core::BUFFER_SIZE;
use super::core::MonoiofsCore;
use super::deleter::MonoiofsDeleter;
use super::reader::*;

/// File system support via [`monoio`].
#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct MonoiofsBuilder {
    pub(super) config: MonoiofsConfig,
}

impl MonoiofsBuilder {
    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };
        self
    }
}

impl Builder for MonoiofsBuilder {
    type Config = MonoiofsConfig;

    fn build(self) -> Result<impl Service> {
        let root = self
            .config
            .root
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .ok_or_else(|| {
                Error::new(ErrorKind::ConfigInvalid, "root is not specified")
                    .with_operation("Builder::build")
            })?;
        match std::fs::metadata(&root) {
            Ok(meta) if !meta.is_dir() => {
                return Err(
                    Error::new(ErrorKind::ConfigInvalid, "root is not a directory")
                        .with_operation("Builder::build")
                        .with_context("root", root.to_string_lossy()),
                );
            }
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                std::fs::create_dir_all(&root).map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "create root dir failed")
                        .with_operation("Builder::build")
                        .with_context("root", root.to_string_lossy())
                        .set_source(e)
                })?;
            }
            Err(_) => {}
        }
        let root = root.canonicalize().map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                "canonicalize of root directory failed",
            )
            .with_operation("Builder::build")
            .with_context("root", root.to_string_lossy())
            .set_source(e)
        })?;
        let worker_threads = 1; // TODO: test concurrency and default to available_parallelism and bind cpu
        let io_uring_entries = 1024;
        Ok(MonoiofsBackend {
            core: Arc::new(MonoiofsCore::new(root, worker_threads, io_uring_entries)),
        })
    }
}

#[derive(Debug, Clone)]
pub struct MonoiofsBackend {
    pub(crate) core: Arc<MonoiofsCore>,
}

impl Service for MonoiofsBackend {
    type Reader = oio::PositionReader<MonoiofsPositionReader>;
    type Writer = MonoiofsLazyWriter;
    type Lister = ();
    type Deleter = oio::OneShotDeleter<MonoiofsDeleter>;
    type Copier = oio::OneShotCopier;
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        self.core.info.clone()
    }

    fn capability(&self) -> Capability {
        self.core.capability
    }

    async fn stat(&self, _ctx: &OperationContext, path: &str, _args: OpStat) -> Result<RpStat> {
        let path = self.core.prepare_path(path)?;
        let meta = self
            .core
            .dispatch(move || monoio::fs::metadata(path))
            .await
            .map_err(new_std_io_error)?;
        let mode = if meta.is_dir() {
            EntryMode::DIR
        } else if meta.is_file() {
            EntryMode::FILE
        } else {
            EntryMode::Unknown
        };
        let mut m = match mode {
            EntryMode::FILE => MetadataBuilder::file(meta.len()),
            EntryMode::DIR => MetadataBuilder::dir(),
            EntryMode::Unknown => MetadataBuilder::unknown(),
        };
        m.last_modified(Timestamp::try_from(
            meta.modified().map_err(new_std_io_error)?,
        )?);
        Ok(RpStat::new(m.build()))
    }
    fn read(&self, _ctx: &OperationContext, path: &str, _args: OpRead) -> Result<Self::Reader> {
        let path = self.core.prepare_path(path)?;
        Ok(oio::PositionReader::new(MonoiofsPositionReader::new(
            self.core.clone(),
            path,
        )))
    }

    fn write(&self, _ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        Ok(MonoiofsLazyWriter::new(self.core.clone(), path, args))
    }

    fn delete(&self, _ctx: &OperationContext) -> Result<Self::Deleter> {
        let output: oio::OneShotDeleter<MonoiofsDeleter> = {
            Ok(oio::OneShotDeleter::new(MonoiofsDeleter::new(
                self.core.clone(),
            )))
        }?;

        Ok(output)
    }

    fn list(&self, _ctx: &OperationContext, _path: &str, _args: OpList) -> Result<Self::Lister> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn rename(
        &self,
        _ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        #[cfg(not(target_os = "linux"))]
        if args.if_not_exists() {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "Conditional rename is only supported on Linux",
            ));
        }

        let from = self.core.prepare_path(from)?;
        // ensure file exists
        let meta = self
            .core
            .dispatch({
                let from = from.clone();
                move || monoio::fs::metadata(from)
            })
            .await
            .map_err(new_std_io_error)?;

        #[cfg(target_os = "linux")]
        if args.if_not_exists() {
            use rustix::fs::{CWD, RenameFlags, renameat_with};

            if meta.is_dir() {
                return Err(Error::new(
                    ErrorKind::IsADirectory,
                    "rename source is a directory",
                ));
            }
            let to = self.core.prepare_write_path(to).await?;
            self.core
                .dispatch(move || async move {
                    renameat_with(CWD, &from, CWD, &to, RenameFlags::NOREPLACE)
                        .map_err(parse_conditional_rename_error)
                })
                .await?;
            return Ok(RpRename::default());
        }

        let _ = meta;
        let to = self.core.prepare_write_path(to).await?;
        self.core
            .dispatch(move || monoio::fs::rename(from, to))
            .await
            .map_err(new_std_io_error)?;
        Ok(RpRename::default())
    }

    async fn create_dir(
        &self,
        _ctx: &OperationContext,
        path: &str,
        _args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        let path = self.core.prepare_path(path)?;
        self.core
            .dispatch(move || monoio::fs::create_dir_all(path))
            .await
            .map_err(new_std_io_error)?;
        Ok(RpCreateDir::default())
    }

    fn copy(
        &self,
        _ctx: &OperationContext,
        from: &str,
        to: &str,
        _args: OpCopy,
    ) -> Result<Self::Copier> {
        let core = self.core.clone();
        let from = self.core.prepare_path(from)?;
        let to_path = self.core.prepare_path(to)?;
        let to = to.to_string();

        let copier = oio::OneShotCopier::new(async move {
            // Ensure source exists, is not a directory, and does not alias destination
            // before creating destination parent directories or opening/truncating destination.
            let (from_meta, to_meta) = core
                .dispatch({
                    let from = from.clone();
                    let to_path = to_path.clone();
                    move || async move {
                        let from_meta = monoio::fs::metadata(&from).await?;
                        if from_meta.is_dir() {
                            return Ok((from_meta, None));
                        }
                        let to_meta = match monoio::fs::metadata(&to_path).await {
                            Ok(meta) => Some(meta),
                            Err(err) if err.kind() == io::ErrorKind::NotFound => None,
                            Err(err) => return Err(err),
                        };
                        Ok((from_meta, to_meta))
                    }
                })
                .await
                .map_err(new_std_io_error)?;
            if from_meta.is_dir() {
                return Err(Error::new(
                    ErrorKind::IsADirectory,
                    "copy source is a directory",
                ));
            }
            if let Some(to_meta) = to_meta {
                if to_meta.is_dir() {
                    return Err(Error::new(
                        ErrorKind::IsADirectory,
                        "copy target is a directory",
                    ));
                }
                if is_same_file(&from, &from_meta, &to_path, &to_meta) {
                    return Err(Error::new(
                        ErrorKind::IsSameFile,
                        "copy source and destination refer to the same file",
                    ));
                }
            }
            let to = core.prepare_write_path(&to).await?;
            core.dispatch({
                let core = core.clone();
                move || async move {
                    let from = OpenOptions::new().read(true).open(from).await?;
                    let to = OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(true)
                        .open(to)
                        .await?;

                    // AsyncReadRent and AsyncWriteRent is not implemented
                    // for File, so we can't write this:
                    // monoio::io::copy(&mut from, &mut to).await?;

                    let mut pos = 0;
                    // allocate and resize buffer
                    let mut buf = core.buf_pool.get();
                    // set capacity of buf to exact size to avoid excessive read
                    buf.reserve(BUFFER_SIZE);
                    let _ = buf.split_off(BUFFER_SIZE);

                    loop {
                        let result;
                        (result, buf) = from.read_at(buf, pos).await;
                        if result? == 0 {
                            // EOF
                            break;
                        }
                        let result;
                        (result, buf) = to.write_all_at(buf, pos).await;
                        result?;
                        pos += buf.len() as u64;
                        buf.clear();
                    }
                    core.buf_pool.put(buf);
                    Ok(pos)
                }
            })
            .await
            .map_err(|err: io::Error| {
                if err.kind() == io::ErrorKind::IsADirectory {
                    Error::new(ErrorKind::IsADirectory, "copy path is a directory").set_source(err)
                } else {
                    new_std_io_error(err)
                }
            })
            .map(|size| {
                let metadata = MetadataBuilder::file(size);
                metadata.build()
            })
        });

        Ok(copier)
    }

    async fn presign(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpPresign,
    ) -> Result<RpPresign> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }
}

fn is_same_file(
    from: &Path,
    from_meta: &monoio::fs::Metadata,
    to: &Path,
    to_meta: &monoio::fs::Metadata,
) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let _ = (from, to);
        from_meta.dev() == to_meta.dev() && from_meta.ino() == to_meta.ino()
    }
    #[cfg(not(unix))]
    {
        let _ = (from_meta, to_meta);
        matches!(
            (std::fs::canonicalize(from), std::fs::canonicalize(to)),
            (Ok(from_canon), Ok(to_canon)) if from_canon == to_canon
        )
    }
}

#[cfg(target_os = "linux")]
fn parse_conditional_rename_error(err: rustix::io::Errno) -> Error {
    use rustix::io::Errno;

    match err {
        Errno::EXIST | Errno::NOTEMPTY => Error::new(
            ErrorKind::ConditionNotMatch,
            "The destination already exists",
        )
        .set_source(std::io::Error::from(err)),
        Errno::NOSYS | Errno::INVAL | Errno::OPNOTSUPP => Error::new(
            ErrorKind::Unsupported,
            "The kernel or filesystem does not support conditional rename",
        )
        .set_source(err),
        _ => new_std_io_error(err.into()),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::MONOIOFS_SCHEME;

    #[test]
    fn test_builder_config_validation() {
        let temp = tempfile::tempdir().unwrap();
        let file_path = temp.path().join("not_a_dir");
        std::fs::write(&file_path, b"data").unwrap();
        let valid_root = temp.path().join("valid_root");
        std::fs::create_dir(&valid_root).unwrap();

        let err = MonoiofsBuilder::default().root("").build().err().unwrap();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid, "{err:?}");

        let cfg = MonoiofsConfig::from_iter([("root".to_string(), String::new())]).unwrap();
        let err = cfg.into_builder().build().err().unwrap();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid, "{err:?}");

        let err = MonoiofsBuilder::default()
            .root(file_path.to_str().unwrap())
            .build()
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid, "{err:?}");

        let backend = MonoiofsBuilder::default()
            .root(valid_root.to_str().unwrap())
            .build()
            .unwrap();
        assert_eq!(backend.info().scheme(), MONOIOFS_SCHEME);

        let missing_root = temp.path().join("created_root");
        let backend = MonoiofsBuilder::default()
            .root(missing_root.to_str().unwrap())
            .build()
            .unwrap();
        assert_eq!(backend.info().scheme(), MONOIOFS_SCHEME);
        assert!(missing_root.is_dir());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_copy_same_file_alias_preserves_data() {
        tokio::time::timeout(Duration::from_secs(30), async {
            let temp_dir = tempfile::tempdir().unwrap();
            let root = temp_dir.path();
            std::fs::write(root.join("real.txt"), b"important data").unwrap();
            std::os::unix::fs::symlink("real.txt", root.join("link.txt")).unwrap();
            std::fs::hard_link(root.join("real.txt"), root.join("hard.txt")).unwrap();
            std::fs::create_dir(root.join("nested")).unwrap();
            std::os::unix::fs::symlink("../real.txt", root.join("nested/link.txt")).unwrap();

            let op =
                Operator::new(MonoiofsBuilder::default().root(root.to_str().unwrap())).unwrap();

            for (from, to) in [
                ("real.txt", "link.txt"),
                ("link.txt", "real.txt"),
                ("real.txt", "hard.txt"),
                ("hard.txt", "real.txt"),
                ("link.txt", "hard.txt"),
                ("real.txt", "nested/link.txt"),
            ] {
                let err = op.copy(from, to).await.unwrap_err();
                assert_eq!(err.kind(), ErrorKind::IsSameFile, "{from} -> {to}: {err:?}");
                assert!(!err.is_temporary(), "{from} -> {to}: {err:?}");
                assert_eq!(
                    std::fs::read(root.join("real.txt")).unwrap(),
                    b"important data",
                    "{from} -> {to} must preserve data"
                );
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_copy_source_dir_preserves_destination() {
        let temp_dir = tempfile::tempdir().unwrap();
        let op = Operator::new(MonoiofsBuilder::default().root(temp_dir.path().to_str().unwrap()))
            .unwrap();

        op.create_dir("dir/").await.unwrap();
        op.write("dst", "existing content").await.unwrap();

        let err = op.copy("dir", "dst").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::IsADirectory);
        assert_eq!(
            op.read("dst").await.unwrap().to_bytes(),
            b"existing content".as_slice()
        );

        let err = op.copy("dir", "new_dir/missing").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::IsADirectory);
        assert!(!temp_dir.path().join("new_dir").exists());
    }

    #[tokio::test]
    async fn test_copy_dest_dir_and_missing_source() {
        let temp_dir = tempfile::tempdir().unwrap();
        let op = Operator::new(MonoiofsBuilder::default().root(temp_dir.path().to_str().unwrap()))
            .unwrap();

        op.write("src", "hello").await.unwrap();
        op.create_dir("dir/").await.unwrap();

        let err = op.copy("src", "dir").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::IsADirectory);

        let err = op.copy("missing", "missing_parent/dst").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound);
        assert!(!temp_dir.path().join("missing_parent").exists());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_conditional_rename_errno_mapping() {
        use rustix::io::Errno;

        for (errno, kind) in [
            (Errno::EXIST, ErrorKind::ConditionNotMatch),
            (Errno::NOTEMPTY, ErrorKind::ConditionNotMatch),
            (Errno::NOSYS, ErrorKind::Unsupported),
            (Errno::INVAL, ErrorKind::Unsupported),
            (Errno::OPNOTSUPP, ErrorKind::Unsupported),
            (Errno::NOENT, ErrorKind::NotFound),
            (Errno::NOTDIR, ErrorKind::NotADirectory),
            (Errno::ISDIR, ErrorKind::IsADirectory),
        ] {
            let err = parse_conditional_rename_error(errno);
            assert_eq!(err.kind(), kind, "{errno:?}");
            assert!(!err.is_temporary(), "{err:?}");
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_rename_with_if_not_exists() {
        let temp_dir = tempfile::tempdir().unwrap();
        let root = temp_dir.path();
        let backend = MonoiofsBuilder::default()
            .root(root.to_str().unwrap())
            .build()
            .unwrap();
        let op = Operator::new(MonoiofsBuilder::default().root(root.to_str().unwrap())).unwrap();
        assert!(op.info().capability().rename_with_if_not_exists);

        // Basic success.
        op.write("src", "source data").await.unwrap();
        op.rename_with("src", "dst")
            .if_not_exists(true)
            .await
            .unwrap();
        assert!(!root.join("src").exists());
        assert_eq!(std::fs::read(root.join("dst")).unwrap(), b"source data");

        // Nested destination creates parent directories.
        op.write("nested_src", "nested data").await.unwrap();
        op.rename_with("nested_src", "a/b/nested_dst")
            .if_not_exists(true)
            .await
            .unwrap();
        assert!(!root.join("nested_src").exists());
        assert_eq!(
            std::fs::read(root.join("a/b/nested_dst")).unwrap(),
            b"nested data"
        );

        // Existing destination file returns ConditionNotMatch and preserves both files.
        op.write("conflict_src", "new content").await.unwrap();
        op.write("conflict_dst", "existing content").await.unwrap();
        let err = op
            .rename_with("conflict_src", "conflict_dst")
            .if_not_exists(true)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch, "{err:?}");
        assert!(!err.is_temporary(), "{err:?}");
        assert_eq!(
            std::fs::read(root.join("conflict_src")).unwrap(),
            b"new content"
        );
        assert_eq!(
            std::fs::read(root.join("conflict_dst")).unwrap(),
            b"existing content"
        );

        // Existing non-empty destination directory returns ConditionNotMatch and preserves both.
        std::fs::create_dir(root.join("existing_dir")).unwrap();
        std::fs::write(root.join("existing_dir/child"), b"keep").unwrap();
        let err = op
            .rename_with("conflict_src", "existing_dir")
            .if_not_exists(true)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch, "{err:?}");
        assert!(!err.is_temporary(), "{err:?}");
        assert_eq!(
            std::fs::read(root.join("conflict_src")).unwrap(),
            b"new content"
        );
        assert_eq!(
            std::fs::read(root.join("existing_dir/child")).unwrap(),
            b"keep"
        );

        // Self-rename via Operator returns IsSameFile; direct backend call and hard-link alias
        // hit RENAME_NOREPLACE and return ConditionNotMatch while preserving the file.
        op.write("self_file", "self content").await.unwrap();
        let err = op
            .rename_with("self_file", "self_file")
            .if_not_exists(true)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::IsSameFile, "{err:?}");
        let err = backend
            .rename(
                &OperationContext::default(),
                "self_file",
                "self_file",
                OpRename::new().with_if_not_exists(true),
            )
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch, "{err:?}");
        assert!(!err.is_temporary(), "{err:?}");
        std::fs::hard_link(root.join("self_file"), root.join("self_hard")).unwrap();
        let err = op
            .rename_with("self_file", "self_hard")
            .if_not_exists(true)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch, "{err:?}");
        assert_eq!(
            std::fs::read(root.join("self_file")).unwrap(),
            b"self content"
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_conditional_rename_source_and_path_errors() {
        let temp_dir = tempfile::tempdir().unwrap();
        let root = temp_dir.path();
        let op = Operator::new(MonoiofsBuilder::default().root(root.to_str().unwrap())).unwrap();

        // Missing source returns NotFound without creating destination parent directories.
        let err = op
            .rename_with("missing", "ghost/dir/dst")
            .if_not_exists(true)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound, "{err:?}");
        assert!(!err.is_temporary(), "{err:?}");
        assert!(!root.join("ghost").exists());

        // Directory source (with or without trailing slash) returns IsADirectory without creating
        // destination parent directories.
        std::fs::create_dir(root.join("src_dir")).unwrap();
        std::fs::write(root.join("src_dir/child"), b"dir child").unwrap();
        for from in ["src_dir", "src_dir/"] {
            let err = op
                .rename_with(from, "dir_ghost/dst")
                .if_not_exists(true)
                .await
                .unwrap_err();
            assert_eq!(err.kind(), ErrorKind::IsADirectory, "{from}: {err:?}");
            assert!(!err.is_temporary(), "{from}: {err:?}");
            assert!(!root.join("dir_ghost").exists());
        }
        assert_eq!(
            std::fs::read(root.join("src_dir/child")).unwrap(),
            b"dir child"
        );

        // Non-directory path components return permanent errors and preserve existing files.
        std::fs::write(root.join("source"), b"source contents").unwrap();
        std::fs::write(root.join("file"), b"parent contents").unwrap();
        for (from, to, kind) in [
            ("file/child", "target", ErrorKind::NotADirectory),
            ("source", "file/child", ErrorKind::AlreadyExists),
            ("source", "file/dir/child", ErrorKind::NotADirectory),
        ] {
            let err = op
                .rename_with(from, to)
                .if_not_exists(true)
                .await
                .unwrap_err();
            assert_eq!(err.kind(), kind, "{from} -> {to}: {err:?}");
            assert!(!err.is_temporary(), "{from} -> {to}: {err:?}");
        }
        assert_eq!(
            std::fs::read(root.join("source")).unwrap(),
            b"source contents"
        );
        assert_eq!(
            std::fs::read(root.join("file")).unwrap(),
            b"parent contents"
        );
    }
}
