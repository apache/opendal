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

use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;

use compio::dispatcher::Dispatcher;
use compio::fs::OpenOptions;

use super::COMPFS_SCHEME;
use super::config::CompfsConfig;
use super::core::CompfsCore;
use super::deleter::CompfsDeleter;
use super::reader::*;
use opendal_core::raw::*;
use opendal_core::*;

/// [`compio`]-based file system support.
#[derive(Debug, Default)]
pub struct CompfsBuilder {
    pub(super) config: CompfsConfig,
}

impl CompfsBuilder {
    /// Set root for Compfs
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }
}

impl Builder for CompfsBuilder {
    type Config = CompfsConfig;

    fn build(self) -> Result<impl Service> {
        let root = match self.config.root.filter(|s| !s.is_empty()) {
            Some(root) => Ok(root),
            None => Err(
                Error::new(ErrorKind::ConfigInvalid, "root is not specified")
                    .with_operation("Builder::build"),
            ),
        }?;

        // If root dir does not exist, we must create it.
        match std::fs::metadata(&root) {
            Ok(meta) if !meta.is_dir() => {
                return Err(
                    Error::new(ErrorKind::ConfigInvalid, "root is not a directory")
                        .with_operation("Builder::build")
                        .with_context("root", root.as_str()),
                );
            }
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir_all(&root).map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "create root dir failed")
                        .with_operation("Builder::build")
                        .with_context("root", root.as_str())
                        .set_source(e)
                })?;
            }
            Err(_) => {}
        }

        let dispatcher = Dispatcher::new().map_err(|_| {
            Error::new(
                ErrorKind::Unexpected,
                "failed to initiate compio dispatcher",
            )
        })?;
        let core = CompfsCore {
            info: ServiceInfo::new(COMPFS_SCHEME, &root, ""),
            capability: Capability {
                stat: true,

                read: true,

                write: true,
                write_can_empty: true,
                write_can_multi: true,
                create_dir: true,
                delete: true,

                list: true,

                copy: true,
                rename: true,
                rename_with_if_not_exists: cfg!(target_os = "linux"),

                shared: true,

                ..Default::default()
            },
            root: root.into(),
            dispatcher,
            buf_pool: oio::PooledBuf::new(16),
        };
        Ok(CompfsBackend {
            core: Arc::new(core),
        })
    }
}

#[derive(Clone, Debug)]
pub struct CompfsBackend {
    pub(crate) core: Arc<CompfsCore>,
}

impl Service for CompfsBackend {
    type Reader = oio::PositionReader<CompfsReader>;
    type Writer = CompfsLazyWriter;
    type Lister = CompfsLazyLister;
    type Deleter = oio::OneShotDeleter<CompfsDeleter>;
    type Copier = oio::OneShotCopier;
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        self.core.info.clone()
    }

    fn capability(&self) -> Capability {
        self.core.capability
    }

    async fn create_dir(
        &self,
        _ctx: &OperationContext,
        path: &str,
        _: OpCreateDir,
    ) -> Result<RpCreateDir> {
        let path = self.core.prepare_path(path)?;

        self.core
            .exec(move || async move { compio::fs::create_dir_all(path).await })
            .await?;

        Ok(RpCreateDir::default())
    }

    async fn stat(&self, _ctx: &OperationContext, path: &str, _: OpStat) -> Result<RpStat> {
        let path = self.core.prepare_path(path)?;
        let meta = self
            .core
            .exec(move || async move { compio::fs::metadata(path).await })
            .await?;
        let ty = meta.file_type();
        let mode = if ty.is_dir() {
            EntryMode::DIR
        } else if ty.is_file() {
            EntryMode::FILE
        } else {
            EntryMode::Unknown
        };
        let last_mod = Timestamp::try_from(meta.modified().map_err(new_std_io_error)?)?;
        let mut ret = match mode {
            EntryMode::FILE => MetadataBuilder::file(meta.len()),
            EntryMode::DIR => MetadataBuilder::dir(),
            EntryMode::Unknown => MetadataBuilder::unknown(),
        };
        ret.last_modified(last_mod);
        Ok(RpStat::new(ret.build()))
    }

    fn delete(&self, _ctx: &OperationContext) -> Result<Self::Deleter> {
        let output: oio::OneShotDeleter<CompfsDeleter> = {
            Ok(oio::OneShotDeleter::new(CompfsDeleter::new(
                self.core.clone(),
            )))
        }?;

        Ok(output)
    }

    fn copy(
        &self,
        _ctx: &OperationContext,
        from: &str,
        to: &str,
        _: OpCopy,
    ) -> Result<Self::Copier> {
        let core = self.core.clone();
        let from = self.core.prepare_path(from)?;
        let to = self.core.prepare_path(to)?;

        Ok(oio::OneShotCopier::new(async move {
            let (from_meta, to_meta) = core
                .exec({
                    let from = from.clone();
                    let to = to.clone();
                    move || async move {
                        let from_meta = compio::fs::metadata(&from).await?;
                        if from_meta.is_dir() {
                            return Ok((from_meta, None));
                        }
                        let to_meta = match compio::fs::metadata(&to).await {
                            Ok(meta) => Some(meta),
                            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
                            Err(err) => return Err(err),
                        };
                        Ok((from_meta, to_meta))
                    }
                })
                .await?;
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
                if is_same_file(&core, &from, &from_meta, &to, &to_meta).await? {
                    return Err(Error::new(
                        ErrorKind::IsSameFile,
                        "copy source and destination refer to the same file",
                    ));
                }
            }

            core.exec(move || async move {
                let from = OpenOptions::new().read(true).open(from).await?;
                if let Some(parent) = to.parent() {
                    compio::fs::create_dir_all(parent).await?;
                }
                let to = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(to)
                    .await?;

                let (mut from, mut to) = (Cursor::new(from), Cursor::new(to));
                let size = compio::io::copy(&mut from, &mut to).await?;

                let metadata = MetadataBuilder::file(size);
                Ok(metadata.build())
            })
            .await
        }))
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
        let to = self.core.prepare_path(to)?;

        #[cfg(target_os = "linux")]
        if args.if_not_exists() {
            use rustix::fs::{CWD, RenameFlags, renameat_with};

            self.core
                .exec_blocking(move || {
                    std::fs::metadata(&from).map_err(parse_conditional_rename_path_error)?;
                    if let Some(parent) = to.parent() {
                        std::fs::create_dir_all(parent)
                            .map_err(parse_conditional_rename_path_error)?;
                    }
                    renameat_with(CWD, &from, CWD, &to, RenameFlags::NOREPLACE)
                        .map_err(parse_conditional_rename_error)
                })
                .await??;

            return Ok(RpRename::default());
        }

        self.core
            .exec(move || async move {
                compio::fs::metadata(&from).await?;
                if let Some(parent) = to.parent() {
                    compio::fs::create_dir_all(parent).await?;
                }
                compio::fs::rename(from, to).await
            })
            .await?;

        Ok(RpRename::default())
    }
    fn read(&self, _ctx: &OperationContext, path: &str, _: OpRead) -> Result<Self::Reader> {
        Ok(oio::PositionReader::new(CompfsReader::new(
            self.core.clone(),
            self.core.prepare_path(path)?,
        )))
    }

    fn write(&self, _ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        Ok(CompfsLazyWriter::new(
            self.core.clone(),
            self.core.prepare_path(path)?,
            args,
        ))
    }

    fn list(&self, _ctx: &OperationContext, path: &str, _: OpList) -> Result<Self::Lister> {
        Ok(CompfsLazyLister::new(
            self.core.clone(),
            self.core.prepare_path(path)?,
        ))
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

async fn is_same_file(
    core: &CompfsCore,
    from: &Path,
    from_meta: &compio::fs::Metadata,
    to: &Path,
    to_meta: &compio::fs::Metadata,
) -> Result<bool> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let _ = (core, from, to);
        Ok(from_meta.dev() == to_meta.dev() && from_meta.ino() == to_meta.ino())
    }
    #[cfg(not(unix))]
    {
        let _ = (from_meta, to_meta);
        let from = from.to_path_buf();
        let to = to.to_path_buf();
        core.exec_blocking(move || {
            matches!(
                (std::fs::canonicalize(from), std::fs::canonicalize(to)),
                (Ok(from_canon), Ok(to_canon)) if from_canon == to_canon
            )
        })
        .await
    }
}

#[cfg(target_os = "linux")]
fn parse_conditional_rename_path_error(err: std::io::Error) -> Error {
    let kind = match err.kind() {
        std::io::ErrorKind::IsADirectory => ErrorKind::IsADirectory,
        std::io::ErrorKind::NotADirectory => ErrorKind::NotADirectory,
        _ => return new_std_io_error(err),
    };
    Error::new(kind, err.kind().to_string()).set_source(err)
}

#[cfg(target_os = "linux")]
fn parse_conditional_rename_error(err: rustix::io::Errno) -> Error {
    use rustix::io::Errno;

    match err {
        Errno::EXIST => Error::new(
            ErrorKind::ConditionNotMatch,
            "The destination already exists",
        )
        .set_source(std::io::Error::from(err)),
        Errno::NOSYS | Errno::INVAL | Errno::OPNOTSUPP => Error::new(
            ErrorKind::Unsupported,
            "The kernel or filesystem does not support conditional rename",
        )
        .set_source(err),
        _ => parse_conditional_rename_path_error(err.into()),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_builder_config_validation() {
        let temp = tempfile::tempdir().unwrap();
        let file_path = temp.path().join("not_a_dir");
        std::fs::write(&file_path, b"data").unwrap();
        let valid_root = temp.path().join("valid_root");
        std::fs::create_dir(&valid_root).unwrap();

        let err = CompfsBuilder::default().root("").build().err().unwrap();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid, "{err:?}");

        let cfg = CompfsConfig::from_iter([("root".to_string(), String::new())]).unwrap();
        let err = cfg.into_builder().build().err().unwrap();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid, "{err:?}");

        let err = CompfsBuilder::default()
            .root(file_path.to_str().unwrap())
            .build()
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid, "{err:?}");

        let backend = CompfsBuilder::default()
            .root(valid_root.to_str().unwrap())
            .build()
            .unwrap();
        assert_eq!(backend.info().scheme(), COMPFS_SCHEME);

        let missing_root = temp.path().join("created_root");
        let backend = CompfsBuilder::default()
            .root(missing_root.to_str().unwrap())
            .build()
            .unwrap();
        assert_eq!(backend.info().scheme(), COMPFS_SCHEME);
        assert!(missing_root.is_dir());
    }

    #[tokio::test]
    async fn test_copy_target_directory_returns_is_a_directory() {
        tokio::time::timeout(Duration::from_secs(30), async {
            let temp_dir = tempfile::tempdir().unwrap();
            let root = temp_dir.path();
            std::fs::write(root.join("src"), b"source contents").unwrap();
            std::fs::create_dir(root.join("dir")).unwrap();
            let op = Operator::new(CompfsBuilder::default().root(root.to_str().unwrap())).unwrap();

            let err = op.copy("src", "dir").await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::IsADirectory, "{err:?}");
            assert!(!err.is_temporary(), "{err:?}");
            assert_eq!(std::fs::read(root.join("src")).unwrap(), b"source contents");
        })
        .await
        .unwrap();
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

            let op = Operator::new(CompfsBuilder::default().root(root.to_str().unwrap())).unwrap();

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

    #[cfg(target_os = "linux")]
    #[test]
    fn test_conditional_rename_errno_mapping() {
        use rustix::io::Errno;

        for (errno, kind) in [
            (Errno::EXIST, ErrorKind::ConditionNotMatch),
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
    async fn test_conditional_rename_rejects_file_as_parent() {
        let temp_dir = tempfile::tempdir().unwrap();
        let root = temp_dir.path();
        std::fs::write(root.join("source"), b"source contents").unwrap();
        std::fs::write(root.join("file"), b"parent contents").unwrap();
        let op = Operator::new(CompfsBuilder::default().root(root.to_str().unwrap())).unwrap();

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
            assert!(!err.is_temporary(), "{err:?}");
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

    #[tokio::test]
    async fn test_copy_directory_source_preserves_destination() {
        let temp_dir = tempfile::tempdir().unwrap();
        let root = temp_dir.path();
        std::fs::create_dir(root.join("dir")).unwrap();
        std::fs::write(root.join("dir/child"), b"source contents").unwrap();
        std::fs::write(root.join("dst"), b"destination contents").unwrap();
        let op = Operator::new(CompfsBuilder::default().root(root.to_str().unwrap())).unwrap();

        let err = op.copy("dir", "dst").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::IsADirectory, "{err:?}");
        assert_eq!(
            std::fs::read(root.join("dst")).unwrap(),
            b"destination contents"
        );
    }

    #[tokio::test]
    async fn test_copy_directory_source_does_not_create_destination() {
        let temp_dir = tempfile::tempdir().unwrap();
        let root = temp_dir.path();
        std::fs::create_dir(root.join("dir")).unwrap();
        let op = Operator::new(CompfsBuilder::default().root(root.to_str().unwrap())).unwrap();

        let err = op.copy("dir", "new_dir/missing").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::IsADirectory, "{err:?}");
        assert!(!root.join("new_dir").exists());
    }

    #[tokio::test]
    async fn test_rename_missing_source_does_not_create_destination_parent() {
        let temp_dir = tempfile::tempdir().unwrap();
        let root = temp_dir.path();
        let op = Operator::new(CompfsBuilder::default().root(root.to_str().unwrap())).unwrap();

        let err = op.rename("missing", "ghost/dir/dst").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound, "{err:?}");
        assert!(!root.join("ghost").exists());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_conditional_rename_missing_source_does_not_create_destination_parent() {
        let temp_dir = tempfile::tempdir().unwrap();
        let root = temp_dir.path();
        std::fs::write(root.join("file"), b"file contents").unwrap();
        let op = Operator::new(CompfsBuilder::default().root(root.to_str().unwrap())).unwrap();

        let err = op
            .rename_with("missing", "ghost/dir/dst")
            .if_not_exists(true)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound, "{err:?}");
        assert!(!root.join("ghost").exists());

        let err = op
            .rename_with("missing", "file/target")
            .if_not_exists(true)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound, "{err:?}");
        assert_eq!(std::fs::read(root.join("file")).unwrap(), b"file contents");
    }
}
