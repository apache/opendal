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
use std::path::Path;
use std::path::PathBuf;

use opendal_core::raw::*;
use opendal_core::*;

#[derive(Debug)]
pub struct FsCore {
    pub info: ServiceInfo,
    pub capability: Capability,
    pub root: PathBuf,
    pub atomic_write_dir: Option<PathBuf>,
    pub direct_io: bool,
    pub buf_pool: oio::PooledBuf,
}

impl FsCore {
    #[inline]
    pub fn root_join(&self, path: &str) -> Result<PathBuf> {
        confined_join(&self.root, path)
    }

    // Build write path and ensure the parent dirs created
    pub async fn ensure_write_abs_path(&self, parent: &Path, path: &str) -> Result<PathBuf> {
        self.ensure_write_abs_path_with(parent, path, new_std_io_error)
            .await
    }

    async fn ensure_write_abs_path_with(
        &self,
        parent: &Path,
        path: &str,
        map_io_error: impl FnOnce(std::io::Error) -> Error,
    ) -> Result<PathBuf> {
        let p = confined_join(parent, path)?;

        // Create dir before write path.
        //
        // TODO(xuanwo): There are many works to do here:
        //   - Is it safe to create dir concurrently?
        //   - Do we need to extract this logic as new util functions?
        //   - Is it better to check the parent dir exists before call mkdir?
        let parent = PathBuf::from(&p)
            .parent()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "path should have parent but not, it must be malformed",
                )
                .with_context("input", p.to_string_lossy())
            })?
            .to_path_buf();

        tokio::fs::create_dir_all(&parent)
            .await
            .map_err(map_io_error)?;

        Ok(p)
    }

    pub async fn fs_create_dir(&self, path: &str) -> Result<()> {
        let p = self.root_join(path)?;
        tokio::fs::create_dir_all(&p)
            .await
            .map_err(new_std_io_error)?;
        Ok(())
    }

    pub async fn fs_stat(&self, path: &str) -> Result<Metadata> {
        let p = self.root_join(path)?;
        let meta = tokio::fs::metadata(&p).await.map_err(new_std_io_error)?;
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

        // Read user metadata from xattr on Unix systems
        #[cfg(unix)]
        {
            let user_metadata = Self::get_user_metadata(&p)?;
            if !user_metadata.is_empty() {
                m.user_metadata(user_metadata);
            }
        }

        Ok(m.build())
    }

    pub async fn fs_list(&self, path: &str) -> Result<Option<tokio::fs::ReadDir>> {
        let p = self.root_join(path)?;

        match tokio::fs::read_dir(&p).await {
            Ok(rd) => Ok(Some(rd)),
            Err(e) => {
                match e.kind() {
                    // Return empty list if the directory not found
                    std::io::ErrorKind::NotFound => Ok(None),
                    // If the path is not a directory, return an empty list
                    //
                    // The path could be a file or a symbolic link in this case.
                    // Returning a NotADirectory error to the user isn't helpful; instead,
                    // providing an empty directory is a more user-friendly. In fact, the dir
                    // `path/` does not exist.
                    std::io::ErrorKind::NotADirectory => Ok(None),
                    _ => Err(new_std_io_error(e)),
                }
            }
        }
    }

    pub async fn fs_copy(&self, from: &str, to: &str) -> Result<u64> {
        let from = self.root_join(from)?;
        // try to get the metadata of the source file to ensure it exists
        tokio::fs::metadata(&from).await.map_err(new_std_io_error)?;

        let to = self
            .ensure_write_abs_path(&self.root, to.trim_end_matches('/'))
            .await?;

        let size = tokio::fs::copy(&from, &to)
            .await
            .map_err(new_std_io_error)?;

        // only *nix supports `write_with_user_metadata`
        #[cfg(unix)]
        {
            if let Ok(user_meta) = Self::get_user_metadata(&from)
                && !user_meta.is_empty()
            {
                Self::set_user_metadata(&to, &user_meta)?;
            }
        }

        Ok(size)
    }

    pub async fn fs_rename(&self, from: &str, to: &str, if_not_exists: bool) -> Result<()> {
        #[cfg(not(target_os = "linux"))]
        if if_not_exists {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "Conditional rename is only supported on Linux",
            ));
        }

        let map_path_error = |err| {
            #[cfg(target_os = "linux")]
            if if_not_exists {
                return parse_conditional_rename_directory_error(err, new_std_io_error);
            }
            new_std_io_error(err)
        };

        let from = self.root_join(from)?;
        tokio::fs::metadata(&from).await.map_err(map_path_error)?;

        let to = self
            .ensure_write_abs_path_with(&self.root, to.trim_end_matches('/'), map_path_error)
            .await?;

        #[cfg(target_os = "linux")]
        if if_not_exists {
            return tokio::task::spawn_blocking(move || rename_noreplace(&from, &to))
                .await
                .map_err(new_task_join_error)?;
        }

        tokio::fs::rename(from, to)
            .await
            .map_err(new_std_io_error)?;
        Ok(())
    }

    /// Get user metadata from extended attributes on Unix systems.
    ///
    /// Reads xattr in the "user." namespace and strips the prefix, allowing
    /// interoperability with other applications that set user xattr.
    #[cfg(unix)]
    pub fn get_user_metadata(path: &Path) -> Result<HashMap<String, String>> {
        let mut user_metadata = HashMap::new();

        let attrs = match xattr::list(path) {
            Ok(attrs) => attrs,
            Err(e) => {
                // xattr may not be supported on some filesystems, ignore the error
                if e.kind() == std::io::ErrorKind::Unsupported {
                    return Ok(user_metadata);
                }
                // Also ignore "operation not supported" errors (ENOTSUP/EOPNOTSUPP)
                // which may occur on filesystems that don't support xattr
                if let Some(os_error) = e.raw_os_error() {
                    // ENOTSUP (95) and EOPNOTSUPP (95) on Linux
                    if os_error == 95 {
                        return Ok(user_metadata);
                    }
                }
                return Err(new_std_io_error(e));
            }
        };

        for attr in attrs {
            let attr_name = attr.to_string_lossy();
            // Only read xattr in the "user." namespace and strip the prefix
            if let Some(key) = attr_name.strip_prefix(XATTR_USER_PREFIX)
                && let Ok(Some(value)) = xattr::get(path, &attr)
                && let Ok(v) = String::from_utf8(value)
            {
                user_metadata.insert(key.to_string(), v);
            }
        }

        Ok(user_metadata)
    }

    /// Set user metadata as extended attributes on Unix systems.
    ///
    /// Keys are automatically prefixed with "user." to comply with Linux xattr
    /// naming requirements. This allows interoperability with other applications.
    #[cfg(unix)]
    pub fn set_user_metadata(path: &Path, user_metadata: &HashMap<String, String>) -> Result<()> {
        for (key, value) in user_metadata {
            let attr_name = format!("{XATTR_USER_PREFIX}{key}");
            xattr::set(path, &attr_name, value.as_bytes()).map_err(new_std_io_error)?;
        }
        Ok(())
    }
}

/// Prefix for user xattr namespace on Linux.
/// Using "user." as the standard namespace for user-defined attributes.
#[cfg(unix)]
const XATTR_USER_PREFIX: &str = "user.";

#[cfg(target_os = "linux")]
pub(super) fn rename_noreplace(from: &Path, to: &Path) -> Result<()> {
    use rustix::fs::{CWD, RenameFlags, renameat_with};
    use rustix::io::Errno;

    renameat_with(CWD, from, CWD, to, RenameFlags::NOREPLACE).map_err(|err| match err {
        Errno::INVAL | Errno::NOSYS | Errno::OPNOTSUPP => Error::new(
            ErrorKind::Unsupported,
            "The kernel or filesystem does not support conditional rename",
        )
        .set_source(err),
        _ => parse_conditional_rename_directory_error(err.into(), |err| {
            parse_error(ErrorContext::new(ServiceOperation("Rename")), err)
        }),
    })
}

#[cfg(target_os = "linux")]
fn parse_conditional_rename_directory_error(
    err: std::io::Error,
    fallback: impl FnOnce(std::io::Error) -> Error,
) -> Error {
    use rustix::io::Errno;

    let kind = match err.raw_os_error().map(Errno::from_raw_os_error) {
        Some(Errno::ISDIR) => ErrorKind::IsADirectory,
        Some(Errno::NOTDIR) => ErrorKind::NotADirectory,
        _ => return fallback(err),
    };

    Error::new(kind, err.kind().to_string())
        .set_source(err)
        .with_context("service_operation", "Rename")
}

/// Context needed to classify an error from this service.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ErrorContext {
    service_operation: ServiceOperation,
}

impl ErrorContext {
    pub(crate) const fn new(service_operation: ServiceOperation) -> Self {
        Self { service_operation }
    }
}

/// Parse an error using its service operation context.
pub(crate) fn parse_error(ctx: ErrorContext, err: std::io::Error) -> Error {
    let err = match err.kind() {
        std::io::ErrorKind::AlreadyExists => Error::new(
            ErrorKind::ConditionNotMatch,
            "The file already exists in the filesystem",
        )
        .set_source(err),
        _ => new_std_io_error(err),
    };

    err.with_context("service_operation", ctx.service_operation.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    #[test]
    fn test_conditional_rename_directory_errno_mapping() {
        use rustix::io::Errno;
        use std::error::Error as _;

        for (errno, kind) in [
            (Errno::ISDIR, ErrorKind::IsADirectory),
            (Errno::NOTDIR, ErrorKind::NotADirectory),
        ] {
            let err = parse_conditional_rename_directory_error(errno.into(), new_std_io_error);
            assert_eq!(err.kind(), kind);
            assert!(!err.is_temporary());
            let source = err
                .source()
                .unwrap()
                .downcast_ref::<std::io::Error>()
                .unwrap();
            assert_eq!(source.raw_os_error(), Some(errno.raw_os_error()));
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_conditional_rename_rejects_non_directory_path_components() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let root = temp_dir.path();
        std::fs::write(root.join("file"), b"data").unwrap();
        let op = Operator::new(crate::Fs::default().root(root.to_str().unwrap())).unwrap();

        for (from, to) in [("file/child", "target"), ("file", "file/dir/target")] {
            let err = op
                .rename_with(from, to)
                .if_not_exists(true)
                .await
                .unwrap_err();
            assert_eq!(err.kind(), ErrorKind::NotADirectory, "{err:?}");
            assert!(!err.is_temporary(), "{err:?}");
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_fs_backend_copy_preserves_user_metadata() {
        use opendal_core::Operator;

        let temp_dir = tempfile::TempDir::new().unwrap();
        let root = temp_dir.path();

        let src = "src_meta.txt";
        let dst = "dst_meta.txt";

        let src_path = root.join(src);
        let dst_path = root.join(dst);

        std::fs::File::create(&src_path).unwrap();

        let mut meta = HashMap::new();
        meta.insert("key".to_string(), "preserved123".to_string());
        FsCore::set_user_metadata(&src_path, &meta).unwrap();

        let op = Operator::new(crate::Fs::default().root(root.to_str().unwrap())).unwrap();
        op.copy(src, dst).await.unwrap();

        let got = FsCore::get_user_metadata(&dst_path).unwrap();
        assert_eq!(got.get("key").map(String::as_str), Some("preserved123"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_delete_iter_normalizes_absolute_path_under_root() {
        use opendal_core::Operator;

        let root_dir = tempfile::TempDir::new().unwrap();
        let outside_dir = tempfile::TempDir::new().unwrap();

        let outside = outside_dir.path().join("outside.txt");
        std::fs::write(&outside, b"content").unwrap();

        let inside = root_dir.path().join(outside.strip_prefix("/").unwrap());
        std::fs::create_dir_all(inside.parent().unwrap()).unwrap();
        std::fs::write(&inside, b"content").unwrap();

        let op =
            Operator::new(crate::Fs::default().root(root_dir.path().to_str().unwrap())).unwrap();

        op.delete_iter([outside.to_str().unwrap().to_string()])
            .await
            .unwrap();
        assert!(!inside.exists());
        assert_eq!(std::fs::read(&outside).unwrap(), b"content");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_list_includes_dangling_symlink() {
        use opendal_core::Operator;

        let temp_dir = tempfile::TempDir::new().unwrap();
        let root = temp_dir.path();

        std::fs::write(root.join("real"), b"data").unwrap();
        std::os::unix::fs::symlink("/nonexistent-target", root.join("linky")).unwrap();
        std::os::unix::fs::symlink(root.join("real"), root.join("valid-link")).unwrap();

        let op = Operator::new(crate::Fs::default().root(root.to_str().unwrap())).unwrap();
        let entries = op.list("/").await.unwrap();
        let modes: std::collections::HashMap<String, EntryMode> = entries
            .iter()
            .map(|e| (e.path().to_string(), e.metadata().mode()))
            .collect();

        assert_eq!(
            modes.get("linky"),
            Some(&EntryMode::Unknown),
            "dangling symlink must be listed as Unknown: {modes:?}"
        );
        assert_eq!(modes.get("real"), Some(&EntryMode::FILE));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_delete_removes_dangling_symlink() {
        use opendal_core::Operator;

        let temp_dir = tempfile::TempDir::new().unwrap();
        let root = temp_dir.path();
        std::os::unix::fs::symlink("nope.txt", root.join("dangle.txt")).unwrap();

        let op = Operator::new(crate::Fs::default().root(root.to_str().unwrap())).unwrap();

        op.delete("dangle.txt/").await.unwrap();
        assert!(
            std::fs::symlink_metadata(root.join("dangle.txt"))
                .unwrap()
                .is_symlink()
        );

        op.delete("dangle.txt").await.unwrap();
        assert_eq!(
            std::fs::symlink_metadata(root.join("dangle.txt"))
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::NotFound
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_delete_removes_symlink_to_dir_and_keeps_target() {
        use opendal_core::Operator;

        let temp_dir = tempfile::TempDir::new().unwrap();
        let root = temp_dir.path();
        std::fs::create_dir(root.join("realdir")).unwrap();
        std::fs::write(root.join("realdir").join("keep"), b"data").unwrap();
        let dir_link = root.join("linkdir");
        let op = Operator::new(crate::Fs::default().root(root.to_str().unwrap())).unwrap();
        for (path, recursive) in [("linkdir", false), ("linkdir/", false), ("linkdir/", true)] {
            std::os::unix::fs::symlink("realdir", &dir_link).unwrap();
            op.delete_with(path).recursive(recursive).await.unwrap();
            assert_eq!(
                std::fs::symlink_metadata(&dir_link).unwrap_err().kind(),
                std::io::ErrorKind::NotFound
            );
            assert_eq!(std::fs::read(root.join("realdir/keep")).unwrap(), b"data");
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_delete_file_symlink_requires_file_path() {
        use opendal_core::Operator;

        let temp_dir = tempfile::TempDir::new().unwrap();
        let root = temp_dir.path();
        let target = root.join("real.txt");
        let link = root.join("link.txt");
        std::fs::write(&target, b"data").unwrap();
        std::os::unix::fs::symlink("real.txt", &link).unwrap();

        let op = Operator::new(crate::Fs::default().root(root.to_str().unwrap())).unwrap();
        let err = op.delete("link.txt/").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotADirectory);
        assert!(std::fs::symlink_metadata(&link).unwrap().is_symlink());
        assert_eq!(std::fs::read(&target).unwrap(), b"data");

        op.delete("link.txt").await.unwrap();
        assert_eq!(
            std::fs::symlink_metadata(&link).unwrap_err().kind(),
            std::io::ErrorKind::NotFound
        );
        assert_eq!(std::fs::read(&target).unwrap(), b"data");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_list_symlink_to_dir_as_dir() {
        use opendal_core::Operator;

        let temp_dir = tempfile::TempDir::new().unwrap();
        let root = temp_dir.path();

        std::fs::create_dir(root.join("real")).unwrap();
        std::os::unix::fs::symlink("real", root.join("link")).unwrap();

        let op = Operator::new(crate::Fs::default().root(root.to_str().unwrap())).unwrap();
        let entries = op.list("/").await.unwrap();
        let modes: std::collections::HashMap<String, EntryMode> = entries
            .iter()
            .map(|e| (e.path().to_string(), e.metadata().mode()))
            .collect();

        assert_eq!(
            modes.get("link/"),
            Some(&EntryMode::DIR),
            "symlink to a directory must be listed as a directory: {modes:?}"
        );
        assert_eq!(modes.get("real/"), Some(&EntryMode::DIR));
    }

    #[tokio::test]
    async fn test_delete_iter_rejects_parent_path_outside_root() {
        use opendal_core::Operator;

        let temp_dir = tempfile::TempDir::new().unwrap();
        let root = temp_dir.path().join("root");
        std::fs::create_dir(&root).unwrap();

        let outside = temp_dir.path().join("outside.txt");
        std::fs::write(&outside, b"content").unwrap();

        let op = Operator::new(crate::Fs::default().root(root.to_str().unwrap())).unwrap();
        let err = op.delete_iter(["../outside.txt"]).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound);
        assert_eq!(std::fs::read(&outside).unwrap(), b"content");
    }
}
