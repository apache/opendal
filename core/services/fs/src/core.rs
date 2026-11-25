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

        if let Err(err) = tokio::fs::create_dir_all(&parent).await {
            return Err(map_io_error(refine_path_error(err, &parent).await));
        }

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
        // Fetch source metadata first so missing or directory sources fail
        // before creating destination parent directories.
        let from_meta = match tokio::fs::metadata(&from).await {
            Ok(meta) => meta,
            Err(err) => return Err(new_std_io_error(refine_path_error(err, &from).await)),
        };
        if from_meta.is_dir() {
            return Err(Error::new(
                ErrorKind::IsADirectory,
                "copy source is a directory",
            ));
        }

        let to_trimmed = to.trim_end_matches('/');
        let to_path = self.root_join(to_trimmed)?;
        match tokio::fs::metadata(&to_path).await {
            Ok(to_meta) => {
                if to_meta.is_dir() {
                    return Err(Error::new(
                        ErrorKind::IsADirectory,
                        "copy target is a directory",
                    ));
                }
                if is_same_file(&from, &from_meta, &to_path, &to_meta).await {
                    return Err(Error::new(
                        ErrorKind::IsSameFile,
                        "copy source and destination refer to the same file",
                    ));
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(new_std_io_error(err)),
        }

        // Read source user metadata before copying. A source whose user
        // metadata cannot be read keeps the previous copy behavior of not
        // failing the content copy.
        #[cfg(unix)]
        let user_metadata = Self::get_user_metadata(&from).ok();

        let to = self.ensure_write_abs_path(&self.root, to_trimmed).await?;

        let size = tokio::fs::copy(&from, &to)
            .await
            .map_err(new_std_io_error)?;

        // Copying truncates an existing destination in place, so stale user
        // metadata from the previous file must be replaced explicitly.
        #[cfg(unix)]
        if let Some(user_metadata) = user_metadata {
            Self::replace_user_metadata(&to, Some(&user_metadata))?;
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
        if let Err(err) = tokio::fs::metadata(&from).await {
            return Err(map_path_error(refine_path_error(err, &from).await));
        }

        let to = self
            .ensure_write_abs_path_with(&self.root, to.trim_end_matches('/'), map_path_error)
            .await?;

        #[cfg(target_os = "linux")]
        if if_not_exists {
            return tokio::task::spawn_blocking(move || rename_noreplace(&from, &to))
                .await
                .map_err(new_task_join_error)?;
        }

        if let Err(err) = tokio::fs::rename(from, &to).await {
            return Err(new_std_io_error(refine_path_error(err, &to).await));
        }
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
            Err(e) if is_xattr_unsupported(&e) => return Ok(user_metadata),
            Err(e) => return Err(new_std_io_error(e)),
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
            xattr::set(path, &attr_name, value.as_bytes()).map_err(|err| {
                if is_xattr_unsupported(&err) {
                    Error::new(
                        ErrorKind::Unsupported,
                        "user metadata is not supported by the filesystem",
                    )
                    .set_source(err)
                } else if err.kind() == std::io::ErrorKind::InvalidInput
                    || matches!(err.raw_os_error(), Some(libc::E2BIG | libc::ERANGE))
                {
                    Error::new(ErrorKind::Unexpected, err.kind().to_string()).set_source(err)
                } else {
                    new_std_io_error(err)
                }
            })?;
        }
        Ok(())
    }

    /// Replace the user metadata on `path` with `user_metadata`.
    ///
    /// Only extended attributes in the "user." namespace are removed; other
    /// namespaces are never touched. Passing `None` clears all user metadata.
    /// Filesystems without xattr support are treated as having no user metadata
    /// so that ordinary writes and copies do not fail there.
    #[cfg(unix)]
    pub fn replace_user_metadata(
        path: &Path,
        user_metadata: Option<&HashMap<String, String>>,
    ) -> Result<()> {
        // Write the new values first so a failure while setting metadata keeps
        // the previous user metadata instead of clearing it first.
        if let Some(user_metadata) = user_metadata {
            Self::set_user_metadata(path, user_metadata)?;
        }

        let attrs = match xattr::list(path) {
            Ok(attrs) => attrs,
            Err(e) if is_xattr_unsupported(&e) => return Ok(()),
            Err(e) => return Err(new_std_io_error(e)),
        };

        for attr in attrs {
            let attr_name = attr.to_string_lossy();
            let Some(key) = attr_name.strip_prefix(XATTR_USER_PREFIX) else {
                continue;
            };
            if user_metadata.is_some_and(|metadata| metadata.contains_key(key)) {
                continue;
            }

            match xattr::remove(path, &attr) {
                Ok(()) => {}
                // Another task may have removed the attribute already.
                Err(e) if is_xattr_not_found(&e) => {}
                Err(e) if is_xattr_unsupported(&e) => return Ok(()),
                Err(e) => return Err(new_std_io_error(e)),
            }
        }

        Ok(())
    }
}

/// Prefix for user xattr namespace on Linux.
/// Using "user." as the standard namespace for user-defined attributes.
#[cfg(unix)]
const XATTR_USER_PREFIX: &str = "user.";

/// Whether an xattr error means the filesystem does not support xattrs.
#[cfg(unix)]
fn is_xattr_unsupported(err: &std::io::Error) -> bool {
    if err.kind() == std::io::ErrorKind::Unsupported {
        return true;
    }

    // Some platforms leave ENOTSUP/EOPNOTSUPP/ENOSYS uncategorized.
    #[cfg(target_os = "linux")]
    {
        use rustix::io::Errno;
        matches!(
            err.raw_os_error().map(Errno::from_raw_os_error),
            Some(Errno::OPNOTSUPP | Errno::NOSYS)
        )
    }

    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

/// Whether an xattr error means the attribute is already gone.
///
/// `xattr` reports a missing attribute as `ENODATA` on Linux and `ENOATTR` on
/// macOS and the BSDs. Neither is mapped to [`std::io::ErrorKind::NotFound`].
/// `ENOENT` reports a missing file, so it must keep surfacing as an error.
#[cfg(unix)]
fn is_xattr_not_found(err: &std::io::Error) -> bool {
    let Some(code) = err.raw_os_error() else {
        return false;
    };

    // Platforms that `xattr` does not support report `Unsupported` for every
    // call, so only the supported ones need a missing-attribute errno.
    #[cfg(any(target_os = "linux", target_os = "android", target_os = "hurd"))]
    {
        code == libc::ENODATA
    }

    #[cfg(any(target_os = "macos", target_os = "freebsd", target_os = "netbsd"))]
    {
        code == libc::ENOATTR
    }

    #[cfg(not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "hurd",
        target_os = "macos",
        target_os = "freebsd",
        target_os = "netbsd"
    )))]
    {
        let _ = code;
        false
    }
}

/// Publish a staged write at `to` without replacing an existing entry.
///
/// Returns `ConditionNotMatch` when the destination already exists.
pub(super) fn publish_staged_file(from: &Path, to: &Path) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        rename_noreplace(from, to)
    }

    #[cfg(not(target_os = "linux"))]
    {
        // Non-Linux systems lack a rename that fails on an existing
        // destination. Linking the staged file to `to` creates the destination
        // atomically and fails with `AlreadyExists` when it is already
        // present, on both POSIX and Windows. Hard links require both paths on
        // one filesystem, which the atomic publish already requires for its
        // rename.
        std::fs::hard_link(from, to).map_err(|err| {
            // Windows reports a regular file in place of the destination's
            // parent as ERROR_INVALID_PARAMETER, which maps to a retryable
            // error and keeps the staged file. Report it as ENOTDIR would be.
            #[cfg(windows)]
            if to.parent().is_some_and(|parent| parent.is_file()) {
                return Error::new(ErrorKind::NotADirectory, "parent is not a directory")
                    .set_source(err)
                    .with_context("service_operation", "Rename");
            }
            parse_error(ErrorContext::new(ServiceOperation("Rename")), err)
        })?;

        match std::fs::remove_file(from) {
            // A concurrent cleanup or an unusual filesystem may have removed
            // the staging entry already; the destination is still published.
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            // The destination was published, but the staging file is orphaned
            // now. Report the error so the caller can clean up the staged file.
            Err(err) => {
                Err(new_std_io_error(err).with_context("service_operation", "RemoveStagedFile"))
            }
            Ok(()) => Ok(()),
        }
    }
}

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

/// Classify an already-failed filesystem call on Windows.
///
/// The failed call is never retried. Windows reports a directory target as
/// `ERROR_ACCESS_DENIED`, and a regular file in a parent component as
/// `ERROR_PATH_NOT_FOUND` or, from `create_dir_all`, `ERROR_ALREADY_EXISTS`.
/// Unix reports these as `EISDIR` and `ENOTDIR`. A read-only metadata probe
/// after the failure tells us what the path is, so the error kind can be
/// mapped. The original error is kept as the source.
pub(crate) async fn refine_path_error(err: std::io::Error, path: &Path) -> std::io::Error {
    #[cfg(windows)]
    {
        use std::io::ErrorKind as IoKind;

        let kind = match err.kind() {
            IoKind::PermissionDenied
                if tokio::fs::metadata(path)
                    .await
                    .is_ok_and(|meta| meta.is_dir()) =>
            {
                Some(IoKind::IsADirectory)
            }
            IoKind::NotFound | IoKind::AlreadyExists if has_file_ancestor(path).await => {
                Some(IoKind::NotADirectory)
            }
            _ => None,
        };
        if let Some(kind) = kind {
            return std::io::Error::new(kind, err);
        }
    }

    #[cfg(not(windows))]
    let _ = path;
    err
}

/// Returns whether the nearest existing ancestor of `path` is not a directory.
#[cfg(windows)]
async fn has_file_ancestor(path: &Path) -> bool {
    for ancestor in path.ancestors().skip(1) {
        if let Ok(meta) = tokio::fs::metadata(ancestor).await {
            return !meta.is_dir();
        }
    }
    false
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

/// Parse an error from an unconditional rename.
pub(crate) fn parse_rename_error(err: std::io::Error) -> Error {
    parse_io_error(err, false).with_context("service_operation", "Rename")
}

/// Parse an error using its service operation context.
pub(crate) fn parse_error(ctx: ErrorContext, err: std::io::Error) -> Error {
    parse_io_error(err, true).with_context("service_operation", ctx.service_operation.0)
}

async fn is_same_file(
    from: &Path,
    from_meta: &std::fs::Metadata,
    to: &Path,
    to_meta: &std::fs::Metadata,
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
            (
                tokio::fs::canonicalize(from).await,
                tokio::fs::canonicalize(to).await,
            ),
            (Ok(from_canon), Ok(to_canon)) if from_canon == to_canon
        )
    }
}

fn parse_io_error(err: std::io::Error, already_exists_as_condition: bool) -> Error {
    match err.kind() {
        std::io::ErrorKind::AlreadyExists if already_exists_as_condition => Error::new(
            ErrorKind::ConditionNotMatch,
            "The file already exists in the filesystem",
        )
        .set_source(err),
        _ => {
            #[cfg(unix)]
            if err.raw_os_error() == Some(libc::EXDEV) {
                return Error::new(ErrorKind::Unexpected, err.kind().to_string()).set_source(err);
            }
            new_std_io_error(err)
        }
    }
}

#[cfg(test)]
mod tests {
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

    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "hurd",
        target_os = "macos",
        target_os = "freebsd",
        target_os = "netbsd"
    ))]
    #[test]
    fn test_is_xattr_not_found_distinguishes_missing_attribute_from_missing_file() {
        #[cfg(any(target_os = "linux", target_os = "android", target_os = "hurd"))]
        let missing_attr = std::io::Error::from_raw_os_error(libc::ENODATA);
        #[cfg(any(target_os = "macos", target_os = "freebsd", target_os = "netbsd"))]
        let missing_attr = std::io::Error::from_raw_os_error(libc::ENOATTR);

        assert!(is_xattr_not_found(&missing_attr));
        assert!(!is_xattr_not_found(&std::io::Error::from_raw_os_error(
            libc::ENOENT
        )));
        assert!(!is_xattr_not_found(&std::io::Error::from_raw_os_error(
            libc::EACCES
        )));
    }

    #[cfg(unix)]
    #[test]
    fn test_xattr_remove_missing_attribute_is_not_a_missing_file() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let file = temp_dir.path().join("file");
        std::fs::write(&file, b"data").unwrap();
        if !xattrs_supported(temp_dir.path()) {
            return;
        }

        // The attribute is already gone, as when another task removes it
        // between listing and removing it. The error must be recognized as a
        // missing attribute so the caller can treat the removal as done.
        let err = xattr::remove(&file, "user.missing").unwrap_err();
        assert!(is_xattr_not_found(&err), "{err:?}");

        // A missing file reports ENOENT, which is not a missing attribute and
        // must keep surfacing.
        let missing = temp_dir.path().join("missing");
        let err = xattr::remove(&missing, "user.missing").unwrap_err();
        assert!(!is_xattr_not_found(&err), "{err:?}");

        let err = FsCore::replace_user_metadata(&missing, None).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound, "{err:?}");
    }

    #[test]
    fn test_parse_rename_error_classifies_permanent_and_temporary_errors() {
        let err = parse_rename_error(std::io::Error::from(std::io::ErrorKind::IsADirectory));
        assert_eq!(err.kind(), ErrorKind::IsADirectory);
        assert!(!err.is_temporary());

        let err = parse_rename_error(std::io::Error::from(std::io::ErrorKind::NotADirectory));
        assert_eq!(err.kind(), ErrorKind::NotADirectory);
        assert!(!err.is_temporary());

        let err = parse_rename_error(std::io::Error::from(std::io::ErrorKind::PermissionDenied));
        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
        assert!(!err.is_temporary());

        let err = parse_rename_error(std::io::Error::from(std::io::ErrorKind::Interrupted));
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(err.is_temporary());

        #[cfg(unix)]
        {
            let err = parse_rename_error(std::io::Error::from_raw_os_error(libc::EXDEV));
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(!err.is_temporary());
        }
    }

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
        if !xattrs_supported(root) {
            return;
        }

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

        assert_eq!(FsCore::get_user_metadata(&dst_path).unwrap(), meta);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_fs_backend_copy_replaces_target_user_metadata() {
        use opendal_core::Operator;

        let temp_dir = tempfile::TempDir::new().unwrap();
        let root = temp_dir.path();
        if !xattrs_supported(root) {
            return;
        }

        let src_path = root.join("src");
        let dst_path = root.join("dst");
        std::fs::write(&src_path, b"source").unwrap();
        std::fs::write(&dst_path, b"old destination").unwrap();

        let source_metadata = HashMap::from([
            ("shared".to_string(), "new".to_string()),
            ("source".to_string(), "kept".to_string()),
        ]);
        FsCore::set_user_metadata(&src_path, &source_metadata).unwrap();
        FsCore::set_user_metadata(
            &dst_path,
            &HashMap::from([
                ("shared".to_string(), "old".to_string()),
                ("stale".to_string(), "gone".to_string()),
            ]),
        )
        .unwrap();

        let op = Operator::new(crate::Fs::default().root(root.to_str().unwrap())).unwrap();
        op.copy("src", "dst").await.unwrap();

        assert_eq!(std::fs::read(&dst_path).unwrap(), b"source");
        assert_eq!(
            FsCore::get_user_metadata(&dst_path).unwrap(),
            source_metadata
        );

        // A source without user metadata must still clear the target's stale
        // user metadata instead of inheriting it.
        std::fs::write(&src_path, b"plain source").unwrap();
        FsCore::replace_user_metadata(&src_path, None).unwrap();
        op.copy("src", "dst").await.unwrap();
        assert!(FsCore::get_user_metadata(&dst_path).unwrap().is_empty());
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

    #[tokio::test]
    async fn test_directory_errors_are_permanent() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let root = temp_dir.path();
        std::fs::write(root.join("src"), b"data").unwrap();
        std::fs::write(root.join("file"), b"data").unwrap();
        std::fs::create_dir(root.join("existing_dir")).unwrap();

        let op = Operator::new(crate::Fs::default().root(root.to_str().unwrap())).unwrap();

        let err = op.rename("src", "existing_dir").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::IsADirectory, "{err:?}");
        assert!(!err.is_temporary(), "{err:?}");

        for (from, to) in [("file/child", "dst"), ("src", "file/dir/dst")] {
            let err = op.rename(from, to).await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::NotADirectory, "{err:?}");
            assert!(!err.is_temporary(), "{err:?}");
        }

        let err = op.copy("src", "existing_dir").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::IsADirectory, "{err:?}");
        assert!(!err.is_temporary(), "{err:?}");

        let err = op.copy("src", "file/dir/dst").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotADirectory, "{err:?}");
        assert!(!err.is_temporary(), "{err:?}");

        let err = op.write("existing_dir", "x").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::IsADirectory, "{err:?}");
        assert!(!err.is_temporary(), "{err:?}");

        let err = op
            .write_with("existing_dir", "x")
            .if_not_exists(true)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch, "{err:?}");
    }

    #[tokio::test]
    async fn test_copy_directory_source_rejects_before_creating_parents() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let root = temp_dir.path();
        std::fs::create_dir(root.join("dir")).unwrap();
        std::fs::write(root.join("existing.txt"), b"keep").unwrap();

        let op = Operator::new(crate::Fs::default().root(root.to_str().unwrap())).unwrap();

        let err = op.copy("dir", "existing.txt").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::IsADirectory, "{err:?}");
        assert!(!err.is_temporary(), "{err:?}");
        assert_eq!(std::fs::read(root.join("existing.txt")).unwrap(), b"keep");

        let err = op.copy("dir", "new_parent/missing").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::IsADirectory, "{err:?}");
        assert!(!err.is_temporary(), "{err:?}");
        assert!(!root.join("new_parent").exists());

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink("dir", root.join("dir_link")).unwrap();
            let err = op
                .copy("dir_link", "new_parent_link/missing")
                .await
                .unwrap_err();
            assert_eq!(err.kind(), ErrorKind::IsADirectory, "{err:?}");
            assert!(!err.is_temporary(), "{err:?}");
            assert!(!root.join("new_parent_link").exists());
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_copy_same_file_via_symlink_and_hardlink_preserves_content() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let root = temp_dir.path();
        let has_xattrs = xattrs_supported(root);

        let op = Operator::new(crate::Fs::default().root(root.to_str().unwrap())).unwrap();
        if has_xattrs {
            op.write_with("real.txt", "important")
                .user_metadata([("k".to_string(), "v".to_string())])
                .await
                .unwrap();
        } else {
            op.write("real.txt", "important").await.unwrap();
        }

        std::os::unix::fs::symlink("real.txt", root.join("link.txt")).unwrap();
        std::fs::hard_link(root.join("real.txt"), root.join("hard.txt")).unwrap();

        for (from, to) in [
            ("real.txt", "link.txt"),
            ("link.txt", "real.txt"),
            ("real.txt", "hard.txt"),
            ("hard.txt", "real.txt"),
            ("link.txt", "hard.txt"),
        ] {
            let err = op.copy(from, to).await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::IsSameFile, "{from} -> {to}: {err:?}");
            assert_eq!(
                std::fs::read(root.join("real.txt")).unwrap(),
                b"important",
                "{from} -> {to} must not truncate source"
            );
            if has_xattrs {
                let meta = op.stat("real.txt").await.unwrap();
                assert_eq!(meta.user_metadata().and_then(|m| m.get("k")), Some("v"));
            }
        }

        // Copying from a dangling symlink still fails with NotFound.
        std::os::unix::fs::symlink("missing.txt", root.join("broken_src.txt")).unwrap();
        let err = op.copy("broken_src.txt", "dst.txt").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound, "{err:?}");

        // Copying to a non-existent destination and to a different file/dangling symlink
        // still succeeds normally.
        op.copy("real.txt", "nested/copied.txt").await.unwrap();
        assert_eq!(
            std::fs::read(root.join("nested/copied.txt")).unwrap(),
            b"important"
        );

        op.write("plain.txt", "payload").await.unwrap();
        std::fs::write(root.join("other.txt"), b"old").unwrap();
        std::os::unix::fs::symlink("other.txt", root.join("other_link.txt")).unwrap();
        op.copy("plain.txt", "other_link.txt").await.unwrap();
        assert_eq!(std::fs::read(root.join("other.txt")).unwrap(), b"payload");

        std::os::unix::fs::symlink("created_via_link.txt", root.join("dangle_dst.txt")).unwrap();
        op.copy("plain.txt", "dangle_dst.txt").await.unwrap();
        assert_eq!(
            std::fs::read(root.join("created_via_link.txt")).unwrap(),
            b"payload"
        );
    }
}
