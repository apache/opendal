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

use std::sync::Arc;

use super::core::*;
use opendal_core::raw::*;
use opendal_core::*;

pub struct CompfsDeleter {
    core: Arc<CompfsCore>,
}

impl CompfsDeleter {
    pub fn new(core: Arc<CompfsCore>) -> Self {
        Self { core }
    }
}

impl oio::OneShotDelete for CompfsDeleter {
    async fn delete_once(&self, path: String, _: OpDelete) -> Result<()> {
        let is_dir_path = path.ends_with('/');
        let path = self.core.prepare_path(&path)?;

        let meta = match self
            .core
            .exec({
                let path = path.clone();
                move || async move { compio::fs::symlink_metadata(path).await }
            })
            .await
        {
            Ok(meta) => meta,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err),
        };
        let is_dir = meta.is_dir();

        // prepare_path calls confined_join, which strips the trailing slash.
        // Check the directory path against the target, then remove the entry
        // identified by symlink_metadata.
        if is_dir_path && !is_dir {
            let target_is_dir = if meta.is_symlink() {
                match self
                    .core
                    .exec({
                        let path = path.clone();
                        move || async move { compio::fs::metadata(path).await }
                    })
                    .await
                {
                    Ok(target) => target.is_dir(),
                    Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
                    Err(err) => return Err(err),
                }
            } else {
                false
            };

            if !target_is_dir {
                return Err(Error::new(
                    ErrorKind::NotADirectory,
                    "delete path is a directory path, but the target is a file",
                ));
            }
        }

        // compio_fs::FileType::is_symlink_dir() is always false on Windows
        // because From<std::fs::FileType> copies std's mutually exclusive
        // is_dir() and is_symlink() flags. Check FILE_ATTRIBUTE_DIRECTORY on
        // the raw dwFileAttributes instead so directory symlinks and junctions
        // are removed with remove_dir.
        #[cfg(windows)]
        let remove_as_dir = {
            const FILE_ATTRIBUTE_DIRECTORY: u32 = 0x10;
            (meta.file_attributes() & FILE_ATTRIBUTE_DIRECTORY) != 0
        };
        #[cfg(not(windows))]
        let remove_as_dir = is_dir;

        let result = if remove_as_dir {
            self.core
                .exec(move || async move { compio::fs::remove_dir(path).await })
                .await
        } else {
            self.core
                .exec(move || async move { compio::fs::remove_file(path).await })
                .await
        };
        match result {
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
            result => result,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_delete_missing_entry_is_a_no_op() {
        let temp_dir = tempfile::tempdir().unwrap();
        let op = Operator::new(crate::Compfs::default().root(temp_dir.path().to_str().unwrap()))
            .unwrap();

        op.delete("missing").await.unwrap();
        op.delete("missing/").await.unwrap();
    }

    #[tokio::test]
    async fn test_delete_file_and_dir_paths() {
        let temp_dir = tempfile::tempdir().unwrap();
        let op = Operator::new(crate::Compfs::default().root(temp_dir.path().to_str().unwrap()))
            .unwrap();

        op.write("file.txt", "hello").await.unwrap();
        let err = op.delete("file.txt/").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotADirectory);
        // The file must survive: a directory path must never remove a file.
        assert!(op.stat("file.txt").await.is_ok());

        op.delete("file.txt").await.unwrap();
        assert!(op.stat("file.txt").await.is_err());

        op.create_dir("empty_dir/").await.unwrap();
        op.delete("empty_dir/").await.unwrap();
        assert!(op.stat("empty_dir/").await.is_err());

        op.create_dir("dir_no_slash/").await.unwrap();
        op.delete("dir_no_slash").await.unwrap();
        assert!(op.stat("dir_no_slash/").await.is_err());

        op.write("nonempty_dir/child.txt", "data").await.unwrap();
        let err = op.delete("nonempty_dir/").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        let err = op.delete("nonempty_dir").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(op.stat("nonempty_dir/child.txt").await.is_ok());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_delete_symlinks() {
        let temp_dir = tempfile::tempdir().unwrap();
        let root = temp_dir.path();
        let op = Operator::new(crate::Compfs::default().root(root.to_str().unwrap())).unwrap();

        // Symlink to file: directory path is rejected, file path unlinks the symlink only.
        std::fs::write(root.join("real.txt"), b"data").unwrap();
        std::os::unix::fs::symlink("real.txt", root.join("link.txt")).unwrap();
        let err = op.delete("link.txt/").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotADirectory);
        assert!(
            std::fs::symlink_metadata(root.join("link.txt"))
                .unwrap()
                .is_symlink()
        );
        assert_eq!(std::fs::read(root.join("real.txt")).unwrap(), b"data");

        op.delete("link.txt").await.unwrap();
        assert_eq!(
            std::fs::symlink_metadata(root.join("link.txt"))
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::NotFound
        );
        assert_eq!(std::fs::read(root.join("real.txt")).unwrap(), b"data");

        // Symlink to dir: both file and directory paths unlink the symlink and keep the target.
        std::fs::create_dir(root.join("realdir")).unwrap();
        std::fs::write(root.join("realdir/keep"), b"data").unwrap();
        for path in ["linkdir", "linkdir/"] {
            std::os::unix::fs::symlink("realdir", root.join("linkdir")).unwrap();
            op.delete(path).await.unwrap();
            assert_eq!(
                std::fs::symlink_metadata(root.join("linkdir"))
                    .unwrap_err()
                    .kind(),
                std::io::ErrorKind::NotFound
            );
            assert_eq!(std::fs::read(root.join("realdir/keep")).unwrap(), b"data");
        }

        // Dangling symlink: directory path is a no-op leaving the link, file path removes it.
        std::os::unix::fs::symlink("nope.txt", root.join("dangle.txt")).unwrap();
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

    #[cfg(windows)]
    #[tokio::test]
    async fn test_delete_symlinks() {
        const ERROR_PRIVILEGE_NOT_HELD: i32 = 1314;

        let temp_dir = tempfile::tempdir().unwrap();
        let root = temp_dir.path();
        let op = Operator::new(crate::Compfs::default().root(root.to_str().unwrap())).unwrap();

        std::fs::write(root.join("real.txt"), b"data").unwrap();
        if let Err(err) =
            std::os::windows::fs::symlink_file(root.join("real.txt"), root.join("link.txt"))
        {
            if err.kind() == std::io::ErrorKind::PermissionDenied
                || err.raw_os_error() == Some(ERROR_PRIVILEGE_NOT_HELD)
            {
                return;
            }
            panic!("failed to create file symlink: {err}");
        }

        let err = op.delete("link.txt/").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotADirectory);
        assert!(
            std::fs::symlink_metadata(root.join("link.txt"))
                .unwrap()
                .is_symlink()
        );
        assert_eq!(std::fs::read(root.join("real.txt")).unwrap(), b"data");

        op.delete("link.txt").await.unwrap();
        assert_eq!(
            std::fs::symlink_metadata(root.join("link.txt"))
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::NotFound
        );
        assert_eq!(std::fs::read(root.join("real.txt")).unwrap(), b"data");

        // Directory symlink: both file and directory paths must use remove_dir
        // to unlink the symlink itself and leave the target intact.
        std::fs::create_dir(root.join("realdir")).unwrap();
        std::fs::write(root.join("realdir/keep"), b"data").unwrap();
        for path in ["linkdir", "linkdir/"] {
            std::os::windows::fs::symlink_dir(root.join("realdir"), root.join("linkdir")).unwrap();
            op.delete(path).await.unwrap();
            assert_eq!(
                std::fs::symlink_metadata(root.join("linkdir"))
                    .unwrap_err()
                    .kind(),
                std::io::ErrorKind::NotFound
            );
            assert_eq!(std::fs::read(root.join("realdir/keep")).unwrap(), b"data");
        }

        // Dangling file and directory symlinks: directory path is a no-op
        // leaving the link; file path removes the link.
        std::os::windows::fs::symlink_file(root.join("nope.txt"), root.join("dangle.txt")).unwrap();
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

        std::os::windows::fs::symlink_dir(root.join("nopedir"), root.join("dangledir")).unwrap();
        op.delete("dangledir/").await.unwrap();
        assert!(
            std::fs::symlink_metadata(root.join("dangledir"))
                .unwrap()
                .is_symlink()
        );
        op.delete("dangledir").await.unwrap();
        assert_eq!(
            std::fs::symlink_metadata(root.join("dangledir"))
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::NotFound
        );
    }
}
