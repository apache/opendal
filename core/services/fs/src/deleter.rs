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

#[cfg(windows)]
use std::os::windows::fs::FileTypeExt;
use std::sync::Arc;

use super::core::*;
use opendal_core::raw::*;
use opendal_core::*;

pub struct FsDeleter {
    core: Arc<FsCore>,
}

impl FsDeleter {
    pub fn new(core: Arc<FsCore>) -> Self {
        Self { core }
    }
}

impl oio::OneShotDelete for FsDeleter {
    async fn delete_once(&self, path: String, args: OpDelete) -> Result<()> {
        let p = self.core.root_join(&path)?;

        let file_type = match tokio::fs::symlink_metadata(&p).await {
            Ok(meta) => meta.file_type(),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(new_std_io_error(err)),
        };
        let is_dir = file_type.is_dir();

        // confined_join strips the trailing slash. Check the directory path
        // against the target, then remove the entry identified by symlink_metadata.
        if path.ends_with('/') && !is_dir {
            let target_is_dir = if file_type.is_symlink() {
                match tokio::fs::metadata(&p).await {
                    Ok(target) => target.is_dir(),
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                    Err(err) => return Err(new_std_io_error(err)),
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

        #[cfg(windows)]
        let remove_as_dir = is_dir || file_type.is_symlink_dir();
        #[cfg(not(windows))]
        let remove_as_dir = is_dir;

        let result = if is_dir && args.recursive() {
            tokio::fs::remove_dir_all(&p).await
        } else if remove_as_dir {
            tokio::fs::remove_dir(&p).await
        } else {
            tokio::fs::remove_file(&p).await
        };
        match result {
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            result => result.map_err(new_std_io_error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_delete_missing_entry_is_a_no_op() {
        let temp_dir = tempfile::tempdir().unwrap();
        let op =
            Operator::new(crate::Fs::default().root(temp_dir.path().to_str().unwrap())).unwrap();

        op.delete("missing").await.unwrap();
        op.delete("missing/").await.unwrap();
    }

    #[tokio::test]
    async fn test_delete_file_through_directory_path_is_rejected() {
        use opendal_core::Operator;

        let temp_dir = tempfile::TempDir::new().unwrap();
        let root = temp_dir.path();

        let op = Operator::new(crate::Fs::default().root(root.to_str().unwrap())).unwrap();

        op.write("f.txt", "x").await.unwrap();
        let err = op.delete("f.txt/").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotADirectory);
        // The file must survive: a directory path must never remove a file.
        assert!(op.stat("f.txt").await.is_ok());

        // Directory deletes keep working.
        op.create_dir("d/").await.unwrap();
        op.delete("d/").await.unwrap();
        assert!(op.stat("d/").await.is_err());
    }
}
