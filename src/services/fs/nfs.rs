// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Async filesystem primitives.
//! We use [`nuclei`] to support io-uring.
//!
//! [`nuclei`]: https://docs.rs/nuclei

#![warn(missing_docs)]
use std::io;
use std::path::Path;

use nuclei::spawn_blocking;

#[doc(no_inline)]
pub use std::fs::{FileType, Metadata, Permissions};

/// Creates a directory and its parent directories if they are missing.
///
/// # Errors
///
/// An error will be returned in the following situations:
///
/// * `path` already points to an existing file or directory.
/// * The current process lacks permissions to create the directory or its missing parents.
/// * Some other I/O error occurred.
pub async fn create_dir_all<P: AsRef<Path>>(path: P) -> io::Result<()> {
    let path = path.as_ref().to_owned();
    spawn_blocking(move || std::fs::create_dir_all(&path)).await
}

/// Reads metadata for a path.
///
/// This function will traverse symbolic links to read metadata for the target file or directory.
/// If you want to read metadata without following symbolic links, use [`symlink_metadata()`]
/// instead.
///
/// # Errors
///
/// An error will be returned in the following situations:
///
/// * `path` does not point to an existing file or directory.
/// * The current process lacks permissions to read metadata for the path.
/// * Some other I/O error occurred.
pub async fn metadata<P: AsRef<Path>>(path: P) -> io::Result<Metadata> {
    let path = path.as_ref().to_owned();
    spawn_blocking(move || std::fs::metadata(path)).await
}

/// Removes an empty directory.
///
/// Note that this function can only delete an empty directory. If you want to delete a directory
/// and all of its contents, use [`remove_dir_all()`] instead.
///
/// # Errors
///
/// An error will be returned in the following situations:
///
/// * `path` is not an existing and empty directory.
/// * The current process lacks permissions to remove the directory.
/// * Some other I/O error occurred.
pub async fn remove_dir<P: AsRef<Path>>(path: P) -> io::Result<()> {
    let path = path.as_ref().to_owned();
    spawn_blocking(move || std::fs::remove_dir(&path)).await
}

/// Removes a file.
///
/// # Errors
///
/// An error will be returned in the following situations:
///
/// * `path` does not point to an existing file.
/// * The current process lacks permissions to remove the file.
/// * Some other I/O error occurred.
pub async fn remove_file<P: AsRef<Path>>(path: P) -> io::Result<()> {
    let path = path.as_ref().to_owned();
    spawn_blocking(move || std::fs::remove_file(&path)).await
}
