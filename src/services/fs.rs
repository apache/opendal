// Copyright 2021 Datafuse Labs.
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

use std::fs;
use std::io::SeekFrom;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use blocking::unblock;
use blocking::Unblock;
use futures::io;
use futures::AsyncReadExt;
use futures::AsyncSeekExt;
use futures::AsyncWriteExt;

use crate::error::Error;
use crate::error::Result;
use crate::object::Metadata;
use crate::ops::OpDelete;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::Accessor;
use crate::BoxedAsyncRead;

#[derive(Default)]
pub struct Builder {
    root: Option<String>,
}

impl Builder {
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.root = Some(root.to_string());

        self
    }

    pub async fn finish(&mut self) -> Result<Arc<dyn Accessor>> {
        // Make `/` as the default of root.
        let root = self.root.clone().unwrap_or_else(|| "/".to_string());

        // If root dir is not exist, we must create it.
        let metadata_root = root.clone();
        if let Err(e) = unblock(|| fs::metadata(metadata_root)).await {
            if e.kind() == std::io::ErrorKind::NotFound {
                let dir_root = root.clone();
                unblock(|| fs::create_dir_all(dir_root))
                    .await
                    .map_err(|e| parse_io_error(&e, PathBuf::from(&root).as_path()))?;
            }
        }

        Ok(Arc::new(Backend { root }))
    }
}

/// Backend is used to serve `Accessor` support for posix alike fs.
///
/// # Note
///
/// We will use separate dedicated thread pool (powered by `unblocking`)
/// for better async performance under tokio. All `std::File` will be wrapped
/// by `Unblock` to gain async support. IO will happen at the separate dedicated
/// thread pool, so we will not block the tokio runtime.
#[derive(Debug, Clone)]
pub struct Backend {
    root: String,
}

impl Backend {
    pub fn build() -> Builder {
        Builder::default()
    }
}

#[async_trait]
impl Accessor for Backend {
    async fn read(&self, args: &OpRead) -> Result<BoxedAsyncRead> {
        let path = PathBuf::from(&self.root).join(&args.path);

        let open_path = path.clone();
        let f = unblock(|| fs::OpenOptions::new().read(true).open(open_path))
            .await
            .map_err(|e| parse_io_error(&e, &path))?;

        let mut f = Unblock::new(f);

        if let Some(offset) = args.offset {
            f.seek(SeekFrom::Start(offset))
                .await
                .map_err(|e| parse_io_error(&e, &path))?;
        };

        let r: BoxedAsyncRead = match args.size {
            Some(size) => Box::new(f.take(size)),
            None => Box::new(f),
        };

        Ok(r)
    }

    async fn write(&self, mut r: BoxedAsyncRead, args: &OpWrite) -> Result<usize> {
        let path = PathBuf::from(&self.root).join(&args.path);

        // Create dir before write path.
        //
        // TODO(xuanwo): There are many works to do here:
        //   - Is it safe to create dir concurrently?
        //   - Do we need to extract this logic as new util functions?
        //   - Is it better to check the parent dir exists before call mkdir?
        let parent = path
            .parent()
            .ok_or_else(|| Error::Unexpected(format!("malformed path: {:?}", path.to_str())))?
            .to_path_buf();

        let capture_parent = parent.clone();
        unblock(|| fs::create_dir_all(capture_parent))
            .await
            .map_err(|e| parse_io_error(&e, &parent))?;

        let capture_path = path.clone();
        let f = unblock(|| {
            fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(capture_path)
        })
        .await
        .map_err(|e| parse_io_error(&e, &path))?;

        let mut f = Unblock::new(f);

        // TODO: we should respect the input size.
        let s = io::copy(&mut r, &mut f)
            .await
            .map_err(|e| parse_io_error(&e, &path))?;

        // `std::fs::File`'s errors detected on closing are ignored by
        // the implementation of Drop.
        // So we need to call `sync_all` to make sure all internal metadata
        // have been flushed to fs successfully.
        f.flush().await.map_err(|e| parse_io_error(&e, &path))?;

        Ok(s as usize)
    }

    async fn stat(&self, args: &OpStat) -> Result<Metadata> {
        let path = PathBuf::from(&self.root).join(&args.path);

        let capture_path = path.clone();
        let meta = unblock(|| fs::metadata(capture_path))
            .await
            .map_err(|e| parse_io_error(&e, &path))?;

        let mut m = Metadata::default();
        m.set_content_length(meta.len() as u64);

        Ok(m)
    }

    async fn delete(&self, args: &OpDelete) -> Result<()> {
        let path = PathBuf::from(&self.root).join(&args.path);

        let capture_path = path.clone();
        // PathBuf.is_dir() is not free, call metadata directly instead.
        let meta = unblock(|| fs::metadata(capture_path)).await;

        if let Err(err) = &meta {
            if err.kind() == std::io::ErrorKind::NotFound {
                return Ok(());
            }
        }

        // Safety: Err branch has been checked, it's OK to unwrap.
        let meta = meta.ok().unwrap();

        let f = if meta.is_dir() {
            let capture_path = path.clone();
            unblock(|| fs::remove_dir(capture_path)).await
        } else {
            let capture_path = path.clone();
            unblock(|| fs::remove_file(capture_path)).await
        };

        f.map_err(|e| parse_io_error(&e, &path))
    }
}

/// Parse all path related errors.
///
/// ## Notes
///
/// Skip utf-8 check to allow invalid path input.
fn parse_io_error(err: &std::io::Error, path: &Path) -> Error {
    use std::io::ErrorKind;

    match err.kind() {
        ErrorKind::NotFound => Error::ObjectNotExist(path.to_string_lossy().into_owned()),
        ErrorKind::PermissionDenied => Error::PermissionDenied(path.to_string_lossy().into_owned()),
        _ => Error::Unexpected(err.to_string()),
    }
}
