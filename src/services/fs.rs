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

use std::collections::HashMap;
use std::fs;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use anyhow::anyhow;
use async_trait::async_trait;
use blocking::unblock;
use blocking::Unblock;
use futures::io;
use futures::ready;
use futures::AsyncReadExt;
use futures::AsyncSeekExt;
use futures::AsyncWriteExt;
use log::debug;
use log::error;
use log::info;

use crate::error::Error;
use crate::error::Kind;
use crate::error::Result;
use crate::object::BoxedObjectStream;
use crate::object::Metadata;
use crate::object::ObjectMode;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::Accessor;
use crate::BoxedAsyncReader;
use crate::Object;

#[derive(Default, Debug)]
pub struct Builder {
    root: Option<String>,
}

impl Builder {
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.root = Some(root.to_string());

        self
    }

    pub async fn finish(&mut self) -> Result<Arc<dyn Accessor>> {
        info!("backend build started: {:?}", &self);

        // Make `/` as the default of root.
        let root = match &self.root {
            None => "/".to_string(),
            Some(v) => {
                if !v.starts_with('/') {
                    return Err(Error::Backend {
                        kind: Kind::BackendConfigurationInvalid,
                        context: HashMap::from([("root".to_string(), v.clone())]),
                        source: anyhow!("Root must start with /"),
                    });
                }
                v.to_string()
            }
        };

        // If root dir is not exist, we must create it.
        let metadata_root = root.clone();
        if let Err(e) = unblock(|| fs::metadata(metadata_root)).await {
            if e.kind() == std::io::ErrorKind::NotFound {
                let dir_root = root.clone();
                unblock(|| fs::create_dir_all(dir_root))
                    .await
                    .map_err(|e| parse_io_error(e, "build", &root))?;
            }
        }

        info!("backend build finished: {:?}", &self);
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

    pub(crate) fn get_abs_path(&self, path: &str) -> String {
        // Joining an absolute path replaces the existing path, we need to
        // normalize it before.
        let path = path
            .split('/')
            .filter(|v| !v.is_empty())
            .collect::<Vec<&str>>()
            .join("/");

        PathBuf::from(&self.root)
            .join(path)
            .to_string_lossy()
            .to_string()
    }
}

#[async_trait]
impl Accessor for Backend {
    async fn read(&self, args: &OpRead) -> Result<BoxedAsyncReader> {
        let path = self.get_abs_path(&args.path);
        info!(
            "object {} read start: offset {:?}, size {:?}",
            &path, args.offset, args.size
        );

        let open_path = path.clone();
        let f = unblock(|| fs::OpenOptions::new().read(true).open(open_path))
            .await
            .map_err(|e| {
                let e = parse_io_error(e, "read", &path);
                error!("object {} open: {:?}", &path, e);
                e
            })?;

        let mut f = Unblock::new(f);

        if let Some(offset) = args.offset {
            f.seek(SeekFrom::Start(offset)).await.map_err(|e| {
                let e = parse_io_error(e, "read", &path);
                error!("object {} seek: {:?}", &path, e);
                e
            })?;
        };

        let r: BoxedAsyncReader = match args.size {
            Some(size) => Box::new(f.take(size)),
            None => Box::new(f),
        };

        info!(
            "object {} reader created: offset {:?}, size {:?}",
            &path, args.offset, args.size
        );
        Ok(r)
    }

    async fn write(&self, mut r: BoxedAsyncReader, args: &OpWrite) -> Result<usize> {
        let path = self.get_abs_path(&args.path);
        info!("object {} write start: size {}", &path, args.size);

        // Create dir before write path.
        //
        // TODO(xuanwo): There are many works to do here:
        //   - Is it safe to create dir concurrently?
        //   - Do we need to extract this logic as new util functions?
        //   - Is it better to check the parent dir exists before call mkdir?
        let parent = PathBuf::from(&path)
            .parent()
            .ok_or_else(|| anyhow!("malformed path: {:?}", &path))?
            .to_path_buf();

        let capture_parent = parent.clone();
        unblock(|| fs::create_dir_all(capture_parent))
            .await
            .map_err(|e| {
                let e = parse_io_error(e, "write", &parent.to_string_lossy());
                error!(
                    "object {} create_dir_all for parent {}: {:?}",
                    &path,
                    &parent.to_string_lossy(),
                    e
                );
                e
            })?;

        let capture_path = path.clone();
        let f = unblock(|| {
            fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(capture_path)
        })
        .await
        .map_err(|e| {
            let e = parse_io_error(e, "write", &path);
            error!("object {} open: {:?}", &path, e);
            e
        })?;

        let mut f = Unblock::new(f);

        // TODO: we should respect the input size.
        let s = io::copy(&mut r, &mut f).await.map_err(|e| {
            let e = parse_io_error(e, "write", &path);
            error!("object {} copy: {:?}", &path, e);
            e
        })?;

        // `std::fs::File`'s errors detected on closing are ignored by
        // the implementation of Drop.
        // So we need to call `flush` to make sure all data have been flushed
        // to fs successfully.
        f.flush().await.map_err(|e| {
            let e = parse_io_error(e, "write", &path);
            error!("object {} flush: {:?}", &path, e);
            e
        })?;

        info!("object {} write finished: size {:?}", &path, args.size);
        Ok(s as usize)
    }

    async fn stat(&self, args: &OpStat) -> Result<Metadata> {
        let path = self.get_abs_path(&args.path);
        info!("object {} stat start", &path);

        let capture_path = path.clone();
        let meta = unblock(|| fs::metadata(capture_path)).await.map_err(|e| {
            let e = parse_io_error(e, "stat", &path);
            error!("object {} stat: {:?}", &path, e);
            e
        })?;

        let mut m = Metadata::default();
        m.set_path(&args.path);
        if meta.is_dir() {
            m.set_mode(ObjectMode::DIR);
        } else {
            // TODO: we should handle LINK or other types here.
            m.set_mode(ObjectMode::FILE);
        }
        m.set_content_length(meta.len() as u64);
        m.set_complete();

        info!("object {} stat finished", &path);
        Ok(m)
    }

    async fn delete(&self, args: &OpDelete) -> Result<()> {
        let path = self.get_abs_path(&args.path);
        info!("object {} delete start", &path);

        let capture_path = path.clone();
        // PathBuf.is_dir() is not free, call metadata directly instead.
        let meta = unblock(|| fs::metadata(capture_path)).await;

        if let Err(err) = meta {
            return if err.kind() == std::io::ErrorKind::NotFound {
                Ok(())
            } else {
                let e = parse_io_error(err, "delete", &path);
                error!("object {} delete: {:?}", &path, e);
                Err(e)
            };
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

        f.map_err(|e| parse_io_error(e, "delete", &path))?;

        info!("object {} delete finished", &path);
        Ok(())
    }

    async fn list(&self, args: &OpList) -> Result<BoxedObjectStream> {
        let path = self.get_abs_path(&args.path);
        info!("object {} list start", &path);

        let open_path = path.clone();
        let f = fs::read_dir(open_path).map_err(|e| {
            let e = parse_io_error(e, "read", &path);
            error!("object {} list: {:?}", &path, e);
            e
        })?;

        let rd = Readdir {
            acc: Arc::new(self.clone()),
            root: self.root.clone(),
            path: args.path.clone(),
            rd: Unblock::new(f),
        };

        Ok(Box::new(rd))
    }
}

struct Readdir {
    acc: Arc<dyn Accessor>,
    root: String,
    path: String,

    rd: Unblock<std::fs::ReadDir>,
}

impl futures::Stream for Readdir {
    type Item = Result<Object>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(Pin::new(&mut self.rd).poll_next(cx)) {
            None => {
                debug!("object {} list done", &self.path);
                Poll::Ready(None)
            }
            Some(Err(e)) => {
                error!("object {} stream poll_next: {:?}", &self.path, e);
                Poll::Ready(Some(Err(parse_io_error(e, "list", &self.path))))
            }
            Some(Ok(de)) => {
                // NOTE: metadata is syscall.
                let de_meta = de.metadata().map_err(|e| {
                    let e = parse_io_error(e, "list", &de.path().to_string_lossy());
                    error!("object {:?} metadata: {:?}", &de.path(), e);
                    e
                });
                if let Err(e) = de_meta {
                    return Poll::Ready(Some(Err(e)));
                }
                let de_meta = de_meta.unwrap();

                let de_path = de.path();
                let de_path = de_path.strip_prefix(&self.root).map_err(|e| {
                    let e = Error::Object {
                        kind: Kind::Unexpected,
                        op: "list",
                        path: de.path().to_string_lossy().to_string(),
                        source: anyhow::Error::from(e),
                    };
                    error!("object {:?} path strip_prefix: {:?}", &de.path(), e);
                    e
                })?;
                let path = de_path.to_string_lossy();

                let mut o = Object::new(self.acc.clone(), &path);

                let meta = o.metadata_mut();
                meta.set_complete();
                if de_meta.is_dir() {
                    meta.set_mode(ObjectMode::DIR);
                } else {
                    meta.set_mode(ObjectMode::FILE);
                }
                meta.set_content_length(de_meta.len());
                meta.set_complete();

                debug!(
                    "object {} got entry, path: {}, mode: {}",
                    &self.path,
                    meta.path(),
                    meta.mode()
                );
                Poll::Ready(Some(Ok(o)))
            }
        }
    }
}

/// Parse all path related errors.
///
/// ## Notes
///
/// Skip utf-8 check to allow invalid path input.
fn parse_io_error(err: std::io::Error, op: &'static str, path: &str) -> Error {
    use std::io::ErrorKind;

    match err.kind() {
        ErrorKind::NotFound => Error::Object {
            kind: Kind::ObjectNotExist,
            op,
            path: path.to_string(),
            source: anyhow::Error::from(err),
        },
        ErrorKind::PermissionDenied => Error::Object {
            kind: Kind::ObjectPermissionDenied,
            op,
            path: path.to_string(),
            source: anyhow::Error::from(err),
        },
        _ => Error::Object {
            kind: Kind::Unexpected,
            op,
            path: path.to_string(),
            source: anyhow::Error::from(err),
        },
    }
}
