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
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::{BufMut, Bytes};
use futures::io::Cursor;
use futures::AsyncWrite;
use minitrace::trace;

use crate::error::other;
use crate::error::ObjectError;
use crate::object::ObjectStreamer;
use crate::ops::OpList;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::ops::{OpCreate, OpDelete};
use crate::Accessor;
use crate::BytesReader;
use crate::BytesWriter;
use crate::Metadata;
use crate::Object;
use crate::ObjectMode;

#[derive(Default)]
pub struct Builder {}

impl Builder {
    pub async fn finish(&mut self) -> Result<Arc<dyn Accessor>> {
        Ok(Arc::new(Backend::default()))
    }
}

#[derive(Debug, Clone, Default)]
pub struct Backend {
    inner: Arc<Mutex<HashMap<String, bytes::Bytes>>>,
}

impl Backend {
    pub fn build() -> Builder {
        Builder::default()
    }
}

#[async_trait]
impl Accessor for Backend {
    #[trace("create")]
    async fn create(&self, args: &OpCreate) -> Result<Metadata> {
        let path = &args.path;

        match args.mode {
            ObjectMode::FILE => {
                if path.ends_with('/') {
                    return Err(other(ObjectError::new(
                        "create",
                        path,
                        anyhow!("is a directory"),
                    )));
                }

                let mut map = self.inner.lock().expect("lock poisoned");
                map.insert(path.clone(), Bytes::new());

                let mut meta = Metadata::default();
                meta.set_path(path);
                meta.set_mode(ObjectMode::FILE);
                meta.set_content_length(0);
                meta.set_complete();
                Ok(meta)
            }
            ObjectMode::DIR => {
                let mut path = path.clone();
                if !path.ends_with('/') {
                    path.push('/');
                }

                let mut map = self.inner.lock().expect("lock poisoned");
                map.insert(path.clone(), Bytes::new());

                let mut meta = Metadata::default();
                meta.set_path(&path);
                meta.set_mode(ObjectMode::DIR);
                meta.set_content_length(0);
                meta.set_complete();
                Ok(meta)
            }
            ObjectMode::Unknown => Err(other(ObjectError::new(
                "create",
                path,
                anyhow!("create unknown object mode is not supported"),
            ))),
        }
    }

    #[trace("read")]
    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        let path = &args.path;
        if path.ends_with('/') {
            return Err(other(ObjectError::new(
                "read",
                path,
                anyhow!("is a directory"),
            )));
        }

        let map = self.inner.lock().expect("lock poisoned");

        let data = map.get(path).ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                ObjectError::new("read", path, anyhow!("key not exists in map")),
            )
        })?;

        let mut data = data.clone();
        if let Some(offset) = args.offset {
            if offset >= data.len() as u64 {
                return Err(other(ObjectError::new(
                    "read",
                    path,
                    anyhow!("offset out of bound {} >= {}", offset, data.len()),
                )));
            }
            data = data.slice(offset as usize..data.len());
        };

        if let Some(size) = args.size {
            if size > data.len() as u64 {
                return Err(other(ObjectError::new(
                    "read",
                    path,
                    anyhow!("size out of bound {} > {}", size, data.len()),
                )));
            }
            data = data.slice(0..size as usize);
        };

        Ok(Box::new(Cursor::new(data)))
    }

    #[trace("write")]
    async fn write(&self, args: &OpWrite) -> Result<BytesWriter> {
        let path = &args.path;
        if path.ends_with('/') {
            return Err(other(ObjectError::new(
                "write",
                path,
                anyhow!("is a directory"),
            )));
        }

        Ok(Box::new(MapWriter {
            path: path.clone(),
            size: args.size,
            map: self.inner.clone(),
            buf: Default::default(),
        }))
    }

    #[trace("stat")]
    async fn stat(&self, args: &OpStat) -> Result<Metadata> {
        let path = &args.path;

        if path.ends_with('/') {
            let mut meta = Metadata::default();
            meta.set_path(path)
                .set_mode(ObjectMode::DIR)
                .set_content_length(0)
                .set_complete();

            return Ok(meta);
        }

        let map = self.inner.lock().expect("lock poisoned");

        let data = map.get(path).ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                ObjectError::new("read", path, anyhow!("key not exists in map")),
            )
        })?;

        let mut meta = Metadata::default();
        meta.set_path(path)
            .set_mode(ObjectMode::FILE)
            .set_content_length(data.len() as u64)
            .set_complete();

        Ok(meta)
    }

    #[trace("delete")]
    async fn delete(&self, args: &OpDelete) -> Result<()> {
        let path = &args.path;

        let mut map = self.inner.lock().expect("lock poisoned");
        map.remove(path);

        Ok(())
    }

    #[trace("list")]
    async fn list(&self, args: &OpList) -> Result<ObjectStreamer> {
        let mut path = args.path.clone();
        if !path.ends_with('/') {
            path.push('/')
        }
        if path == "/" {
            path.clear();
        }

        let map = self.inner.lock().expect("lock poisoned");

        let paths = map
            .iter()
            .map(|(k, _)| k.clone())
            .filter(|k| k.starts_with(&path))
            .collect::<Vec<String>>();

        Ok(Box::new(EntryStream {
            backend: self.clone(),
            paths,
            idx: 0,
        }))
    }
}

struct MapWriter {
    path: String,
    size: u64,
    map: Arc<Mutex<HashMap<String, bytes::Bytes>>>,

    buf: bytes::BytesMut,
}

impl AsyncWrite for MapWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        let size = buf.len();
        self.buf.put_slice(buf);
        Poll::Ready(Ok(size))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        if self.buf.len() != self.size as usize {
            return Poll::Ready(Err(other(ObjectError::new(
                "write",
                &self.path,
                anyhow!(
                    "write short, expect {} actual {}",
                    self.size,
                    self.buf.len()
                ),
            ))));
        }

        let buf = mem::take(&mut self.buf);
        let mut map = self.map.lock().expect("lock poisoned");
        map.insert(self.path.clone(), buf.freeze());

        Poll::Ready(Ok(()))
    }
}

struct EntryStream {
    backend: Backend,
    paths: Vec<String>,
    idx: usize,
}

impl futures::Stream for EntryStream {
    type Item = Result<Object>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.idx >= self.paths.len() {
            return Poll::Ready(None);
        }

        let idx = self.idx;
        self.idx += 1;

        let path = self.paths.get(idx).expect("path must valid");

        let backend = self.backend.clone();
        let map = backend.inner.lock().expect("lock poisoned");

        let data = map.get(path);
        // If the path is not get, we can skip it safely.
        if data.is_none() {
            return self.poll_next(cx);
        }
        let bs = data.expect("object must exist");

        let mut o = Object::new(Arc::new(self.backend.clone()), path);
        let meta = o.metadata_mut();
        meta.set_path(path)
            .set_mode(ObjectMode::FILE)
            .set_content_length(bs.len() as u64)
            .set_complete();

        Poll::Ready(Some(Ok(o)))
    }
}
