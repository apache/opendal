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
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::{BufMut, Bytes};
use futures::stream;
use futures::{io, Sink};
use minitrace::trace;

use crate::error::Error;
use crate::error::Kind;
use crate::error::Result;
use crate::io::{BytesSink, BytesStream};
use crate::object::BoxedObjectStream;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::Accessor;
use crate::BoxedAsyncReader;
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

    // normalize_path removes all internal `//` inside path.
    pub(crate) fn normalize_path(path: &str) -> String {
        let has_trailing = path.ends_with('/');

        let mut p = path
            .split('/')
            .filter(|v| !v.is_empty())
            .collect::<Vec<&str>>()
            .join("/");

        if has_trailing && !p.eq("/") {
            p.push('/')
        }

        p
    }
}

#[async_trait]
impl Accessor for Backend {
    #[trace("read")]
    async fn read(&self, args: &OpRead) -> Result<BytesStream> {
        let path = Backend::normalize_path(&args.path);

        let map = self.inner.lock().expect("lock poisoned");

        let data = map.get(&path).ok_or_else(|| Error::Object {
            kind: Kind::ObjectNotExist,
            op: "read",
            path: path.to_string(),
            source: anyhow!("key not exists in map"),
        })?;

        let mut data = data.clone();
        if let Some(offset) = args.offset {
            if offset >= data.len() as u64 {
                return Err(Error::Object {
                    kind: Kind::Unexpected,
                    op: "read",
                    path: path.to_string(),
                    source: anyhow!("offset out of bound {} >= {}", offset, data.len()),
                });
            }
            data = data.slice(offset as usize..data.len());
        };

        if let Some(size) = args.size {
            if size > data.len() as u64 {
                return Err(Error::Object {
                    kind: Kind::Unexpected,
                    op: "read",
                    path: path.to_string(),
                    source: anyhow!("size out of bound {} > {}", size, data.len()),
                });
            }
            data = data.slice(0..size as usize);
        };

        Ok(Box::new(Box::pin(stream::once(async {
            Ok::<_, Error>(data)
        }))))
    }
    #[trace("write")]
    async fn write(&self, mut r: BoxedAsyncReader, args: &OpWrite) -> Result<usize> {
        let path = Backend::normalize_path(&args.path);

        let bs = vec![0; args.size as usize];
        let mut cursor = io::Cursor::new(bs);
        let n = io::copy(&mut r, &mut cursor)
            .await
            .map_err(|e| Error::Object {
                kind: Kind::Unexpected,
                op: "write",
                path: path.clone(),
                source: anyhow::Error::from(e),
            })?;
        if n < args.size {
            return Err(Error::Object {
                kind: Kind::Unexpected,
                op: "write",
                path: path.clone(),
                source: anyhow!("write short  {} M {}", n, args.size),
            });
        }

        let mut map = self.inner.lock().expect("lock poisoned");
        map.insert(path.to_string(), Bytes::from(cursor.into_inner()));

        Ok(n as usize)
    }

    async fn write2(&self, args: &OpWrite) -> Result<BytesSink> {
        let path = Backend::normalize_path(&args.path);

        let x: BoxedAsyncReader;

        Ok(Box::new(MapSink {
            path,
            size: args.size,
            map: self.inner.clone(),
            buf: Default::default(),
        }))
    }

    #[trace("stat")]
    async fn stat(&self, args: &OpStat) -> Result<Metadata> {
        let path = Backend::normalize_path(&args.path);

        if path.ends_with('/') || path.is_empty() {
            let mut meta = Metadata::default();
            meta.set_path(&path)
                .set_mode(ObjectMode::DIR)
                .set_content_length(0)
                .set_complete();

            return Ok(meta);
        }

        let map = self.inner.lock().expect("lock poisoned");

        let data = map.get(&path).ok_or_else(|| Error::Object {
            kind: Kind::ObjectNotExist,
            op: "stat",
            path: path.to_string(),
            source: anyhow!("key not exists in map"),
        })?;

        let mut meta = Metadata::default();
        meta.set_path(&path)
            .set_mode(ObjectMode::FILE)
            .set_content_length(data.len() as u64)
            .set_complete();

        Ok(meta)
    }
    #[trace("delete")]
    async fn delete(&self, args: &OpDelete) -> Result<()> {
        let path = Backend::normalize_path(&args.path);

        let mut map = self.inner.lock().expect("lock poisoned");
        map.remove(&path);

        Ok(())
    }
    #[trace("list")]
    async fn list(&self, args: &OpList) -> Result<BoxedObjectStream> {
        let path = Backend::normalize_path(&args.path);

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

struct MapSink {
    path: String,
    size: u64,
    map: Arc<Mutex<HashMap<String, bytes::Bytes>>>,

    buf: bytes::BytesMut,
}

impl Sink<Bytes> for MapSink {
    type Error = Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, item: Bytes) -> std::result::Result<(), Self::Error> {
        self.buf.put_slice(&item);
        Ok(())
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        if self.buf.len() != self.size as usize {
            return Poll::Ready(Err(Error::Object {
                kind: Kind::Unexpected,
                op: "write",
                path: self.path.clone(),
                source: anyhow!(
                    "write short, expect {} actual {}",
                    self.size,
                    self.buf.len()
                ),
            }));
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
