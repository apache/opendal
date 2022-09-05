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
use std::collections::HashSet;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::BufMut;
use bytes::Bytes;
use futures::io::Cursor;
use futures::AsyncWrite;
use parking_lot::Mutex;

use crate::accessor::AccessorCapability;
use crate::error::other;
use crate::error::ObjectError;
use crate::ops::OpCreate;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::ops::Operation;
use crate::Accessor;
use crate::AccessorMetadata;
use crate::BytesReader;
use crate::DirEntry;
use crate::DirStreamer;
use crate::ObjectMetadata;
use crate::ObjectMode;
use crate::Scheme;

/// Builder for memory backend
#[derive(Default)]
pub struct Builder {}

impl Builder {
    /// Consume builder to build a memory backend.
    pub fn build(&mut self) -> Result<Backend> {
        Ok(Backend {
            inner: Arc::new(Mutex::new(HashMap::default())),
        })
    }
}

/// Backend is used to serve `Accessor` support in memory.
#[derive(Debug, Clone)]
pub struct Backend {
    inner: Arc<Mutex<HashMap<String, Bytes>>>,
}

#[async_trait]
impl Accessor for Backend {
    fn metadata(&self) -> AccessorMetadata {
        let mut am = AccessorMetadata::default();
        am.set_scheme(Scheme::Memory)
            .set_root("/")
            .set_name("memory")
            .set_capabilities(
                AccessorCapability::Read | AccessorCapability::Write | AccessorCapability::List,
            );

        am
    }

    async fn create(&self, args: &OpCreate) -> Result<()> {
        let path = args.path();

        match args.mode() {
            ObjectMode::FILE => {
                let mut map = self.inner.lock();
                map.insert(path.to_string(), Bytes::new());

                Ok(())
            }
            ObjectMode::DIR => {
                let mut map = self.inner.lock();
                map.insert(path.to_string(), Bytes::new());

                Ok(())
            }
            _ => unreachable!(),
        }
    }

    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        let path = args.path();

        let map = self.inner.lock();

        let data = map.get(path).ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                ObjectError::new(Operation::Read, path, anyhow!("key not exists in map")),
            )
        })?;

        let mut data = data.clone();
        if let Some(offset) = args.offset() {
            if offset >= data.len() as u64 {
                return Err(other(ObjectError::new(
                    Operation::Read,
                    path,
                    anyhow!("offset out of bound {} >= {}", offset, data.len()),
                )));
            }
            data = data.slice(offset as usize..data.len());
        };

        if let Some(size) = args.size() {
            if size > data.len() as u64 {
                return Err(other(ObjectError::new(
                    Operation::Read,
                    path,
                    anyhow!("size out of bound {} > {}", size, data.len()),
                )));
            }
            data = data.slice(0..size as usize);
        };

        Ok(Box::new(Cursor::new(data)))
    }

    async fn write(&self, args: &OpWrite, r: BytesReader) -> Result<u64> {
        let path = args.path();

        let mut buf = Vec::with_capacity(args.size() as usize);
        let n = futures::io::copy(r, &mut buf).await?;
        if n != args.size() {
            return Err(other(ObjectError::new(
                Operation::Write,
                path,
                anyhow!("write short, expect {} actual {}", args.size(), n),
            )));
        }
        let mut map = self.inner.lock();
        map.insert(path.to_string(), Bytes::from(buf));

        Ok(n)
    }

    async fn stat(&self, args: &OpStat) -> Result<ObjectMetadata> {
        let path = args.path();

        if path.ends_with('/') {
            let mut meta = ObjectMetadata::default();
            meta.set_mode(ObjectMode::DIR);

            return Ok(meta);
        }

        let map = self.inner.lock();

        let data = map.get(path).ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                ObjectError::new(Operation::Read, path, anyhow!("key not exists in map")),
            )
        })?;

        let mut meta = ObjectMetadata::default();
        meta.set_mode(ObjectMode::FILE)
            .set_content_length(data.len() as u64);

        Ok(meta)
    }

    async fn delete(&self, args: &OpDelete) -> Result<()> {
        let path = args.path();

        let mut map = self.inner.lock();
        map.remove(path);

        Ok(())
    }

    async fn list(&self, args: &OpList) -> Result<DirStreamer> {
        let mut path = args.path().to_string();
        if path == "/" {
            path.clear();
        }

        let map = self.inner.lock();

        let paths = map
            .iter()
            // Make sure k is at the same level with input path.
            .filter_map(|(k, _)| {
                let k = k.as_str();
                // `/xyz` should not belong to `/abc`
                if !k.starts_with(&path) {
                    return None;
                }

                // We should remove `/abc` if self
                if k == path {
                    return None;
                }

                match k[path.len()..].find('/') {
                    // File `/abc/def.csv` must belong to `/abc`
                    None => Some(k.to_string()),
                    Some(idx) => {
                        // The index of first `/` after `/abc`.
                        let dir_idx = idx + 1 + path.len();

                        if dir_idx == k.len() {
                            // Dir `/abc/def/` belongs to `/abc/`
                            Some(k.to_string())
                        } else {
                            // File/Dir `/abc/def/xyz` deoesn't belongs to `/abc`.
                            // But we need to list `/abc/def` out so that we can walk down.
                            Some(k[..dir_idx].to_string())
                        }
                    }
                }
            })
            .collect::<HashSet<_>>();

        Ok(Box::new(DirStream {
            backend: Arc::new(self.clone()),
            paths: paths.into_iter().collect(),
            idx: 0,
        }))
    }
}

struct MapWriter {
    path: String,
    size: u64,
    map: Arc<Mutex<HashMap<String, Bytes>>>,

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
                Operation::Write,
                &self.path,
                anyhow!(
                    "write short, expect {} actual {}",
                    self.size,
                    self.buf.len()
                ),
            ))));
        }

        let buf = mem::take(&mut self.buf);
        let mut map = self.map.lock();
        map.insert(self.path.clone(), buf.freeze());

        Poll::Ready(Ok(()))
    }
}

struct DirStream {
    backend: Arc<Backend>,
    paths: Vec<String>,
    idx: usize,
}

impl futures::Stream for DirStream {
    type Item = Result<DirEntry>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.idx >= self.paths.len() {
            return Poll::Ready(None);
        }

        let idx = self.idx;
        self.idx += 1;

        let path = self.paths.get(idx).expect("path must valid");

        let de = if path.ends_with('/') {
            DirEntry::new(self.backend.clone(), ObjectMode::DIR, path)
        } else {
            DirEntry::new(self.backend.clone(), ObjectMode::FILE, path)
        };

        Poll::Ready(Some(Ok(de)))
    }
}
