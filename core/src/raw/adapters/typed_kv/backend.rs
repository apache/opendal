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
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::FutureExt;

use super::Adapter;
use super::Value;
use crate::raw::*;
use crate::*;

/// The typed kv backend which implements Accessor for for typed kv adapter.
#[derive(Debug, Clone)]
pub struct Backend<S: Adapter> {
    kv: Arc<S>,
    root: String,
}

impl<S> Backend<S>
where
    S: Adapter,
{
    /// Create a new kv backend.
    pub fn new(kv: S) -> Self {
        Self {
            kv: Arc::new(kv),
            root: "/".to_string(),
        }
    }

    /// Configure root within this backend.
    pub fn with_root(mut self, root: &str) -> Self {
        self.root = normalize_root(root);
        self
    }
}

#[async_trait]
impl<S: Adapter> Accessor for Backend<S> {
    type Reader = oio::Cursor;
    type BlockingReader = oio::Cursor;
    type Writer = KvWriter<S>;
    type BlockingWriter = KvWriter<S>;
    type Pager = KvPager;
    type BlockingPager = KvPager;

    fn info(&self) -> AccessorInfo {
        let kv_info = self.kv.info();
        let mut am: AccessorInfo = AccessorInfo::default();
        am.set_root(&self.root);
        am.set_scheme(kv_info.scheme());
        am.set_name(kv_info.name());

        let kv_cap = kv_info.capabilities();
        let mut cap = Capability::default();
        if kv_cap.get {
            cap.read = true;
            cap.read_can_seek = true;
            cap.read_can_next = true;
            cap.read_with_range = true;
            cap.stat = true;
        }

        if kv_cap.set {
            cap.write = true;
            cap.write_can_empty = true;
            cap.create_dir = true;
        }

        if kv_cap.delete {
            cap.delete = true;
        }

        if kv_cap.scan {
            cap.list = true;
            cap.list_without_delimiter = true;
        }

        if cap.read && cap.write {
            cap.copy = true;
        }

        if cap.read && cap.write && cap.delete {
            cap.rename = true;
        }
        cap.blocking = true;

        am.set_native_capability(cap);

        am
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        let p = build_abs_path(&self.root, path);
        self.kv.set(&p, Value::new_dir()).await?;
        Ok(RpCreateDir::default())
    }

    fn blocking_create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        let p = build_abs_path(&self.root, path);
        self.kv.blocking_set(&p, Value::new_dir())?;

        Ok(RpCreateDir::default())
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let p = build_abs_path(&self.root, path);

        let bs = match self.kv.get(&p).await? {
            // TODO: we can reuse the metadata in value to build content range.
            Some(bs) => bs.value,
            None => return Err(Error::new(ErrorKind::NotFound, "kv doesn't have this path")),
        };

        let bs = self.apply_range(bs, args.range());

        let length = bs.len();
        Ok((RpRead::new(length as u64), oio::Cursor::from(bs)))
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        let p = build_abs_path(&self.root, path);

        let bs = match self.kv.blocking_get(&p)? {
            // TODO: we can reuse the metadata in value to build content range.
            Some(bs) => bs.value,
            None => return Err(Error::new(ErrorKind::NotFound, "kv doesn't have this path")),
        };

        let bs = self.apply_range(bs, args.range());
        Ok((RpRead::new(bs.len() as u64), oio::Cursor::from(bs)))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let p = build_abs_path(&self.root, path);

        Ok((RpWrite::new(), KvWriter::new(self.kv.clone(), p, args)))
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        let p = build_abs_path(&self.root, path);

        Ok((RpWrite::new(), KvWriter::new(self.kv.clone(), p, args)))
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_abs_path(&self.root, path);

        if p.is_empty() || p.ends_with('/') {
            Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
        } else {
            let bs = self.kv.get(&p).await?;
            match bs {
                Some(bs) => Ok(RpStat::new(bs.metadata)),
                None => Err(Error::new(ErrorKind::NotFound, "kv doesn't have this path")),
            }
        }
    }

    fn blocking_stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_abs_path(&self.root, path);

        if p.is_empty() || p.ends_with('/') {
            Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
        } else {
            let bs = self.kv.blocking_get(&p)?;
            match bs {
                Some(bs) => Ok(RpStat::new(bs.metadata)),
                None => Err(Error::new(ErrorKind::NotFound, "kv doesn't have this path")),
            }
        }
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let p = build_abs_path(&self.root, path);

        self.kv.delete(&p).await?;
        Ok(RpDelete::default())
    }

    fn blocking_delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let p = build_abs_path(&self.root, path);

        self.kv.blocking_delete(&p)?;
        Ok(RpDelete::default())
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        if !args.delimiter().is_empty() {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "kv doesn't support delimiter",
            ));
        }

        let p = build_abs_path(&self.root, path);
        let res = self.kv.scan(&p).await?;
        let pager = KvPager::new(&self.root, res);

        Ok((RpList::default(), pager))
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
        if !args.delimiter().is_empty() {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "kv doesn't support delimiter",
            ));
        }

        let p = build_abs_path(&self.root, path);
        let res = self.kv.blocking_scan(&p)?;
        let pager = KvPager::new(&self.root, res);

        Ok((RpList::default(), pager))
    }

    async fn rename(&self, from: &str, to: &str, _: OpRename) -> Result<RpRename> {
        let from = build_abs_path(&self.root, from);
        let to = build_abs_path(&self.root, to);

        let bs = match self.kv.get(&from).await? {
            // TODO: we can reuse the metadata in value to build content range.
            Some(bs) => bs,
            None => return Err(Error::new(ErrorKind::NotFound, "kv doesn't have this path")),
        };

        self.kv.set(&to, bs).await?;
        self.kv.delete(&from).await?;
        Ok(RpRename::default())
    }

    fn blocking_rename(&self, from: &str, to: &str, _: OpRename) -> Result<RpRename> {
        let from = build_abs_path(&self.root, from);
        let to = build_abs_path(&self.root, to);

        let bs = match self.kv.blocking_get(&from)? {
            // TODO: we can reuse the metadata in value to build content range.
            Some(bs) => bs,
            None => return Err(Error::new(ErrorKind::NotFound, "kv doesn't have this path")),
        };

        self.kv.blocking_set(&to, bs)?;
        self.kv.blocking_delete(&from)?;
        Ok(RpRename::default())
    }

    async fn copy(&self, from: &str, to: &str, _: OpCopy) -> Result<RpCopy> {
        let from = build_abs_path(&self.root, from);
        let to = build_abs_path(&self.root, to);

        let bs = match self.kv.get(&from).await? {
            // TODO: we can reuse the metadata in value to build content range.
            Some(bs) => bs,
            None => return Err(Error::new(ErrorKind::NotFound, "kv doesn't have this path")),
        };

        self.kv.set(&to, bs).await?;

        Ok(RpCopy::default())
    }

    fn blocking_copy(&self, from: &str, to: &str, _: OpCopy) -> Result<RpCopy> {
        let from = build_abs_path(&self.root, from);
        let to = build_abs_path(&self.root, to);

        let bs = match self.kv.blocking_get(&from)? {
            // TODO: we can reuse the metadata in value to build content range.
            Some(bs) => bs,
            None => return Err(Error::new(ErrorKind::NotFound, "kv doesn't have this path")),
        };

        self.kv.blocking_set(&to, bs)?;

        Ok(RpCopy::default())
    }
}

impl<S> Backend<S>
where
    S: Adapter,
{
    fn apply_range(&self, mut bs: Bytes, br: BytesRange) -> Bytes {
        match (br.offset(), br.size()) {
            (Some(offset), Some(size)) => {
                let mut bs = bs.split_off(offset as usize);
                if (size as usize) < bs.len() {
                    let _ = bs.split_off(size as usize);
                }
                bs
            }
            (Some(offset), None) => bs.split_off(offset as usize),
            (None, Some(size)) => bs.split_off(bs.len() - size as usize),
            (None, None) => bs,
        }
    }
}

pub struct KvPager {
    root: String,
    inner: Option<Vec<String>>,
}

impl KvPager {
    fn new(root: &str, inner: Vec<String>) -> Self {
        Self {
            root: root.to_string(),
            inner: Some(inner),
        }
    }

    fn inner_next_page(&mut self) -> Option<Vec<oio::Entry>> {
        let res = self
            .inner
            .take()?
            .into_iter()
            .map(|v| {
                let mode = if v.ends_with('/') {
                    EntryMode::DIR
                } else {
                    EntryMode::FILE
                };

                oio::Entry::new(&build_rel_path(&self.root, &v), Metadata::new(mode))
            })
            .collect();

        Some(res)
    }
}

#[async_trait]
impl oio::Page for KvPager {
    async fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        Ok(self.inner_next_page())
    }
}

impl oio::BlockingPage for KvPager {
    fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        Ok(self.inner_next_page())
    }
}

pub struct KvWriter<S> {
    kv: Arc<S>,
    path: String,

    op: OpWrite,
    buf: Option<Vec<u8>>,
    value: Option<Value>,
    future: Option<BoxFuture<'static, Result<()>>>,
}

/// # Safety
///
/// We will only take `&mut Self` reference for KvWriter.
unsafe impl<S: Adapter> Sync for KvWriter<S> {}

impl<S> KvWriter<S> {
    fn new(kv: Arc<S>, path: String, op: OpWrite) -> Self {
        KvWriter {
            kv,
            path,
            op,
            buf: None,
            value: None,
            future: None,
        }
    }

    fn build(&mut self) -> Value {
        let value = self.buf.take().map(Bytes::from).unwrap_or_default();

        let mut metadata = Metadata::new(EntryMode::FILE);
        metadata.set_content_length(value.len() as u64);

        if let Some(v) = self.op.cache_control() {
            metadata.set_cache_control(v);
        }
        if let Some(v) = self.op.content_disposition() {
            metadata.set_content_disposition(v);
        }
        if let Some(v) = self.op.content_type() {
            metadata.set_content_type(v);
        }

        Value { metadata, value }
    }
}

#[async_trait]
impl<S: Adapter> oio::Write for KvWriter<S> {
    fn poll_write(&mut self, _: &mut Context<'_>, bs: &dyn oio::WriteBuf) -> Poll<Result<usize>> {
        if self.future.is_some() {
            self.future = None;
            return Poll::Ready(Err(Error::new(
                ErrorKind::Unexpected,
                "there is a future on going, it's maybe a bug to go into this case",
            )));
        }

        let size = bs.chunk().len();

        let mut buf = self.buf.take().unwrap_or_else(|| Vec::with_capacity(size));
        buf.extend_from_slice(bs.chunk());

        self.buf = Some(buf);

        Poll::Ready(Ok(size))
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        loop {
            match self.future.as_mut() {
                Some(fut) => {
                    let res = ready!(fut.poll_unpin(cx));
                    self.future = None;
                    return Poll::Ready(res);
                }
                None => {
                    let kv = self.kv.clone();
                    let path = self.path.clone();
                    let value = match &self.value {
                        Some(value) => value.clone(),
                        None => {
                            let value = self.build();
                            self.value = Some(value.clone());
                            value
                        }
                    };

                    let fut = async move { kv.set(&path, value).await };
                    self.future = Some(Box::pin(fut));
                }
            }
        }
    }

    fn poll_abort(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        if self.future.is_some() {
            self.future = None;
            return Poll::Ready(Err(Error::new(
                ErrorKind::Unexpected,
                "there is a future on going, it's maybe a bug to go into this case",
            )));
        }

        self.buf = None;
        Poll::Ready(Ok(()))
    }
}

impl<S: Adapter> oio::BlockingWrite for KvWriter<S> {
    fn write(&mut self, bs: &dyn oio::WriteBuf) -> Result<usize> {
        let size = bs.chunk().len();

        let mut buf = self.buf.take().unwrap_or_else(|| Vec::with_capacity(size));
        buf.extend_from_slice(bs.chunk());

        self.buf = Some(buf);

        Ok(size)
    }

    fn close(&mut self) -> Result<()> {
        let kv = self.kv.clone();
        let value = match &self.value {
            Some(value) => value.clone(),
            None => {
                let value = self.build();
                self.value = Some(value.clone());
                value
            }
        };

        kv.blocking_set(&self.path, value)?;
        Ok(())
    }
}
