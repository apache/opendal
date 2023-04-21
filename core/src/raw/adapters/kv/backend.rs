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

use async_trait::async_trait;
use bytes::Bytes;

use super::Adapter;
use crate::ops::*;
use crate::raw::*;
use crate::*;

/// Backend of kv service.
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
        let mut am: AccessorInfo = self.kv.metadata().into();
        am.set_root(&self.root);

        let cap = am.capability_mut();
        if cap.read {
            cap.read_can_seek = true;
            cap.read_can_next = true;
            cap.stat = true;
        }

        if cap.write {
            cap.create_dir = true;
            cap.delete = true;
        }

        am
    }

    async fn create_dir(&self, path: &str, _: OpCreate) -> Result<RpCreate> {
        let p = build_abs_path(&self.root, path);
        self.kv.set(&p, &[]).await?;
        Ok(RpCreate::default())
    }

    fn blocking_create_dir(&self, path: &str, _: OpCreate) -> Result<RpCreate> {
        let p = build_abs_path(&self.root, path);
        self.kv.blocking_set(&p, &[])?;

        Ok(RpCreate::default())
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let p = build_abs_path(&self.root, path);

        let bs = match self.kv.get(&p).await? {
            Some(bs) => bs,
            None => return Err(Error::new(ErrorKind::NotFound, "kv doesn't have this path")),
        };

        let bs = self.apply_range(bs, args.range());

        let length = bs.len();
        Ok((RpRead::new(length as u64), oio::Cursor::from(bs)))
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        let p = build_abs_path(&self.root, path);

        let bs = match self.kv.blocking_get(&p)? {
            Some(bs) => bs,
            None => return Err(Error::new(ErrorKind::NotFound, "kv doesn't have this path")),
        };

        let bs = self.apply_range(bs, args.range());
        Ok((RpRead::new(bs.len() as u64), oio::Cursor::from(bs)))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        if args.content_length().is_none() {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "write without content length is not supported",
            ));
        }

        let p = build_abs_path(&self.root, path);

        Ok((RpWrite::new(), KvWriter::new(self.kv.clone(), p)))
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        if args.content_length().is_none() {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "write without content length is not supported",
            ));
        }

        let p = build_abs_path(&self.root, path);

        Ok((RpWrite::new(), KvWriter::new(self.kv.clone(), p)))
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_abs_path(&self.root, path);

        if p.is_empty() || p.ends_with('/') {
            Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
        } else {
            let bs = self.kv.get(&p).await?;
            match bs {
                Some(bs) => Ok(RpStat::new(
                    Metadata::new(EntryMode::FILE).with_content_length(bs.len() as u64),
                )),
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
                Some(bs) => Ok(RpStat::new(
                    Metadata::new(EntryMode::FILE).with_content_length(bs.len() as u64),
                )),
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

    async fn scan(&self, path: &str, _: OpScan) -> Result<(RpScan, Self::Pager)> {
        let p = build_abs_path(&self.root, path);
        let res = self.kv.scan(&p).await?;
        let pager = KvPager::new(&self.root, res);

        Ok((RpScan::default(), pager))
    }

    fn blocking_scan(&self, path: &str, _: OpScan) -> Result<(RpScan, Self::BlockingPager)> {
        let p = build_abs_path(&self.root, path);
        let res = self.kv.blocking_scan(&p)?;
        let pager = KvPager::new(&self.root, res);

        Ok((RpScan::default(), pager))
    }
}

impl<S> Backend<S>
where
    S: Adapter,
{
    fn apply_range(&self, mut bs: Vec<u8>, br: BytesRange) -> Vec<u8> {
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

    /// TODO: if kv supports append, we can use them directly.
    buf: Option<Vec<u8>>,
}

impl<S> KvWriter<S> {
    fn new(kv: Arc<S>, path: String) -> Self {
        KvWriter {
            kv,
            path,
            buf: None,
        }
    }
}

#[async_trait]
impl<S: Adapter> oio::Write for KvWriter<S> {
    // TODO: we need to support append in the future.
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        self.buf = Some(bs.into());

        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "output writer doesn't support abort",
        ))
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(buf) = self.buf.as_deref() {
            self.kv.set(&self.path, buf).await?;
        }

        Ok(())
    }
}

impl<S: Adapter> oio::BlockingWrite for KvWriter<S> {
    fn write(&mut self, bs: Bytes) -> Result<()> {
        self.buf = Some(bs.into());

        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        if let Some(buf) = self.buf.as_deref() {
            self.kv.blocking_set(&self.path, buf)?;
        }

        Ok(())
    }
}
