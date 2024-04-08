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
use std::vec::IntoIter;

use async_trait::async_trait;
use bytes::BytesMut;
use bytes::{Buf, Bytes};

use super::Adapter;
use crate::raw::oio::HierarchyLister;
use crate::raw::*;
use crate::*;

/// Backend of kv service. If the storage service is one k-v-like service, it should implement this kv [`Backend`] by right.
///
/// `Backend` implements one general logic on how to read, write, scan the data from one kv store efficiently.
/// And the [`Adapter`] held by `Backend` will handle how to communicate with one k-v-like service really and provides
/// a series of basic operation for this service.
///
/// OpenDAL developer can implement one new k-v store backend easily with help of this Backend.
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Adapter> Accessor for Backend<S> {
    type Reader = Bytes;
    type BlockingReader = Bytes;
    type Writer = KvWriter<S>;
    type BlockingWriter = KvWriter<S>;
    type Lister = HierarchyLister<KvLister>;
    type BlockingLister = HierarchyLister<KvLister>;

    fn info(&self) -> AccessorInfo {
        let mut am: AccessorInfo = self.kv.metadata().into();
        am.set_root(&self.root);

        let mut cap = am.native_capability();
        if cap.read {
            cap.stat = true;
        }

        if cap.write {
            cap.write_can_empty = true;
            cap.delete = true;
        }

        if cap.list {
            cap.list_with_recursive = true;
        }

        am.set_native_capability(cap);

        am
    }

    async fn read(&self, path: &str, _: OpRead) -> Result<(RpRead, Self::Reader)> {
        let p = build_abs_path(&self.root, path);

        let bs = match self.kv.get(&p).await? {
            Some(bs) => bs,
            None => return Err(Error::new(ErrorKind::NotFound, "kv doesn't have this path")),
        };

        let bs = Bytes::from(bs);

        Ok((RpRead::new(), bs))
    }

    fn blocking_read(&self, path: &str, _: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        let p = build_abs_path(&self.root, path);

        let bs = match self.kv.blocking_get(&p)? {
            Some(bs) => bs,
            None => return Err(Error::new(ErrorKind::NotFound, "kv doesn't have this path")),
        };

        let bs = Bytes::from(bs);
        Ok((RpRead::new(), bs))
    }

    async fn write(&self, path: &str, _: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let p = build_abs_path(&self.root, path);

        Ok((RpWrite::new(), KvWriter::new(self.kv.clone(), p)))
    }

    fn blocking_write(&self, path: &str, _: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        let p = build_abs_path(&self.root, path);

        Ok((RpWrite::new(), KvWriter::new(self.kv.clone(), p)))
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_abs_path(&self.root, path);

        if p == build_abs_path(&self.root, "") {
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

        if p == build_abs_path(&self.root, "") {
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

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let p = build_abs_path(&self.root, path);
        let res = self.kv.scan(&p).await?;
        let lister = KvLister::new(&self.root, res);
        let lister = HierarchyLister::new(lister, path, args.recursive());

        Ok((RpList::default(), lister))
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        let p = build_abs_path(&self.root, path);
        let res = self.kv.blocking_scan(&p)?;
        let lister = KvLister::new(&self.root, res);
        let lister = HierarchyLister::new(lister, path, args.recursive());

        Ok((RpList::default(), lister))
    }
}

pub struct KvLister {
    root: String,
    inner: IntoIter<String>,
}

impl KvLister {
    fn new(root: &str, inner: Vec<String>) -> Self {
        Self {
            root: root.to_string(),
            inner: inner.into_iter(),
        }
    }

    fn inner_next(&mut self) -> Option<oio::Entry> {
        self.inner.next().map(|v| {
            let mode = if v.ends_with('/') {
                EntryMode::DIR
            } else {
                EntryMode::FILE
            };

            oio::Entry::new(&build_rel_path(&self.root, &v), Metadata::new(mode))
        })
    }
}

impl oio::List for KvLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        Ok(self.inner_next())
    }
}

impl oio::BlockingList for KvLister {
    fn next(&mut self) -> Result<Option<oio::Entry>> {
        Ok(self.inner_next())
    }
}

pub struct KvWriter<S> {
    kv: Arc<S>,
    path: String,

    buffer: Buffer,
}

impl<S> KvWriter<S> {
    fn new(kv: Arc<S>, path: String) -> Self {
        KvWriter {
            kv,
            path,
            buffer: Buffer::Active(BytesMut::new()),
        }
    }
}

/// TODO: replace with FlexBuf.
enum Buffer {
    Active(BytesMut),
    Frozen(Bytes),
}

/// # Safety
///
/// We will only take `&mut Self` reference for KvWriter.
unsafe impl<S: Adapter> Sync for KvWriter<S> {}

impl<S: Adapter> oio::Write for KvWriter<S> {
    async unsafe fn write(&mut self, bs: oio::Buffer) -> Result<usize> {
        match &mut self.buffer {
            Buffer::Active(buf) => {
                buf.extend_from_slice(bs.chunk());
                Ok(bs.chunk().len())
            }
            Buffer::Frozen(_) => unreachable!("KvWriter should not be frozen during poll_write"),
        }
    }

    async fn close(&mut self) -> Result<()> {
        let buf = match &mut self.buffer {
            Buffer::Active(buf) => {
                let buf = buf.split().freeze();
                self.buffer = Buffer::Frozen(buf.clone());
                buf
            }
            Buffer::Frozen(buf) => buf.clone(),
        };

        self.kv.set(&self.path, &buf).await
    }

    async fn abort(&mut self) -> Result<()> {
        self.buffer = Buffer::Active(BytesMut::new());
        Ok(())
    }
}

impl<S: Adapter> oio::BlockingWrite for KvWriter<S> {
    unsafe fn write(&mut self, bs: oio::Buffer) -> Result<usize> {
        match &mut self.buffer {
            Buffer::Active(buf) => {
                buf.extend_from_slice(bs.chunk());
                Ok(bs.chunk().len())
            }
            Buffer::Frozen(_) => unreachable!("KvWriter should not be frozen during poll_write"),
        }
    }

    fn close(&mut self) -> Result<()> {
        let buf = match &mut self.buffer {
            Buffer::Active(buf) => {
                let buf = buf.split().freeze();
                self.buffer = Buffer::Frozen(buf.clone());
                buf
            }
            Buffer::Frozen(buf) => buf.clone(),
        };

        self.kv.blocking_set(&self.path, &buf)?;
        Ok(())
    }
}
