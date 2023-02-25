// Copyright 2022 Datafuse Labs
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

use async_trait::async_trait;
use futures::AsyncReadExt;

use super::Adapter;
use crate::ops::*;
use crate::raw::*;
use crate::*;

/// Backend of kv service.
#[derive(Debug, Clone)]
pub struct Backend<S: Adapter> {
    kv: S,
    root: String,
}

impl<S> Backend<S>
where
    S: Adapter,
{
    /// Create a new kv backend.
    pub fn new(kv: S) -> Self {
        Self {
            kv,
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
    type Reader = output::Cursor;
    type BlockingReader = output::Cursor;
    type Pager = KvPager;
    type BlockingPager = KvPager;

    fn metadata(&self) -> AccessorMetadata {
        let mut am: AccessorMetadata = self.kv.metadata().into();
        am.set_root(&self.root)
            .set_hints(AccessorHint::ReadStreamable | AccessorHint::ReadSeekable);

        am
    }

    async fn create(&self, path: &str, _: OpCreate) -> Result<RpCreate> {
        let p = build_abs_path(&self.root, path);
        self.kv.set(&p, &[]).await?;
        Ok(RpCreate::default())
    }

    fn blocking_create(&self, path: &str, _: OpCreate) -> Result<RpCreate> {
        let p = build_abs_path(&self.root, path);
        self.kv.blocking_set(&p, &[])?;

        Ok(RpCreate::default())
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let p = build_abs_path(&self.root, path);

        let bs = match self.kv.get(&p).await? {
            Some(bs) => bs,
            None => {
                return Err(Error::new(
                    ErrorKind::ObjectNotFound,
                    "kv doesn't have this path",
                ))
            }
        };

        let bs = self.apply_range(bs, args.range());

        let length = bs.len();
        Ok((RpRead::new(length as u64), output::Cursor::from(bs)))
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        let p = build_abs_path(&self.root, path);

        let bs = match self.kv.blocking_get(&p)? {
            Some(bs) => bs,
            None => {
                return Err(Error::new(
                    ErrorKind::ObjectNotFound,
                    "kv doesn't have this path",
                ))
            }
        };

        let bs = self.apply_range(bs, args.range());
        Ok((RpRead::new(bs.len() as u64), output::Cursor::from(bs)))
    }

    async fn write(&self, path: &str, args: OpWrite, mut r: input::Reader) -> Result<RpWrite> {
        let p = build_abs_path(&self.root, path);

        let mut bs = Vec::with_capacity(args.size() as usize);
        r.read_to_end(&mut bs)
            .await
            .map_err(|err| Error::new(ErrorKind::Unexpected, "read from source").set_source(err))?;

        self.kv.set(&p, &bs).await?;

        Ok(RpWrite::new(args.size()))
    }

    fn blocking_write(
        &self,
        path: &str,
        args: OpWrite,
        mut r: input::BlockingReader,
    ) -> Result<RpWrite> {
        let p = build_abs_path(&self.root, path);

        let mut bs = Vec::with_capacity(args.size() as usize);
        r.read_to_end(&mut bs)
            .map_err(|err| Error::new(ErrorKind::Unexpected, "read from source").set_source(err))?;

        self.kv.blocking_set(&p, &bs)?;

        Ok(RpWrite::new(args.size()))
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_abs_path(&self.root, path);

        if p.is_empty() || p.ends_with('/') {
            Ok(RpStat::new(ObjectMetadata::new(ObjectMode::DIR)))
        } else {
            let bs = self.kv.get(&p).await?;
            match bs {
                Some(bs) => Ok(RpStat::new(
                    ObjectMetadata::new(ObjectMode::FILE).with_content_length(bs.len() as u64),
                )),
                None => Err(Error::new(
                    ErrorKind::ObjectNotFound,
                    "kv doesn't have this path",
                )),
            }
        }
    }

    fn blocking_stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_abs_path(&self.root, path);

        if p.is_empty() || p.ends_with('/') {
            Ok(RpStat::new(ObjectMetadata::new(ObjectMode::DIR)))
        } else {
            let bs = self.kv.blocking_get(&p)?;
            match bs {
                Some(bs) => Ok(RpStat::new(
                    ObjectMetadata::new(ObjectMode::FILE).with_content_length(bs.len() as u64),
                )),
                None => Err(Error::new(
                    ErrorKind::ObjectNotFound,
                    "kv doesn't have this path",
                )),
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
        let root = if root == "/" {
            "".to_string()
        } else {
            root.to_string()
        };

        Self {
            root,
            inner: Some(inner),
        }
    }

    fn inner_next_page(&mut self) -> Option<Vec<output::Entry>> {
        let res = self
            .inner
            .take()?
            .into_iter()
            .map(|v| {
                let mode = if v.ends_with('/') {
                    ObjectMode::DIR
                } else {
                    ObjectMode::FILE
                };

                output::Entry::new(
                    v.strip_prefix(&self.root)
                        .expect("key must start with root"),
                    ObjectMetadata::new(mode),
                )
            })
            .collect();

        Some(res)
    }
}

#[async_trait]
impl output::Page for KvPager {
    async fn next_page(&mut self) -> Result<Option<Vec<output::Entry>>> {
        Ok(self.inner_next_page())
    }
}

impl output::BlockingPage for KvPager {
    fn next_page(&mut self) -> Result<Option<Vec<output::Entry>>> {
        Ok(self.inner_next_page())
    }
}
