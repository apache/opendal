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

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::FutureExt;

use crate::raw::*;
use crate::*;

/// SubdirLayer to switch to subdir for existing operator.
///
/// # Examples
///
/// ```
/// use anyhow::Result;
/// use opendal::layers::SubdirLayer;
/// use opendal::Operator;
/// use opendal::Scheme;
///
/// let _ = Operator::from_env(Scheme::Fs)
///     .expect("must init")
///     .layer(SubdirLayer::new("path/to/subdir"));
/// ```
#[derive(Debug, Clone)]
pub struct SubdirLayer {
    subdir: String,
}

impl SubdirLayer {
    /// Create a new subdir layer.
    pub fn new(subdir: &str) -> SubdirLayer {
        let dir = normalize_root(subdir);

        SubdirLayer {
            // Always trim the first `/`
            subdir: dir[1..].to_string(),
        }
    }
}

impl<A: Accessor> Layer<A> for SubdirLayer {
    type LayeredAccessor = SubdirAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        SubdirAccessor {
            subdir: self.subdir.clone(),
            inner,
        }
    }
}

#[derive(Debug, Clone)]
struct SubdirAccessor<A: Accessor> {
    /// Subdir must be like `abc/`
    subdir: String,
    inner: A,
}

impl<A: Accessor> SubdirAccessor<A> {
    fn prepend_subdir(&self, path: &str) -> String {
        if path == "/" {
            self.subdir.clone()
        } else {
            self.subdir.clone() + path
        }
    }
}

#[async_trait]
impl<A: Accessor> LayeredAccessor for SubdirAccessor<A> {
    type Inner = A;
    type Reader = A::Reader;
    type BlockingReader = A::BlockingReader;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    fn metadata(&self) -> AccessorMetadata {
        let mut meta = self.inner.metadata();
        meta.set_root(&format!("{}{}", meta.root(), self.subdir));
        meta
    }

    fn create(&self, path: &str, args: OpCreate) -> FutureResult<RpCreate> {
        let path = self.prepend_subdir(path);

        self.inner.create(&path, args)
    }

    fn read(&self, path: &str, args: OpRead) -> FutureResult<(RpRead, Self::Reader)> {
        let path = self.prepend_subdir(path);

        self.inner.read(&path, args)
    }

    fn write(&self, path: &str, args: OpWrite, r: input::Reader) -> FutureResult<RpWrite> {
        let path = self.prepend_subdir(path);

        self.inner.write(&path, args, r)
    }

    fn stat(&self, path: &str, args: OpStat) -> FutureResult<RpStat> {
        let path = self.prepend_subdir(path);

        self.inner.stat(&path, args)
    }

    fn delete(&self, path: &str, args: OpDelete) -> FutureResult<RpDelete> {
        let path = self.prepend_subdir(path);

        self.inner.delete(&path, args)
    }

    fn list(&self, path: &str, args: OpList) -> FutureResult<(RpList, ObjectPager)> {
        let path = self.prepend_subdir(path);

        Box::pin(self.inner.list(&path, args).map(|v| {
            v.map(|(rp, p)| {
                (
                    rp,
                    Box::new(SubdirPager::new(&self.subdir, p)) as ObjectPager,
                )
            })
        }))
    }

    fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        let path = self.prepend_subdir(path);

        self.inner.presign(&path, args)
    }

    fn create_multipart(
        &self,
        path: &str,
        args: OpCreateMultipart,
    ) -> FutureResult<RpCreateMultipart> {
        let path = self.prepend_subdir(path);

        self.inner.create_multipart(&path, args)
    }

    fn write_multipart(
        &self,
        path: &str,
        args: OpWriteMultipart,
        r: input::Reader,
    ) -> FutureResult<RpWriteMultipart> {
        let path = self.prepend_subdir(path);

        self.inner.write_multipart(&path, args, r)
    }

    fn complete_multipart(
        &self,
        path: &str,
        args: OpCompleteMultipart,
    ) -> FutureResult<RpCompleteMultipart> {
        let path = self.prepend_subdir(path);

        self.inner.complete_multipart(&path, args)
    }

    fn abort_multipart(
        &self,
        path: &str,
        args: OpAbortMultipart,
    ) -> FutureResult<RpAbortMultipart> {
        let path = self.prepend_subdir(path);

        self.inner.abort_multipart(&path, args)
    }

    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        let path = self.prepend_subdir(path);

        self.inner.blocking_create(&path, args)
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        let path = self.prepend_subdir(path);

        self.inner.blocking_read(&path, args)
    }

    fn blocking_write(
        &self,
        path: &str,
        args: OpWrite,
        r: input::BlockingReader,
    ) -> Result<RpWrite> {
        let path = self.prepend_subdir(path);

        self.inner.blocking_write(&path, args, r)
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let path = self.prepend_subdir(path);

        self.inner.blocking_stat(&path, args)
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        let path = self.prepend_subdir(path);

        self.inner.blocking_delete(&path, args)
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, BlockingObjectPager)> {
        let path = self.prepend_subdir(path);
        let (rp, pager) = self.inner.blocking_list(&path, args)?;

        Ok((rp, Box::new(BlockingSubdirPager::new(&self.subdir, pager))))
    }
}

fn strip_subdir(subdir: &str, path: &str) -> String {
    path.strip_prefix(subdir)
        .expect("strip subdir must succeed")
        .to_string()
}

struct SubdirPager {
    subdir: String,
    inner: ObjectPager,
}

impl SubdirPager {
    fn new(subdir: &str, inner: ObjectPager) -> Self {
        Self {
            subdir: subdir.to_string(),
            inner,
        }
    }
}

#[async_trait]
impl ObjectPage for SubdirPager {
    async fn next_page(&mut self) -> Result<Option<Vec<ObjectEntry>>> {
        let res = self.inner.next_page().await?;
        let res = res.map(|res| {
            res.into_iter()
                .map(|mut v| {
                    v.set_path(&strip_subdir(&self.subdir, v.path()));
                    v
                })
                .collect()
        });

        Ok(res)
    }
}

struct BlockingSubdirPager {
    subdir: String,
    inner: BlockingObjectPager,
}

impl BlockingSubdirPager {
    fn new(subdir: &str, inner: BlockingObjectPager) -> Self {
        Self {
            subdir: subdir.to_string(),
            inner,
        }
    }
}

impl BlockingObjectPage for BlockingSubdirPager {
    fn next_page(&mut self) -> Result<Option<Vec<ObjectEntry>>> {
        let res = self.inner.next_page()?;

        let res = res.map(|res| {
            res.into_iter()
                .map(|mut v| {
                    v.set_path(&strip_subdir(&self.subdir, v.path()));
                    v
                })
                .collect()
        });

        Ok(res)
    }
}
