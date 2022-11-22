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
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use futures::Stream;

use crate::ops::*;
use crate::path::normalize_root;
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

impl Layer for SubdirLayer {
    fn layer(&self, inner: Arc<dyn Accessor>) -> Arc<dyn Accessor> {
        Arc::new(SubdirAccessor {
            subdir: self.subdir.clone(),
            inner,
        })
    }
}

#[derive(Debug, Clone)]
struct SubdirAccessor {
    /// Subdir must be like `abc/`
    subdir: String,
    inner: Arc<dyn Accessor>,
}

impl SubdirAccessor {
    fn prepend_subdir(&self, path: &str) -> String {
        if path == "/" {
            self.subdir.clone()
        } else {
            self.subdir.clone() + path
        }
    }
}

#[async_trait]
impl Accessor for SubdirAccessor {
    fn inner(&self) -> Option<Arc<dyn Accessor>> {
        Some(self.inner.clone())
    }

    fn metadata(&self) -> AccessorMetadata {
        let mut meta = self.inner.metadata();
        meta.set_root(&format!("{}{}", meta.root(), self.subdir));
        meta
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        let path = self.prepend_subdir(path);

        self.inner.create(&path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, BytesReader)> {
        let path = self.prepend_subdir(path);

        self.inner.read(&path, args).await
    }

    async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<RpWrite> {
        let path = self.prepend_subdir(path);

        self.inner.write(&path, args, r).await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let path = self.prepend_subdir(path);

        self.inner.stat(&path, args).await
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        let path = self.prepend_subdir(path);

        self.inner.delete(&path, args).await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<ObjectStreamer> {
        let path = self.prepend_subdir(path);

        Ok(Box::new(SubdirStreamer::new(
            Arc::new(self.clone()),
            &self.subdir,
            self.inner.list(&path, args).await?,
        )))
    }

    fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        let path = self.prepend_subdir(path);

        self.inner.presign(&path, args)
    }

    async fn create_multipart(
        &self,
        path: &str,
        args: OpCreateMultipart,
    ) -> Result<RpCreateMultipart> {
        let path = self.prepend_subdir(path);

        self.inner.create_multipart(&path, args).await
    }

    async fn write_multipart(
        &self,
        path: &str,
        args: OpWriteMultipart,
        r: BytesReader,
    ) -> Result<RpWriteMultipart> {
        let path = self.prepend_subdir(path);

        self.inner.write_multipart(&path, args, r).await
    }

    async fn complete_multipart(
        &self,
        path: &str,
        args: OpCompleteMultipart,
    ) -> Result<RpCompleteMultipart> {
        let path = self.prepend_subdir(path);

        self.inner.complete_multipart(&path, args).await
    }

    async fn abort_multipart(
        &self,
        path: &str,
        args: OpAbortMultipart,
    ) -> Result<RpAbortMultipart> {
        let path = self.prepend_subdir(path);

        self.inner.abort_multipart(&path, args).await
    }

    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<()> {
        let path = self.prepend_subdir(path);

        self.inner.blocking_create(&path, args)
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<BlockingBytesReader> {
        let path = self.prepend_subdir(path);

        self.inner.blocking_read(&path, args)
    }

    fn blocking_write(&self, path: &str, args: OpWrite, r: BlockingBytesReader) -> Result<u64> {
        let path = self.prepend_subdir(path);

        self.inner.blocking_write(&path, args, r)
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<ObjectMetadata> {
        let path = self.prepend_subdir(path);

        self.inner.blocking_stat(&path, args)
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        let path = self.prepend_subdir(path);

        self.inner.blocking_delete(&path, args)
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<ObjectIterator> {
        let path = self.prepend_subdir(path);

        Ok(Box::new(SubdirIterator::new(
            Arc::new(self.clone()),
            &self.subdir,
            self.inner.blocking_list(&path, args)?,
        )))
    }
}

fn strip_subdir(subdir: &str, path: &str) -> String {
    path.strip_prefix(subdir)
        .expect("strip subdir must succeed")
        .to_string()
}

struct SubdirStreamer {
    acc: Arc<dyn Accessor>,
    subdir: String,
    inner: ObjectStreamer,
}

impl SubdirStreamer {
    fn new(acc: Arc<dyn Accessor>, subdir: &str, inner: ObjectStreamer) -> Self {
        Self {
            acc,
            subdir: subdir.to_string(),
            inner,
        }
    }
}

impl Stream for SubdirStreamer {
    type Item = Result<ObjectEntry>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut (*self.inner)).poll_next(cx) {
            Poll::Ready(Some(Ok(mut de))) => {
                de.set_accessor(self.acc.clone());
                de.set_path(&strip_subdir(&self.subdir, de.path()));
                Poll::Ready(Some(Ok(de)))
            }
            v => v,
        }
    }
}

struct SubdirIterator {
    acc: Arc<dyn Accessor>,
    subdir: String,
    inner: ObjectIterator,
}

impl SubdirIterator {
    fn new(acc: Arc<dyn Accessor>, subdir: &str, inner: ObjectIterator) -> Self {
        Self {
            acc,
            subdir: subdir.to_string(),
            inner,
        }
    }
}

impl Iterator for SubdirIterator {
    type Item = Result<ObjectEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(Ok(mut de)) => {
                de.set_accessor(self.acc.clone());
                de.set_path(&strip_subdir(&self.subdir, de.path()));
                Some(Ok(de))
            }
            v => v,
        }
    }
}
