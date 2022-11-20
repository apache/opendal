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

use async_trait::async_trait;
use futures::{ready, Stream, StreamExt};

use crate::ops::{OpDelete, OpList, OpRead, OpStat, OpWrite, Operation};
use crate::{ops::OpCreate, Accessor, AccessorMetadata};
use crate::{
    BytesReader, ObjectEntry, ObjectMetadata, ObjectReader, ObjectStreamer, Result, Scheme,
};
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Clone)]
pub struct BackendErrorContextWrapper<T: Accessor + 'static> {
    meta: AccessorMetadata,
    inner: T,
}

impl<T: Accessor + 'static> Debug for BackendErrorContextWrapper<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T: Accessor + 'static> BackendErrorContextWrapper<T> {
    pub fn new(inner: T) -> Self {
        let meta = inner.metadata();
        Self { meta, inner }
    }
}

#[async_trait]
impl<T: Accessor + 'static> Accessor for BackendErrorContextWrapper<T> {
    fn metadata(&self) -> AccessorMetadata {
        self.meta.clone()
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<()> {
        self.inner.create(path, args).await.map_err(|err| {
            err.with_operation(Operation::Create.into_static())
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        })
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<ObjectReader> {
        let br = args.range();
        self.inner.read(path, args).await.map_err(|err| {
            err.with_operation(Operation::Read.into_static())
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
                .with_context("range", br.to_string())
        })
    }

    async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<u64> {
        self.inner.write(path, args, r).await.map_err(|err| {
            err.with_operation(Operation::Write.into_static())
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        })
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<ObjectMetadata> {
        self.inner.stat(path, args).await.map_err(|err| {
            err.with_operation(Operation::Stat.into_static())
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        })
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<()> {
        self.inner.delete(path, args).await.map_err(|err| {
            err.with_operation(Operation::Delete.into_static())
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        })
    }

    async fn list(&self, path: &str, args: OpList) -> Result<ObjectStreamer> {
        self.inner
            .list(path, args)
            .await
            .map(|os| {
                Box::new(ObjectPageStreamErrorContextWrapper {
                    scheme: self.meta.scheme(),
                    path: path.to_string(),
                    // TODO
                    //
                    // after `box_into_inner` has been stablized, we can use
                    // `Box::into_inner` to avoid extra boxing.
                    //
                    // ref: https://github.com/rust-lang/rust/issues/80437
                    inner: os,
                }) as ObjectStreamer
            })
            .map_err(|err| {
                err.with_operation(Operation::List.into_static())
                    .with_context("service", self.meta.scheme())
                    .with_context("path", path)
            })
    }
}

struct ObjectPageStreamErrorContextWrapper<
    T: Stream<Item = Result<ObjectEntry>> + Unpin + Send + 'static,
> {
    scheme: Scheme,
    path: String,
    inner: T,
}

impl<T> Stream for ObjectPageStreamErrorContextWrapper<T>
where
    T: Stream<Item = Result<ObjectEntry>> + Unpin + Send + 'static,
{
    type Item = Result<ObjectEntry>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(ready!(self.inner.poll_next_unpin(cx)).map(|v| {
            v.map_err(|err| {
                err.with_operation(Operation::List.into_static())
                    .with_context("service", self.scheme)
                    .with_context("path", self.path)
            })
        }))
    }
}
