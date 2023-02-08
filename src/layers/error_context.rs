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
use std::fmt::Formatter;

use async_trait::async_trait;
use futures::TryFutureExt;

use crate::ops::*;
use crate::raw::*;
use crate::*;

/// ErrorContextLayer will add error context into all layers.
pub struct ErrorContextLayer;

impl<A: Accessor> Layer<A> for ErrorContextLayer {
    type LayeredAccessor = ErrorContextAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        let meta = inner.metadata();
        ErrorContextAccessor { meta, inner }
    }
}

/// Provide error context wrapper for backend.
pub struct ErrorContextAccessor<A: Accessor> {
    meta: AccessorMetadata,
    inner: A,
}

impl<A: Accessor> Debug for ErrorContextAccessor<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

#[async_trait]
impl<A: Accessor> LayeredAccessor for ErrorContextAccessor<A> {
    type Inner = A;
    type Reader = A::Reader;
    type BlockingReader = A::BlockingReader;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    fn metadata(&self) -> AccessorMetadata {
        self.meta.clone()
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        self.inner
            .create(path, args)
            .map_err(|err| {
                err.with_operation(Operation::Create.into_static())
                    .with_context("service", self.meta.scheme())
                    .with_context("path", path)
            })
            .await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let br = args.range();

        self.inner
            .read(path, args)
            .map_err(|err| {
                err.with_operation(Operation::Read.into_static())
                    .with_context("service", self.meta.scheme())
                    .with_context("path", path)
                    .with_context("range", br.to_string())
            })
            .await
    }

    async fn write(&self, path: &str, args: OpWrite, r: input::Reader) -> Result<RpWrite> {
        self.inner
            .write(path, args, r)
            .map_err(|err| {
                err.with_operation(Operation::Write.into_static())
                    .with_context("service", self.meta.scheme())
                    .with_context("path", path)
            })
            .await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner
            .stat(path, args)
            .map_err(|err| {
                err.with_operation(Operation::Stat.into_static())
                    .with_context("service", self.meta.scheme())
                    .with_context("path", path)
            })
            .await
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.inner
            .delete(path, args)
            .map_err(|err| {
                err.with_operation(Operation::Delete.into_static())
                    .with_context("service", self.meta.scheme())
                    .with_context("path", path)
            })
            .await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, ObjectPager)> {
        self.inner
            .list(path, args)
            .map_ok(|(rp, os)| {
                (
                    rp,
                    Box::new(ObjectStreamErrorContextWrapper {
                        scheme: self.meta.scheme(),
                        path: path.to_string(),
                        // TODO
                        //
                        // after `box_into_inner` has been stablized, we can use
                        // `Box::into_inner` to avoid extra boxing.
                        //
                        // ref: https://github.com/rust-lang/rust/issues/80437
                        inner: os,
                    }) as ObjectPager,
                )
            })
            .map_err(|err| {
                err.with_operation(Operation::List.into_static())
                    .with_context("service", self.meta.scheme())
                    .with_context("path", path)
            })
            .await
    }

    fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.inner.presign(path, args).map_err(|err| {
            err.with_operation(Operation::Presign.into_static())
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        })
    }

    async fn create_multipart(
        &self,
        path: &str,
        args: OpCreateMultipart,
    ) -> Result<RpCreateMultipart> {
        self.inner
            .create_multipart(path, args)
            .map_err(|err| {
                err.with_operation(Operation::CreateMultipart.into_static())
                    .with_context("service", self.meta.scheme())
                    .with_context("path", path)
            })
            .await
    }

    async fn write_multipart(
        &self,
        path: &str,
        args: OpWriteMultipart,
        r: input::Reader,
    ) -> Result<RpWriteMultipart> {
        self.inner
            .write_multipart(path, args, r)
            .map_err(|err| {
                err.with_operation(Operation::WriteMultipart.into_static())
                    .with_context("service", self.meta.scheme())
                    .with_context("path", path)
            })
            .await
    }

    async fn complete_multipart(
        &self,
        path: &str,
        args: OpCompleteMultipart,
    ) -> Result<RpCompleteMultipart> {
        self.inner
            .complete_multipart(path, args)
            .map_err(|err| {
                err.with_operation(Operation::CompleteMultipart.into_static())
                    .with_context("service", self.meta.scheme())
                    .with_context("path", path)
            })
            .await
    }

    async fn abort_multipart(
        &self,
        path: &str,
        args: OpAbortMultipart,
    ) -> Result<RpAbortMultipart> {
        self.inner
            .abort_multipart(path, args)
            .map_err(|err| {
                err.with_operation(Operation::AbortMultipart.into_static())
                    .with_context("service", self.meta.scheme())
                    .with_context("path", path)
            })
            .await
    }

    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        self.inner.blocking_create(path, args).map_err(|err| {
            err.with_operation(Operation::BlockingCreate.into_static())
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        })
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.inner.blocking_read(path, args).map_err(|err| {
            err.with_operation(Operation::BlockingRead.into_static())
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        })
    }

    fn blocking_write(
        &self,
        path: &str,
        args: OpWrite,
        r: input::BlockingReader,
    ) -> Result<RpWrite> {
        self.inner.blocking_write(path, args, r).map_err(|err| {
            err.with_operation(Operation::BlockingWrite.into_static())
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        })
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.blocking_stat(path, args).map_err(|err| {
            err.with_operation(Operation::BlockingStat.into_static())
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        })
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.inner.blocking_delete(path, args).map_err(|err| {
            err.with_operation(Operation::BlockingDelete.into_static())
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        })
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, BlockingObjectPager)> {
        self.inner.blocking_list(path, args).map_err(|err| {
            err.with_operation(Operation::BlockingList.into_static())
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        })
    }
}

struct ObjectStreamErrorContextWrapper<T: ObjectPage> {
    scheme: Scheme,
    path: String,
    inner: T,
}

#[async_trait::async_trait]
impl<T: ObjectPage> ObjectPage for ObjectStreamErrorContextWrapper<T> {
    async fn next_page(&mut self) -> Result<Option<Vec<ObjectEntry>>> {
        self.inner.next_page().await.map_err(|err| {
            err.with_operation("ObjectPage::next_page")
                .with_context("service", self.scheme)
                .with_context("path", &self.path)
        })
    }
}
