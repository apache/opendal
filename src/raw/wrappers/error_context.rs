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

use futures::TryFutureExt;

use crate::raw::*;
use crate::*;

/// Provide a zero cost error context wrapper for backend.
#[derive(Clone)]
pub struct ErrorContextWrapper<A: Accessor> {
    meta: AccessorMetadata,
    inner: A,
}

impl<A: Accessor> Debug for ErrorContextWrapper<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<A: Accessor> ErrorContextWrapper<A> {
    /// Create a new error context wrapper
    pub fn new(inner: A) -> Self {
        let meta = inner.metadata();
        Self { meta, inner }
    }
}

impl<A: Accessor> Accessor for ErrorContextWrapper<A> {
    type Reader = A::Reader;
    type BlockingReader = A::BlockingReader;

    fn metadata(&self) -> AccessorMetadata {
        self.meta.clone()
    }

    fn create(&self, path: &str, args: OpCreate) -> FutureResult<RpCreate> {
        let fut = self.inner.create(path, args).map_err(|err| {
            err.with_operation(Operation::Create.into_static())
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        });

        Box::pin(fut)
    }

    fn read(&self, path: &str, args: OpRead) -> FutureResult<(RpRead, Self::Reader)> {
        let br = args.range();

        let fut = self.inner.read(path, args).map_err(|err| {
            err.with_operation(Operation::Read.into_static())
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
                .with_context("range", br.to_string())
        });

        Box::pin(fut)
    }

    fn write(&self, path: &str, args: OpWrite, r: input::Reader) -> FutureResult<RpWrite> {
        let fut = self.inner.write(path, args, r).map_err(|err| {
            err.with_operation(Operation::Write.into_static())
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        });

        Box::pin(fut)
    }

    fn stat(&self, path: &str, args: OpStat) -> FutureResult<RpStat> {
        let fut = self.inner.stat(path, args).map_err(|err| {
            err.with_operation(Operation::Stat.into_static())
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        });

        Box::pin(fut)
    }

    fn delete(&self, path: &str, args: OpDelete) -> FutureResult<RpDelete> {
        let fut = self.inner.delete(path, args).map_err(|err| {
            err.with_operation(Operation::Delete.into_static())
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        });

        Box::pin(fut)
    }

    fn list(&self, path: &str, args: OpList) -> FutureResult<(RpList, ObjectPager)> {
        let fut = self
            .inner
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
            });

        Box::pin(fut)
    }

    fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.inner.presign(path, args).map_err(|err| {
            err.with_operation(Operation::Presign.into_static())
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        })
    }

    fn create_multipart(
        &self,
        path: &str,
        args: OpCreateMultipart,
    ) -> FutureResult<RpCreateMultipart> {
        let fut = self.inner.create_multipart(path, args).map_err(|err| {
            err.with_operation(Operation::CreateMultipart.into_static())
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        });

        Box::pin(fut)
    }

    fn write_multipart(
        &self,
        path: &str,
        args: OpWriteMultipart,
        r: input::Reader,
    ) -> FutureResult<RpWriteMultipart> {
        let fut = self.inner.write_multipart(path, args, r).map_err(|err| {
            err.with_operation(Operation::WriteMultipart.into_static())
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        });

        Box::pin(fut)
    }

    fn complete_multipart(
        &self,
        path: &str,
        args: OpCompleteMultipart,
    ) -> FutureResult<RpCompleteMultipart> {
        let fut = self.inner.complete_multipart(path, args).map_err(|err| {
            err.with_operation(Operation::CompleteMultipart.into_static())
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        });

        Box::pin(fut)
    }

    fn abort_multipart(
        &self,
        path: &str,
        args: OpAbortMultipart,
    ) -> FutureResult<RpAbortMultipart> {
        let fut = self.inner.abort_multipart(path, args).map_err(|err| {
            err.with_operation(Operation::AbortMultipart.into_static())
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        });

        Box::pin(fut)
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
