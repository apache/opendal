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

use crate::object::BlockingObjectPager;
use crate::object::ObjectPage;
use crate::object::ObjectPager;
use crate::raw::*;
use crate::*;

/// Provide a zero cost error context wrapper for backend.
#[derive(Clone)]
pub struct ErrorContextWrapper<T: Accessor + 'static> {
    meta: AccessorMetadata,
    inner: T,
}

impl<T: Accessor + 'static> Debug for ErrorContextWrapper<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T: Accessor + 'static> ErrorContextWrapper<T> {
    /// Create a new error context wrapper
    pub fn new(inner: T) -> Self {
        let meta = inner.metadata();
        Self { meta, inner }
    }
}

#[async_trait]
impl<T: Accessor + 'static> Accessor for ErrorContextWrapper<T> {
    fn metadata(&self) -> AccessorMetadata {
        self.inner.metadata()
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        self.inner.create(path, args).await.map_err(|err| {
            err.with_operation(Operation::Create.into_static())
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        })
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, BytesReader)> {
        let br = args.range();
        self.inner.read(path, args).await.map_err(|err| {
            err.with_operation(Operation::Read.into_static())
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
                .with_context("range", br.to_string())
        })
    }

    async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<RpWrite> {
        self.inner.write(path, args, r).await.map_err(|err| {
            err.with_operation(Operation::Write.into_static())
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        })
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.stat(path, args).await.map_err(|err| {
            err.with_operation(Operation::Stat.into_static())
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        })
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.inner.delete(path, args).await.map_err(|err| {
            err.with_operation(Operation::Delete.into_static())
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        })
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, ObjectPager)> {
        self.inner
            .list(path, args)
            .await
            .map(|(rp, os)| {
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
            .await
            .map_err(|err| {
                err.with_operation(Operation::CreateMultipart.into_static())
                    .with_context("service", self.meta.scheme())
                    .with_context("path", path)
            })
    }

    async fn write_multipart(
        &self,
        path: &str,
        args: OpWriteMultipart,
        r: BytesReader,
    ) -> Result<RpWriteMultipart> {
        self.inner
            .write_multipart(path, args, r)
            .await
            .map_err(|err| {
                err.with_operation(Operation::WriteMultipart.into_static())
                    .with_context("service", self.meta.scheme())
                    .with_context("path", path)
            })
    }

    async fn complete_multipart(
        &self,
        path: &str,
        args: OpCompleteMultipart,
    ) -> Result<RpCompleteMultipart> {
        self.inner
            .complete_multipart(path, args)
            .await
            .map_err(|err| {
                err.with_operation(Operation::CompleteMultipart.into_static())
                    .with_context("service", self.meta.scheme())
                    .with_context("path", path)
            })
    }

    async fn abort_multipart(
        &self,
        path: &str,
        args: OpAbortMultipart,
    ) -> Result<RpAbortMultipart> {
        self.inner.abort_multipart(path, args).await.map_err(|err| {
            err.with_operation(Operation::AbortMultipart.into_static())
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        })
    }

    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        self.inner.blocking_create(path, args).map_err(|err| {
            err.with_operation(Operation::BlockingCreate.into_static())
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        })
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, BlockingBytesReader)> {
        self.inner.blocking_read(path, args).map_err(|err| {
            err.with_operation(Operation::BlockingRead.into_static())
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        })
    }

    fn blocking_write(&self, path: &str, args: OpWrite, r: BlockingBytesReader) -> Result<RpWrite> {
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

#[async_trait]
impl<T: ObjectPage> ObjectPage for ObjectStreamErrorContextWrapper<T> {
    async fn next_page(&mut self) -> Result<Option<Vec<ObjectEntry>>> {
        self.inner.next_page().await.map_err(|err| {
            err.with_operation("ObjectPage::next_page")
                .with_context("service", self.scheme)
                .with_context("path", &self.path)
        })
    }
}
