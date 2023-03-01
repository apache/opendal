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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::io;
use std::io::SeekFrom;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use bytes::Bytes;
use futures::TryFutureExt;

use crate::ops::*;
use crate::raw::output::PageOperation;
use crate::raw::*;
use crate::*;

/// ErrorContextLayer will add error context into all layers.
///
/// # Notes
///
/// This layer will adding the following error context into all errors:
///
/// - `service`: The [`Scheme`] of underlying service.
/// - `operation`: The [`Operation`] of this operation
/// - `path`: The path of this operation
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
    type Writer = A::Writer;
    type BlockingWriter = A::BlockingWriter;
    type Pager = ErrorContextWrapper<A::Pager>;
    type BlockingPager = ErrorContextWrapper<A::BlockingPager>;

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
                err.with_operation(Operation::Create)
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
                err.with_operation(Operation::Read)
                    .with_context("service", self.meta.scheme())
                    .with_context("path", path)
                    .with_context("range", br.to_string())
            })
            .await
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.inner
            .write(path, args)
            .map_err(|err| {
                err.with_operation(Operation::Write)
                    .with_context("service", self.meta.scheme())
                    .with_context("path", path)
            })
            .await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner
            .stat(path, args)
            .map_err(|err| {
                err.with_operation(Operation::Stat)
                    .with_context("service", self.meta.scheme())
                    .with_context("path", path)
            })
            .await
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.inner
            .delete(path, args)
            .map_err(|err| {
                err.with_operation(Operation::Delete)
                    .with_context("service", self.meta.scheme())
                    .with_context("path", path)
            })
            .await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        self.inner
            .list(path, args)
            .map_ok(|(rp, os)| {
                (
                    rp,
                    ErrorContextWrapper {
                        scheme: self.meta.scheme(),
                        path: path.to_string(),
                        inner: os,
                    },
                )
            })
            .map_err(|err| {
                err.with_operation(Operation::List)
                    .with_context("service", self.meta.scheme())
                    .with_context("path", path)
            })
            .await
    }

    async fn scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::Pager)> {
        self.inner
            .scan(path, args)
            .map_ok(|(rp, os)| {
                (
                    rp,
                    ErrorContextWrapper {
                        scheme: self.meta.scheme(),
                        path: path.to_string(),
                        inner: os,
                    },
                )
            })
            .map_err(|err| {
                err.with_operation(Operation::Scan)
                    .with_context("service", self.meta.scheme())
                    .with_context("path", path)
            })
            .await
    }

    fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.inner.presign(path, args).map_err(|err| {
            err.with_operation(Operation::Presign)
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        })
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        self.inner
            .batch(args)
            .map_ok(|v| {
                let BatchedResults::Delete(res) = v.into_results();

                let res = res
                    .into_iter()
                    .map(|(path, res)| {
                        let res = res.map_err(|err| {
                            err.with_operation(Operation::Delete)
                                .with_context("service", self.meta.scheme())
                                .with_context("path", &path)
                        });
                        (path, res)
                    })
                    .collect();

                RpBatch::new(BatchedResults::Delete(res))
            })
            .map_err(|err| {
                err.with_operation(Operation::Batch)
                    .with_context("service", self.meta.scheme())
            })
            .await
    }

    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        self.inner.blocking_create(path, args).map_err(|err| {
            err.with_operation(Operation::BlockingCreate)
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        })
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.inner.blocking_read(path, args).map_err(|err| {
            err.with_operation(Operation::BlockingRead)
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        })
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.inner.blocking_write(path, args).map_err(|err| {
            err.with_operation(Operation::BlockingWrite)
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        })
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.blocking_stat(path, args).map_err(|err| {
            err.with_operation(Operation::BlockingStat)
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        })
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.inner.blocking_delete(path, args).map_err(|err| {
            err.with_operation(Operation::BlockingDelete)
                .with_context("service", self.meta.scheme())
                .with_context("path", path)
        })
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
        self.inner
            .blocking_list(path, args)
            .map(|(rp, os)| {
                (
                    rp,
                    ErrorContextWrapper {
                        scheme: self.meta.scheme(),
                        path: path.to_string(),
                        inner: os,
                    },
                )
            })
            .map_err(|err| {
                err.with_operation(Operation::BlockingList)
                    .with_context("service", self.meta.scheme())
                    .with_context("path", path)
            })
    }

    fn blocking_scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::BlockingPager)> {
        self.inner
            .blocking_scan(path, args)
            .map(|(rp, os)| {
                (
                    rp,
                    ErrorContextWrapper {
                        scheme: self.meta.scheme(),
                        path: path.to_string(),
                        inner: os,
                    },
                )
            })
            .map_err(|err| {
                err.with_operation(Operation::BlockingScan)
                    .with_context("service", self.meta.scheme())
                    .with_context("path", path)
            })
    }
}

pub struct ErrorContextWrapper<T> {
    scheme: Scheme,
    path: String,
    inner: T,
}

impl<T: output::Read> output::Read for ErrorContextWrapper<T> {
    fn poll_read(&mut self, _cx: &mut Context<'_>, _buf: &mut [u8]) -> Poll<io::Result<usize>> {
        todo!()
    }

    fn poll_seek(&mut self, _cx: &mut Context<'_>, _pos: SeekFrom) -> Poll<io::Result<u64>> {
        todo!()
    }

    fn poll_next(&mut self, _cx: &mut Context<'_>) -> Poll<Option<io::Result<Bytes>>> {
        todo!()
    }
}

#[async_trait::async_trait]
impl<T: output::Page> output::Page for ErrorContextWrapper<T> {
    async fn next(&mut self) -> Result<Option<Vec<output::Entry>>> {
        self.inner.next().await.map_err(|err| {
            err.with_operation(PageOperation::Next)
                .with_context("service", self.scheme)
                .with_context("path", &self.path)
        })
    }
}

impl<T: output::BlockingPage> output::BlockingPage for ErrorContextWrapper<T> {
    fn next(&mut self) -> Result<Option<Vec<output::Entry>>> {
        self.inner.next().map_err(|err| {
            err.with_operation(PageOperation::BlockingNext)
                .with_context("service", self.scheme)
                .with_context("path", &self.path)
        })
    }
}
