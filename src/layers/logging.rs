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
use std::io;
use std::io::Read;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use futures::AsyncRead;
use futures::Stream;
use log::debug;
use log::error;
use log::trace;
use log::warn;

use crate::ops::*;
use crate::*;

/// LoggingLayer will add logging for OpenDAL.
///
/// # Logging
///
/// - OpenDAL will log in structural way.
/// - Every operation will start with a `started` log entry.
/// - Every operation will finish with the following status:
///   - `finished`: the operation is successful.
///   - `errored`: the operation returns an expected error like `NotFound`.
///   - `failed`: the operation returns an unexpected error.
///
/// # Todo
///
/// We should migrate to log's kv api after it's ready.
///
/// Tracking issue: <https://github.com/rust-lang/log/issues/328>
///
/// # Examples
///
/// ```
/// use anyhow::Result;
/// use opendal::layers::LoggingLayer;
/// use opendal::Operator;
/// use opendal::Scheme;
///
/// let _ = Operator::from_env(Scheme::Fs)
///     .expect("must init")
///     .layer(LoggingLayer);
/// ```
#[derive(Debug, Copy, Clone)]
pub struct LoggingLayer;

impl Layer for LoggingLayer {
    fn layer(&self, inner: Arc<dyn Accessor>) -> Arc<dyn Accessor> {
        let meta = inner.metadata();
        Arc::new(LoggingAccessor {
            scheme: meta.scheme(),
            inner,
        })
    }
}

#[derive(Clone, Debug)]
struct LoggingAccessor {
    scheme: Scheme,
    inner: Arc<dyn Accessor>,
}

#[async_trait]
impl Accessor for LoggingAccessor {
    fn inner(&self) -> Option<Arc<dyn Accessor>> {
        Some(self.inner.clone())
    }

    fn metadata(&self) -> AccessorMetadata {
        debug!(
            target: "opendal::services",
            "service={} operation={} -> started",
            self.scheme, Operation::Metadata
        );
        let result = self.inner.metadata();
        debug!(
            target: "opendal::services",
            "service={} operation={} -> finished: {:?}",
            self.scheme, Operation::Metadata, result
        );

        result
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} -> started",
            self.scheme, Operation::Create, path
        );

        self.inner
            .create(path, args.clone())
            .await
            .map(|v| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} -> finished",
                    self.scheme, Operation::Create, path
                );
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Unexpected {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> failed: {err:?}",
                        self.scheme, Operation::Create, path
                    );
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> errored: {err:?}",
                        self.scheme, Operation::Create, path
                    );
                };
                err
            })
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, BytesReader)> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} range={} -> started",
            self.scheme, Operation::Read, path, args.range()
        );

        self.inner
            .read(path, args.clone())
            .await
            .map(|(rp, r)| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} range={} -> got reader",
                    self.scheme, Operation::Read, path,
                    args.range()
                );
                (
                    rp,
                    Box::new(LoggingReader::new(
                        self.scheme,
                        Operation::Read,
                        path,
                        args.range().size(),
                        r,
                    )) as BytesReader,
                )
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Unexpected {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} range={} -> failed: {err:?}",
                        self.scheme, Operation::Read, path, args.range());
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} range={} -> errored: {err:?}",
                        self.scheme, Operation::Read, path, args.range());
                };
                err
            })
    }

    async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<RpWrite> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} size={:?} -> started",
            self.scheme, Operation::Write, path, args.size()
        );

        let reader = LoggingReader::new(self.scheme, Operation::Write, path, Some(args.size()), r);
        let r = Box::new(reader) as BytesReader;

        self.inner
            .write(path, args.clone(), r)
            .await
            .map(|v| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} size={:?} -> written",
                    self.scheme, Operation::Write, path, args.size()
                );
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Unexpected {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} size={:?} -> failed: {err:?}",
                        self.scheme, Operation::Write, path, args.size()
                    );
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} size={:?} -> errored: {err:?}",
                        self.scheme, Operation::Write, path, args.size()
                    );
                };
                err
            })
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} -> started",
            self.scheme, Operation::Stat, path
        );

        self.inner
            .stat(path, args.clone())
            .await
            .map(|v| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} -> finished: {v:?}",
                    self.scheme, Operation::Stat, path
                );
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Unexpected {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> failed: {err:?}",
                        self.scheme, Operation::Stat, path
                    );
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> errored: {err:?}",
                        self.scheme, Operation::Stat, path
                    );
                };
                err
            })
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} -> started",
            self.scheme, Operation::Delete, path
        );

        self.inner
            .delete(path, args.clone())
            .await
            .map(|v| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} -> finished",
                    self.scheme, Operation::Delete, path);
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Unexpected {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> failed: {err:?}",
                        self.scheme, Operation::Delete, path);
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> errored: {err:?}",
                        self.scheme, Operation::Delete, path);
                };
                err
            })
    }

    async fn list(&self, path: &str, args: OpList) -> Result<ObjectStreamer> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} -> started",
            self.scheme, Operation::List, path
        );

        self.inner
            .list(path, args)
            .await
            .map(|v| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} -> got dir streamer",
                    self.scheme, Operation::List, path
                );
                let streamer = LoggingStreamer::new(Arc::new(self.clone()), self.scheme, path, v);
                Box::new(streamer) as ObjectStreamer
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Unexpected {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> failed: {err:?}",
                        self.scheme, Operation::List, path
                    );
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> errored: {err:?}",
                        self.scheme, Operation::List, path
                    );
                };
                err
            })
    }

    fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} -> started",
            self.scheme, Operation::Presign, path
        );

        self.inner
            .presign(path, args)
            .map(|v| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} -> finished: {v:?}",
                    self.scheme, Operation::Presign, path
                );
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Unexpected {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> failed: {err:?}",
                        self.scheme, Operation::Presign, path
                    );
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> errored: {err:?}",
                        self.scheme, Operation::Presign, path
                    );
                };
                err
            })
    }

    async fn create_multipart(
        &self,
        path: &str,
        args: OpCreateMultipart,
    ) -> Result<RpCreateMultipart> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} -> started",
            self.scheme, Operation::CreateMultipart, path
        );

        self.inner
            .create_multipart(path, args.clone())
            .await
            .map(|v| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} -> finished",
                    self.scheme, Operation::CreateMultipart, path);
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Unexpected {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> failed: {err:?}",
                        self.scheme, Operation::CreateMultipart, path);
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> errored: {err:?}",
                        self.scheme, Operation::CreateMultipart, path);
                };
                err
            })
    }

    async fn write_multipart(
        &self,
        path: &str,
        args: OpWriteMultipart,
        r: BytesReader,
    ) -> Result<RpWriteMultipart> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} upload_id={} part_number={:?} size={:?} -> started",
            self.scheme,
            Operation::WriteMultipart,
            path,
            args.upload_id(),
            args.part_number(),
            args.size()
        );

        let r = LoggingReader::new(self.scheme, Operation::Write, path, Some(args.size()), r);
        let r = Box::new(r);

        self.inner
            .write_multipart(path, args.clone(), r)
            .await
            .map(|v| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} upload_id={} part_number={:?} size={:?} -> written",
                    self.scheme,
                    Operation::WriteMultipart,
                    path,
                    args.upload_id(),
                    args.part_number(),
                    args.size()
                );
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Unexpected {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} upload_id={} part_number={:?} size={:?} -> failed: {err:?}",
                        self.scheme,
                        Operation::WriteMultipart,
                        path,
                        args.upload_id(),
                        args.part_number(),
                        args.size()
                    );
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} upload_id={} part_number={:?} size={:?} -> errored: {err:?}",
                        self.scheme,
                        Operation::WriteMultipart,
                        path,
                        args.upload_id(),
                        args.part_number(),
                        args.size()
                    );
                };
                err
            })
    }

    async fn complete_multipart(
        &self,
        path: &str,
        args: OpCompleteMultipart,
    ) -> Result<RpCompleteMultipart> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} upload_id={} -> started",
            self.scheme,
            Operation::CompleteMultipart,
            path,
            args.upload_id(),
        );

        self.inner
            .complete_multipart(path, args.clone())
            .await
            .map(|v| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} upload_id={} -> finished",
                    self.scheme, Operation::CompleteMultipart, path, args.upload_id());
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Unexpected {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} upload_id={} -> failed: {err:?}",
                        self.scheme, Operation::CompleteMultipart, path, args.upload_id());
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} upload_id={} -> errored: {err:?}",
                        self.scheme, Operation::CompleteMultipart, path, args.upload_id());
                };
                err
            })
    }

    async fn abort_multipart(
        &self,
        path: &str,
        args: OpAbortMultipart,
    ) -> Result<RpAbortMultipart> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} upload_id={} -> started",
            self.scheme,
            Operation::AbortMultipart,
            path,
            args.upload_id()
        );

        self.inner
            .abort_multipart(path, args.clone())
            .await
            .map(|v| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} upload_id={} -> finished",self.scheme, Operation::AbortMultipart, path, args.upload_id());
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Unexpected {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} upload_id={} -> failed: {err:?}",self.scheme, Operation::AbortMultipart, path, args.upload_id());
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} upload_id={} -> errored: {err:?}",self.scheme, Operation::AbortMultipart, path, args.upload_id());
                };
                err
            })
    }

    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} -> started",
            self.scheme,
            Operation::BlockingCreate,
            path
        );

        self.inner
            .blocking_create(path, args)
            .map(|v| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} -> finished",
                    self.scheme,
                    Operation::BlockingCreate,
                    path
                );
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Unexpected {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> failed: {err:?}",
                        self.scheme,
                        Operation::BlockingCreate,
                        path
                    );
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> errored: {err:?}",
                        self.scheme,
                        Operation::BlockingCreate,
                        path
                    );
                };
                err
            })
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, BlockingBytesReader)> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} range={} -> started",
            self.scheme,
            Operation::BlockingRead,
            path,
            args.range(),
        );

        self.inner
            .blocking_read(path, args.clone())
            .map(|(rp, r)| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} range={} -> got reader",
                    self.scheme,
                    Operation::BlockingRead,
                    path,
                    args.range(),
                );
                let r = BlockingLoggingReader::new(
                    self.scheme,
                    Operation::BlockingRead,
                    path,
                    args.range().size(),
                    r,
                );
                (rp, Box::new(r) as BlockingBytesReader)
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Unexpected {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} range={} -> failed: {err:?}",
                        self.scheme, Operation::BlockingRead, path, args.range());
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} range={} -> errored: {err:?}",
                        self.scheme, Operation::BlockingRead, path, args.range());
                };
                err
            })
    }

    fn blocking_write(&self, path: &str, args: OpWrite, r: BlockingBytesReader) -> Result<RpWrite> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} size={:?} -> started",
            self.scheme,
            Operation::BlockingWrite,
            path,
            args.size()
        );

        let reader = BlockingLoggingReader::new(
            self.scheme,
            Operation::BlockingWrite,
            path,
            Some(args.size()),
            r,
        );
        let r = Box::new(reader) as BlockingBytesReader;

        self.inner
            .blocking_write(path, args.clone(), r)
            .map(|v| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} size={:?} -> written",
                    self.scheme,
                    Operation::BlockingWrite,
                    path,
                    args.size()
                );
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Unexpected {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} size={:?} -> failed: {err:?}",
                        self.scheme,
                        Operation::BlockingWrite,
                        path,
                        args.size()
                    );
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} size={:?} -> errored: {err:?}",
                        self.scheme,
                        Operation::BlockingWrite,
                        path,
                        args.size()
                    );
                };
                err
            })
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<ObjectMetadata> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} -> started",
            self.scheme,
            Operation::BlockingStat,
            path
        );

        self.inner
            .blocking_stat(path, args)
            .map(|v| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} -> finished: {v:?}",
                    self.scheme,
                    Operation::BlockingStat,
                    path
                );
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Unexpected {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> failed: {err:?}",
                        self.scheme,
                        Operation::BlockingStat,
                        path
                    );
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> errored: {err:?}",
                        self.scheme,
                        Operation::BlockingStat,
                        path
                    );
                };
                err
            })
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} -> started",
            self.scheme,
            Operation::BlockingDelete,
            path
        );

        self.inner
            .blocking_delete(path, args)
            .map(|v| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} -> finished",
                    self.scheme, Operation::BlockingDelete, path);
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Unexpected {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> failed: {err:?}",
                        self.scheme, Operation::BlockingDelete, path);
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> errored: {err:?}",
                        self.scheme, Operation::BlockingDelete, path);
                };
                err
            })
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<ObjectIterator> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} -> started",
            self.scheme,
            Operation::BlockingList,
            path
        );

        self.inner
            .blocking_list(path, args)
            .map(|v| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} -> got dir streamer",
                    self.scheme,
                    Operation::BlockingList,
                    path
                );
                let li = LoggingIterator::new(Arc::new(self.clone()), self.scheme, path, v);
                Box::new(li) as ObjectIterator
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Unexpected {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> failed: {err:?}",
                        self.scheme,
                        Operation::BlockingList,
                        path
                    );
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> errored: {err:?}",
                        self.scheme,
                        Operation::BlockingList,
                        path
                    );
                };
                err
            })
    }
}

/// `LoggingReader` is a wrapper of `BytesReader`, with logging functionality.
struct LoggingReader {
    scheme: Scheme,
    path: String,
    op: Operation,

    size: Option<u64>,
    has_read: u64,

    inner: BytesReader,
}

impl LoggingReader {
    fn new(
        scheme: Scheme,
        op: Operation,
        path: &str,
        size: Option<u64>,
        reader: BytesReader,
    ) -> Self {
        Self {
            scheme,
            op,
            path: path.to_string(),

            size,
            has_read: 0,

            inner: reader,
        }
    }
}

impl Drop for LoggingReader {
    fn drop(&mut self) {
        if let Some(size) = self.size {
            if size == self.has_read {
                debug!(
                target: "opendal::services",
                "service={} operation={} path={} has_read={} -> consumed reader fully",
                self.scheme, self.op, self.path, self.has_read);

                return;
            }
        }

        debug!(
            target: "opendal::services",
            "service={} operation={} path={} has_read={} -> dropped reader",
            self.scheme, self.op, self.path, self.has_read);
    }
}

impl AsyncRead for LoggingReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        match Pin::new(&mut (*self.inner)).poll_read(cx, buf) {
            Poll::Ready(res) => match res {
                Ok(n) => {
                    self.has_read += n as u64;
                    trace!(
                        target: "opendal::services",
                        "service={} operation={} path={} has_read={} -> {}: {}B",
                        self.scheme, self.op, self.path, self.has_read, self.op, n);
                    Poll::Ready(Ok(n))
                }
                Err(e) => {
                    if e.kind() == io::ErrorKind::Other {
                        error!(
                            target: "opendal::services",
                            "service={} operation={} path={} has_read={} -> failed: {:?}",
                            self.scheme, self.op, self.path, self.has_read, e);
                    } else {
                        warn!(
                            target: "opendal::services",
                            "service={} operation={} path={} has_read={} -> errored: {:?}",
                            self.scheme, self.op, self.path,  self.has_read, e);
                    }
                    Poll::Ready(Err(e))
                }
            },
            Poll::Pending => {
                trace!(
                    target: "opendal::services",
                    "service={} operation={} path={} has_read={} -> pending",
                    self.scheme, self.op, self.path, self.has_read);
                Poll::Pending
            }
        }
    }
}

/// `BlockingLoggingReader` is a wrapper of `BlockingBytesReader`, with logging functionality.
struct BlockingLoggingReader {
    scheme: Scheme,
    path: String,
    op: Operation,

    size: Option<u64>,
    has_read: u64,

    inner: BlockingBytesReader,
}

impl BlockingLoggingReader {
    fn new(
        scheme: Scheme,
        op: Operation,
        path: &str,
        size: Option<u64>,
        reader: BlockingBytesReader,
    ) -> Self {
        Self {
            scheme,
            op,
            path: path.to_string(),

            size,
            has_read: 0,
            inner: reader,
        }
    }
}

impl Drop for BlockingLoggingReader {
    fn drop(&mut self) {
        if let Some(size) = self.size {
            if size == self.has_read {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} has_read={} -> consumed reader fully",
                    self.scheme, self.op, self.path, self.has_read);

                return;
            }
        }

        debug!(
            target: "opendal::services",
            "service={} operation={} path={} has_read={} -> dropped reader",
            self.scheme, self.op, self.path, self.has_read);
    }
}

impl Read for BlockingLoggingReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.inner.read(buf) {
            Ok(n) => {
                self.has_read += n as u64;
                trace!(
                    target: "opendal::services",
                    "service={} operation={} path={} has_read={} -> {}: {}B",
                    self.scheme, self.op, self.path, self.has_read, self.op, n);
                Ok(n)
            }
            Err(e) => {
                if e.kind() == io::ErrorKind::Other {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} has_read={} -> failed: {:?}",
                        self.scheme, self.op, self.path, self.has_read, e);
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} has_read={} -> errored: {:?}",
                        self.scheme, self.op, self.path,  self.has_read, e);
                }
                Err(e)
            }
        }
    }
}

struct LoggingStreamer {
    acc: Arc<LoggingAccessor>,
    scheme: Scheme,
    path: String,
    finished: bool,
    inner: ObjectStreamer,
}

impl LoggingStreamer {
    fn new(acc: Arc<LoggingAccessor>, scheme: Scheme, path: &str, inner: ObjectStreamer) -> Self {
        Self {
            acc,
            scheme,
            path: path.to_string(),
            finished: false,
            inner,
        }
    }
}

impl Drop for LoggingStreamer {
    fn drop(&mut self) {
        if self.finished {
            debug!(
                target: "opendal::services",
                "service={} operation={} path={} -> consumed streamer fully",
                self.scheme, Operation::List, self.path);
        } else {
            debug!(
                target: "opendal::services",
                "service={} operation={} path={} -> dropped streamer",
                self.scheme, Operation::List, self.path);
        }
    }
}

impl Stream for LoggingStreamer {
    type Item = Result<ObjectEntry>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut (*self.inner)).poll_next(cx) {
            Poll::Ready(opt) => match opt {
                Some(res) => match res {
                    Ok(mut de) => {
                        debug!(
                            target: "opendal::service",
                            "service={} operation={} path={} -> got entry: mode={} path={}",
                            self.scheme,
                            Operation::List,
                            self.path,
                            de.mode(),
                            de.path(),
                        );
                        de.set_accessor(self.acc.clone());
                        Poll::Ready(Some(Ok(de)))
                    }
                    Err(e) => {
                        if e.kind() == ErrorKind::Unexpected {
                            error!(
                                target: "opendal::service",
                                "service={} operation={} path={} -> failed: {:?}",
                                self.scheme,
                                Operation::List,
                                self.path,
                                e
                            );
                        } else {
                            warn!(
                                target: "opendal::service",
                                "service={} operation={} path={} -> errored: {:?}",
                                self.scheme,
                                Operation::List,
                                self.path,
                                e
                            );
                        }
                        Poll::Ready(Some(Err(e)))
                    }
                },
                None => {
                    debug!(
                        target: "opendal::service",
                        "service={} operation={} path={} -> finished",
                        self.scheme,
                        Operation::List,
                        self.path
                    );
                    self.finished = true;
                    Poll::Ready(None)
                }
            },
            Poll::Pending => {
                trace!(
                    target: "opendal::service",
                    "service={} operation={} path={} -> pending",
                    self.scheme,
                    Operation::List,
                    self.path
                );
                Poll::Pending
            }
        }
    }
}

struct LoggingIterator {
    acc: Arc<LoggingAccessor>,
    scheme: Scheme,
    path: String,
    finished: bool,
    inner: ObjectIterator,
}

impl LoggingIterator {
    fn new(acc: Arc<LoggingAccessor>, scheme: Scheme, path: &str, inner: ObjectIterator) -> Self {
        Self {
            acc,
            scheme,
            path: path.to_string(),
            finished: false,
            inner,
        }
    }
}

impl Drop for LoggingIterator {
    fn drop(&mut self) {
        if self.finished {
            debug!(
                target: "opendal::services",
                "service={} operation={} path={} -> consumed iterator fully",
                self.scheme, Operation::BlockingList, self.path);
        } else {
            debug!(
                target: "opendal::services",
                "service={} operation={} path={} -> dropped iterator",
                self.scheme, Operation::BlockingList, self.path);
        }
    }
}

impl Iterator for LoggingIterator {
    type Item = Result<ObjectEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(res) => match res {
                Ok(mut de) => {
                    debug!(
                        target: "opendal::service",
                        "service={} operation={} path={} -> got entry: mode={} path={}",
                        self.scheme,
                        Operation::BlockingList,
                        self.path,
                        de.mode(),
                        de.path(),
                    );
                    de.set_accessor(self.acc.clone());
                    Some(Ok(de))
                }
                Err(e) => {
                    if e.kind() == ErrorKind::Unexpected {
                        error!(
                            target: "opendal::service",
                            "service={} operation={} path={} -> failed: {:?}",
                            self.scheme,
                            Operation::BlockingList,
                            self.path,
                            e
                        );
                    } else {
                        warn!(
                            target: "opendal::service",
                            "service={} operation={} path={} -> errored: {:?}",
                            self.scheme,
                            Operation::BlockingList,
                            self.path,
                            e
                        );
                    }
                    Some(Err(e))
                }
            },
            None => {
                debug!(
                    target: "opendal::service",
                    "service={} operation={} path={} -> finished",
                    self.scheme,
                    Operation::BlockingList,
                    self.path
                );
                self.finished = true;
                None
            }
        }
    }
}
