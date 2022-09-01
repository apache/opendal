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
use std::io::ErrorKind;
use std::io::Read;
use std::io::Result;
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

use crate::multipart::ObjectPart;
use crate::ops::OpAbortMultipart;
use crate::ops::OpCompleteMultipart;
use crate::ops::OpCreate;
use crate::ops::OpCreateMultipart;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpPresign;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::ops::OpWriteMultipart;
use crate::ops::Operation;
use crate::ops::PresignedRequest;
use crate::Accessor;
use crate::AccessorMetadata;
use crate::BlockingBytesReader;
use crate::BytesReader;
use crate::DirEntry;
use crate::DirIterator;
use crate::DirStreamer;
use crate::Layer;
use crate::ObjectMetadata;
use crate::Scheme;

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

#[derive(Debug)]
struct LoggingAccessor {
    scheme: Scheme,
    inner: Arc<dyn Accessor>,
}

#[async_trait]
impl Accessor for LoggingAccessor {
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

    async fn create(&self, args: &OpCreate) -> Result<()> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} -> started",
            self.scheme, Operation::Create, args.path()
        );

        self.inner
            .create(args)
            .await
            .map(|v| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} -> finished",
                    self.scheme, Operation::Create, args.path()
                );
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Other {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> failed: {err:?}",
                        self.scheme, Operation::Create, args.path()
                    );
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> errored: {err:?}",
                        self.scheme, Operation::Create, args.path()
                    );
                };
                err
            })
    }

    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} offset={:?} size={:?} -> started",
            self.scheme, Operation::Read, args.path(), args.offset(), args.size()
        );

        self.inner
            .read(args)
            .await
            .map(|v| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} offset={:?} size={:?} -> got reader",
                    self.scheme, Operation::Read, args.path(),
                    args.offset(), args.size()
                );
                let r = LoggingReader::new(self.scheme, Operation::Read, args.path(), v);
                Box::new(r) as BytesReader
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Other {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} offset={:?} size={:?} -> failed: {err:?}",
                        self.scheme, Operation::Read, args.path(),args.offset(),  args.size());
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} offset={:?} size={:?} -> errored: {err:?}",
                        self.scheme, Operation::Read, args.path(), args.offset(),  args.size());
                };
                err
            })
    }

    async fn write(&self, args: &OpWrite, r: BytesReader) -> Result<u64> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} size={:?} -> started",
            self.scheme, Operation::Write, args.path(), args.size()
        );

        let reader = LoggingReader::new(self.scheme, Operation::Write, args.path(), r);
        let r = Box::new(reader) as BytesReader;

        self.inner
            .write(args, r)
            .await
            .map(|v| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} size={:?} -> written",
                    self.scheme, Operation::Write, args.path(), args.size()
                );
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Other {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} size={:?} -> failed: {err:?}",
                        self.scheme, Operation::Write, args.path(), args.size()
                    );
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} size={:?} -> errored: {err:?}",
                        self.scheme, Operation::Write, args.path(), args.size()
                    );
                };
                err
            })
    }

    async fn stat(&self, args: &OpStat) -> Result<ObjectMetadata> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} -> started",
            self.scheme, Operation::Stat, args.path()
        );

        self.inner
            .stat(args)
            .await
            .map(|v| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} -> finished: {v:?}",
                    self.scheme, Operation::Stat, args.path()
                );
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Other {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> failed: {err:?}",
                        self.scheme, Operation::Stat, args.path()
                    );
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> errored: {err:?}",
                        self.scheme, Operation::Stat, args.path()
                    );
                };
                err
            })
    }

    async fn delete(&self, args: &OpDelete) -> Result<()> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} -> started",
            self.scheme, Operation::Delete, args.path()
        );

        self.inner
            .delete(args)
            .await
            .map(|v| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} -> finished",
                    self.scheme, Operation::Delete, args.path());
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Other {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> failed: {err:?}",
                        self.scheme, Operation::Delete, args.path());
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> errored: {err:?}",
                        self.scheme, Operation::Delete, args.path());
                };
                err
            })
    }

    async fn list(&self, args: &OpList) -> Result<DirStreamer> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} -> started",
            self.scheme, Operation::List, args.path()
        );

        self.inner
            .list(args)
            .await
            .map(|v| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} -> got dir streamer",
                    self.scheme, Operation::List, args.path()
                );
                let streamer = LoggingStreamer::new(self.scheme, args.path(), v);
                Box::new(streamer) as DirStreamer
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Other {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> failed: {err:?}",
                        self.scheme, Operation::List, args.path()
                    );
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> errored: {err:?}",
                        self.scheme, Operation::List, args.path()
                    );
                };
                err
            })
    }

    fn presign(&self, args: &OpPresign) -> Result<PresignedRequest> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} -> started",
            self.scheme, Operation::Presign, args.path()
        );

        self.inner
            .presign(args)
            .map(|v| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} -> finished: {v:?}",
                    self.scheme, Operation::Presign, args.path()
                );
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Other {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> failed: {err:?}",
                        self.scheme, Operation::Presign, args.path()
                    );
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> errored: {err:?}",
                        self.scheme, Operation::Presign, args.path()
                    );
                };
                err
            })
    }

    async fn create_multipart(&self, args: &OpCreateMultipart) -> Result<String> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} -> started",
            self.scheme, Operation::CreateMultipart, args.path()
        );

        self.inner
            .create_multipart(args)
            .await
            .map(|v| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} -> finished",
                    self.scheme, Operation::CreateMultipart, args.path());
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Other {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> failed: {err:?}",
                        self.scheme, Operation::CreateMultipart, args.path());
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> errored: {err:?}",
                        self.scheme, Operation::CreateMultipart, args.path());
                };
                err
            })
    }

    async fn write_multipart(&self, args: &OpWriteMultipart, r: BytesReader) -> Result<ObjectPart> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} upload_id={} part_number={:?} size={:?} -> started",
            self.scheme,
            Operation::WriteMultipart,
            args.path(),
            args.upload_id(),
            args.part_number(),
            args.size()
        );

        let r = LoggingReader::new(self.scheme, Operation::Write, args.path(), r);
        let r = Box::new(r);

        self.inner
            .write_multipart(args, r)
            .await
            .map(|v| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} upload_id={} part_number={:?} size={:?} -> written",
                    self.scheme,
                    Operation::WriteMultipart,
                    args.path(),
                    args.upload_id(),
                    args.part_number(),
                    args.size()
                );
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Other {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} upload_id={} part_number={:?} size={:?} -> failed: {err:?}",
                        self.scheme,
                        Operation::WriteMultipart,
                        args.path(),
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
                        args.path(),
                        args.upload_id(),
                        args.part_number(),
                        args.size()
                    );
                };
                err
            })
    }

    async fn complete_multipart(&self, args: &OpCompleteMultipart) -> Result<()> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} upload_id={} -> started",
            self.scheme,
            Operation::CompleteMultipart,
            args.path(),
            args.upload_id(),
        );

        self.inner
            .complete_multipart(args)
            .await
            .map(|v| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} upload_id={} -> finished",
                    self.scheme, Operation::CompleteMultipart, args.path(), args.upload_id());
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Other {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} upload_id={} -> failed: {err:?}",
                        self.scheme, Operation::CompleteMultipart, args.path(), args.upload_id());
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} upload_id={} -> errored: {err:?}",
                        self.scheme, Operation::CompleteMultipart, args.path(), args.upload_id());
                };
                err
            })
    }

    async fn abort_multipart(&self, args: &OpAbortMultipart) -> Result<()> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} upload_id={} -> started",
            self.scheme,
            Operation::AbortMultipart,
            args.path(),
            args.upload_id()
        );

        self.inner
            .abort_multipart(args)
            .await
            .map(|v| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} upload_id={} -> finished",self.scheme, Operation::AbortMultipart, args.path(), args.upload_id());
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Other {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} upload_id={} -> failed: {err:?}",self.scheme, Operation::AbortMultipart, args.path(), args.upload_id());
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} upload_id={} -> errored: {err:?}",self.scheme, Operation::AbortMultipart, args.path(), args.upload_id());
                };
                err
            })
    }

    fn blocking_create(&self, args: &OpCreate) -> Result<()> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} -> started",
            self.scheme,
            Operation::BlockingCreate,
            args.path()
        );

        self.inner
            .blocking_create(args)
            .map(|v| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} -> finished",
                    self.scheme,
                    Operation::BlockingCreate,
                    args.path()
                );
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Other {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> failed: {err:?}",
                        self.scheme,
                        Operation::BlockingCreate,
                        args.path()
                    );
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> errored: {err:?}",
                        self.scheme,
                        Operation::BlockingCreate,
                        args.path()
                    );
                };
                err
            })
    }

    fn blocking_read(&self, args: &OpRead) -> Result<BlockingBytesReader> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} offset={:?} size={:?} -> started",
            self.scheme,
            Operation::BlockingRead,
            args.path(),
            args.offset(),
            args.size()
        );

        self.inner
            .blocking_read(args)
            .map(|v| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} offset={:?} size={:?} -> got reader",
                    self.scheme,
                    Operation::BlockingRead,
                    args.path(),
                    args.offset(),
                    args.size()
                );
                let r = BlockingLoggingReader::new(self.scheme, Operation::BlockingRead, args.path(),v);
                Box::new(r) as BlockingBytesReader
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Other {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} offset={:?} size={:?} -> failed: {err:?}",
                        self.scheme, Operation::BlockingRead, args.path(), args.offset(), args.size());
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} offset={:?} size={:?} -> errored: {err:?}",
                        self.scheme, Operation::BlockingRead, args.path(), args.offset(), args.size());
                };
                err
            })
    }

    fn blocking_write(&self, args: &OpWrite, r: BlockingBytesReader) -> Result<u64> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} size={:?} -> started",
            self.scheme,
            Operation::BlockingWrite,
            args.path(),
            args.size()
        );

        let reader =
            BlockingLoggingReader::new(self.scheme, Operation::BlockingWrite, args.path(), r);
        let r = Box::new(reader) as BlockingBytesReader;

        self.inner
            .blocking_write(args, r)
            .map(|v| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} size={:?} -> written",
                    self.scheme,
                    Operation::BlockingWrite,
                    args.path(),
                    args.size()
                );
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Other {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} size={:?} -> failed: {err:?}",
                        self.scheme,
                        Operation::BlockingWrite,
                        args.path(),
                        args.size()
                    );
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} size={:?} -> errored: {err:?}",
                        self.scheme,
                        Operation::BlockingWrite,
                        args.path(),
                        args.size()
                    );
                };
                err
            })
    }

    fn blocking_stat(&self, args: &OpStat) -> Result<ObjectMetadata> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} -> started",
            self.scheme,
            Operation::BlockingStat,
            args.path()
        );

        self.inner
            .blocking_stat(args)
            .map(|v| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} -> finished: {v:?}",
                    self.scheme,
                    Operation::BlockingStat,
                    args.path()
                );
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Other {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> failed: {err:?}",
                        self.scheme,
                        Operation::BlockingStat,
                        args.path()
                    );
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> errored: {err:?}",
                        self.scheme,
                        Operation::BlockingStat,
                        args.path()
                    );
                };
                err
            })
    }

    fn blocking_delete(&self, args: &OpDelete) -> Result<()> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} -> started",
            self.scheme,
            Operation::BlockingDelete,
            args.path()
        );

        self.inner
            .blocking_delete(args)
            .map(|v| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} -> finished",
                    self.scheme, Operation::BlockingDelete, args.path());
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Other {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> failed: {err:?}",
                        self.scheme, Operation::BlockingDelete, args.path());
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> errored: {err:?}",
                        self.scheme, Operation::BlockingDelete, args.path());
                };
                err
            })
    }

    fn blocking_list(&self, args: &OpList) -> Result<DirIterator> {
        debug!(
            target: "opendal::services",
            "service={} operation={} path={} -> started",
            self.scheme,
            Operation::BlockingList,
            args.path()
        );

        self.inner
            .blocking_list(args)
            .map(|v| {
                debug!(
                    target: "opendal::services",
                    "service={} operation={} path={} -> got dir streamer",
                    self.scheme,
                    Operation::BlockingList,
                    args.path()
                );
                let li = LoggingIterator::new(self.scheme, args.path(), v);
                Box::new(li) as DirIterator
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Other {
                    error!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> failed: {err:?}",
                        self.scheme,
                        Operation::BlockingList,
                        args.path()
                    );
                } else {
                    warn!(
                        target: "opendal::services",
                        "service={} operation={} path={} -> errored: {err:?}",
                        self.scheme,
                        Operation::BlockingList,
                        args.path()
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
    has_read: u64,
    inner: BytesReader,
}

impl LoggingReader {
    fn new(scheme: Scheme, op: Operation, path: &str, reader: BytesReader) -> Self {
        Self {
            scheme,
            op,
            path: path.to_string(),
            has_read: 0,
            inner: reader,
        }
    }
}

impl AsyncRead for LoggingReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        match Pin::new(&mut (*self.inner)).poll_read(cx, buf) {
            Poll::Ready(res) => match res {
                Ok(n) => {
                    self.has_read += n as u64;
                    trace!(
                        target: "opendal::services",
                        "service={} operation={} path={} has_read={} -> {}: {}B",
                        self.scheme, self.op, self.path, self.has_read,self.op, n);
                    Poll::Ready(Ok(n))
                }
                Err(e) => {
                    if e.kind() == ErrorKind::Other {
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
    has_read: u64,
    inner: BlockingBytesReader,
}

impl BlockingLoggingReader {
    fn new(scheme: Scheme, op: Operation, path: &str, reader: BlockingBytesReader) -> Self {
        Self {
            scheme,
            op,
            path: path.to_string(),
            has_read: 0,
            inner: reader,
        }
    }
}

impl Read for BlockingLoggingReader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
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
                if e.kind() == ErrorKind::Other {
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
    scheme: Scheme,
    path: String,
    inner: DirStreamer,
}

impl LoggingStreamer {
    fn new(scheme: Scheme, path: &str, inner: DirStreamer) -> Self {
        Self {
            scheme,
            path: path.to_string(),
            inner,
        }
    }
}

impl Stream for LoggingStreamer {
    type Item = Result<DirEntry>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut (*self.inner)).poll_next(cx) {
            Poll::Ready(opt) => match opt {
                Some(res) => match res {
                    Ok(de) => {
                        debug!(
                            target: "opendal::service",
                            "service={} operation={} path={} -> got entry: mode={} path={}",
                            self.scheme,
                            Operation::List,
                            self.path,
                            de.mode(),
                            de.path(),
                        );
                        Poll::Ready(Some(Ok(de)))
                    }
                    Err(e) => {
                        if e.kind() == ErrorKind::Other {
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
    scheme: Scheme,
    path: String,
    inner: DirIterator,
}

impl LoggingIterator {
    fn new(scheme: Scheme, path: &str, inner: DirIterator) -> Self {
        Self {
            scheme,
            path: path.to_string(),
            inner,
        }
    }
}

impl Iterator for LoggingIterator {
    type Item = Result<DirEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(res) => match res {
                Ok(de) => {
                    debug!(
                        target: "opendal::service",
                        "service={} operation={} path={} -> got entry: mode={} path={}",
                        self.scheme,
                        Operation::BlockingList,
                        self.path,
                        de.mode(),
                        de.path(),
                    );
                    Some(Ok(de))
                }
                Err(e) => {
                    if e.kind() == ErrorKind::Other {
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
                None
            }
        }
    }
}
