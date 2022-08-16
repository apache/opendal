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
use std::io::Result;
use std::sync::Arc;

use async_trait::async_trait;
use log::debug;
use log::error;
use log::warn;

use crate::ops::OpCreate;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpPresign;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::ops::Operation;
use crate::ops::PresignedRequest;
use crate::Accessor;
use crate::AccessorMetadata;
use crate::BytesReader;
use crate::BytesWriter;
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
            service = self.scheme.into_static(),
            operation = Operation::Metadata.into_static();
            "started");
        let result = self.inner.metadata();
        debug!(
            service = self.scheme.into_static(),
            operation = Operation::Metadata.into_static();
            "finished: {:?}", result);

        result
    }

    async fn create(&self, args: &OpCreate) -> Result<()> {
        debug!(
            service = self.scheme.into_static(),
            operation = Operation::Create.into_static(),
            path = args.path();
            "started");

        self.inner
            .create(args)
            .await
            .map(|v| {
                debug!(
                    service = self.scheme.into_static(),
                    operation = Operation::Create.into_static(),
                    path = args.path();
                    "finished");
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Other {
                    error!(
                        service = self.scheme.into_static(),
                        operation = Operation::Create.into_static(),
                        path = args.path();
                        "failed: {err:?}");
                } else {
                    warn!(
                        service = self.scheme.into_static(),
                        operation = Operation::Create.into_static(),
                        path = args.path();
                        "errored: {err:?}");
                };
                err
            })
    }

    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        debug!(
            service = self.scheme.into_static(),
            operation = Operation::Read.into_static(),
            path = args.path(),
            offset = format!("{:?}", args.offset()),
            size = format!("{:?}", args.size());
            "started");

        self.inner
            .read(args)
            .await
            .map(|v| {
                debug!(
                    service = self.scheme.into_static(),
                    operation = Operation::Read.into_static(),
                    path = args.path(),
                    offset = format!("{:?}", args.offset()),
                    size = format!("{:?}", args.size());
                    "got reader");
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Other {
                    error!(
                        service = self.scheme.into_static(),
                        operation = Operation::Read.into_static(),
                        path = args.path(),
                        offset = format!("{:?}", args.offset()),
                        size = format!("{:?}", args.size());
                        "failed: {err:?}");
                } else {
                    warn!(
                        service = self.scheme.into_static(),
                        operation = Operation::Read.into_static(),
                         path = args.path(),
                        offset = format!("{:?}", args.offset()),
                        size = format!("{:?}", args.size());
                        "errored: {err:?}");
                };
                err
            })
    }

    async fn write(&self, args: &OpWrite) -> Result<BytesWriter> {
        debug!(
            service = self.scheme.into_static(),
            operation = Operation::Write.into_static(),
            path = args.path(),
            size = format!("{:?}", args.size());
            "started");

        self.inner
            .write(args)
            .await
            .map(|v| {
                debug!(
                    service = self.scheme.into_static(),
                    operation = Operation::Write.into_static(),
                    path = args.path(),
                    size = format!("{:?}", args.size());
                    "got writer");
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Other {
                    error!(
                        service = self.scheme.into_static(),
                        operation = Operation::Write.into_static(),
                        path = args.path(),
                        size = format!("{:?}", args.size());
                        "failed: {err:?}");
                } else {
                    warn!(
                        service = self.scheme.into_static(),
                        operation = Operation::Write.into_static(),
                         path = args.path(),
                        size = format!("{:?}", args.size());
                        "errored: {err:?}");
                };
                err
            })
    }

    async fn stat(&self, args: &OpStat) -> Result<ObjectMetadata> {
        debug!(
            service = self.scheme.into_static(),
            operation = Operation::Stat.into_static(),
            path = args.path();
            "started");

        self.inner
            .stat(args)
            .await
            .map(|v| {
                debug!(
                    service = self.scheme.into_static(),
                    operation = Operation::Stat.into_static(),
                    path = args.path();
                    "finished: {v:?}");
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Other {
                    error!(
                        service = self.scheme.into_static(),
                        operation = Operation::Stat.into_static(),
                        path = args.path();
                        "failed: {err:?}");
                } else {
                    warn!(
                        service = self.scheme.into_static(),
                        operation = Operation::Stat.into_static(),
                         path = args.path();
                        "errored: {err:?}");
                };
                err
            })
    }

    async fn delete(&self, args: &OpDelete) -> Result<()> {
        debug!(
            service = self.scheme.into_static(),
            operation = Operation::Delete.into_static(),
            path = args.path();
            "started");

        self.inner
            .delete(args)
            .await
            .map(|v| {
                debug!(
                    service = self.scheme.into_static(),
                    operation = Operation::Delete.into_static(),
                    path = args.path();
                    "finished");
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Other {
                    error!(
                        service = self.scheme.into_static(),
                        operation = Operation::Delete.into_static(),
                        path = args.path();
                        "failed: {err:?}");
                } else {
                    warn!(
                        service = self.scheme.into_static(),
                        operation = Operation::Delete.into_static(),
                         path = args.path();
                        "errored: {err:?}");
                };
                err
            })
    }

    async fn list(&self, args: &OpList) -> Result<DirStreamer> {
        debug!(
            service = self.scheme.into_static(),
            operation = Operation::List.into_static(),
            path = args.path();
            "started");

        self.inner
            .list(args)
            .await
            .map(|v| {
                debug!(
                    service = self.scheme.into_static(),
                    operation = Operation::List.into_static(),
                    path = args.path();
                    "got dir streamer");
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Other {
                    error!(
                        service = self.scheme.into_static(),
                        operation = Operation::List.into_static(),
                        path = args.path();
                        "failed: {err:?}");
                } else {
                    warn!(
                        service = self.scheme.into_static(),
                        operation = Operation::List.into_static(),
                         path = args.path();
                        "errored: {err:?}");
                };
                err
            })
    }

    fn presign(&self, args: &OpPresign) -> Result<PresignedRequest> {
        debug!(
            service = self.scheme.into_static(),
            operation = Operation::Presign.into_static();
            "started");

        self.inner
            .presign(args)
            .map(|v| {
                debug!(
                    service = self.scheme.into_static(),
                    operation = Operation::Presign.into_static(),
                    path = args.path();
                    "finished: {v:?}");
                v
            })
            .map_err(|err| {
                if err.kind() == ErrorKind::Other {
                    error!(
                        service = self.scheme.into_static(),
                        operation = Operation::Presign.into_static(),
                        path = args.path();
                        "failed: {err:?}");
                } else {
                    warn!(
                        service = self.scheme.into_static(),
                        operation = Operation::Presign.into_static(),
                         path = args.path();
                        "errored: {err:?}");
                };
                err
            })
    }
}
