// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Timeout layer implementation for Apache OpenDAL.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_docs)]

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use opendal_core::raw::*;
use opendal_core::*;

/// Add timeout for every operation to avoid slow or unexpected hang operations.
#[derive(Clone)]
pub struct TimeoutLayer {
    timeout: Duration,
    io_timeout: Duration,
}

impl Default for TimeoutLayer {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(60),
            io_timeout: Duration::from_secs(10),
        }
    }
}

impl TimeoutLayer {
    /// Create a new [`TimeoutLayer`] with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set timeout for non-io operations like `stat`, `delete`.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set io timeout for operations like `read`, `write`.
    pub fn with_io_timeout(mut self, timeout: Duration) -> Self {
        self.io_timeout = timeout;
        self
    }
}

impl<A: Access> Layer<A> for TimeoutLayer {
    type LayeredAccess = TimeoutAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        let info = inner.info();
        info.update_executor(|exec| {
            Executor::with(TimeoutExecutor::new(exec.into_inner(), self.io_timeout))
        });

        TimeoutAccessor {
            inner,
            timeout: self.timeout,
            io_timeout: self.io_timeout,
        }
    }
}

#[doc(hidden)]
#[derive(Debug)]
pub struct TimeoutAccessor<A: Access> {
    inner: A,
    timeout: Duration,
    io_timeout: Duration,
}

impl<A: Access> TimeoutAccessor<A> {
    async fn timeout<F: Future<Output = Result<T>>, T>(&self, op: Operation, fut: F) -> Result<T> {
        tokio::time::timeout(self.timeout, fut).await.map_err(|_| {
            Error::new(ErrorKind::Unexpected, "operation timeout reached")
                .with_operation(op)
                .with_context("timeout", format!("{:?}", self.timeout))
                .set_temporary()
        })?
    }

    async fn io_timeout<F: Future<Output = Result<T>>, T>(
        &self,
        op: Operation,
        fut: F,
    ) -> Result<T> {
        tokio::time::timeout(self.io_timeout, fut)
            .await
            .map_err(|_| {
                Error::new(ErrorKind::Unexpected, "io timeout reached")
                    .with_operation(op)
                    .with_context("timeout", format!("{:?}", self.io_timeout))
                    .set_temporary()
            })?
    }
}

impl<A: Access> LayeredAccess for TimeoutAccessor<A> {
    type Inner = A;
    type Reader = TimeoutWrapper<A::Reader>;
    type Writer = TimeoutWrapper<A::Writer>;
    type Lister = TimeoutWrapper<A::Lister>;
    type Deleter = TimeoutWrapper<A::Deleter>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.timeout(Operation::CreateDir, self.inner.create_dir(path, args))
            .await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.io_timeout(Operation::Read, self.inner.read(path, args))
            .await
            .map(|(rp, r)| (rp, TimeoutWrapper::new(r, self.io_timeout)))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.io_timeout(Operation::Write, self.inner.write(path, args))
            .await
            .map(|(rp, r)| (rp, TimeoutWrapper::new(r, self.io_timeout)))
    }

    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        self.timeout(Operation::Copy, self.inner.copy(from, to, args))
            .await
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        self.timeout(Operation::Rename, self.inner.rename(from, to, args))
            .await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.timeout(Operation::Stat, self.inner.stat(path, args))
            .await
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        self.timeout(Operation::Delete, self.inner.delete())
            .await
            .map(|(rp, r)| (rp, TimeoutWrapper::new(r, self.io_timeout)))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.io_timeout(Operation::List, self.inner.list(path, args))
            .await
            .map(|(rp, r)| (rp, TimeoutWrapper::new(r, self.io_timeout)))
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.timeout(Operation::Presign, self.inner.presign(path, args))
            .await
    }
}

struct TimeoutExecutor {
    exec: Arc<dyn Execute>,
    timeout: Duration,
}

impl TimeoutExecutor {
    fn new(exec: Arc<dyn Execute>, timeout: Duration) -> Self {
        Self { exec, timeout }
    }
}

impl Execute for TimeoutExecutor {
    fn execute(&self, f: BoxedStaticFuture<()>) {
        self.exec.execute(f)
    }

    fn timeout(&self) -> Option<BoxedStaticFuture<()>> {
        Some(Box::pin(tokio::time::sleep(self.timeout)))
    }
}

#[doc(hidden)]
pub struct TimeoutWrapper<R> {
    inner: R,
    timeout: Duration,
}

impl<R> TimeoutWrapper<R> {
    fn new(inner: R, timeout: Duration) -> Self {
        Self { inner, timeout }
    }

    #[inline]
    async fn io_timeout<F: Future<Output = Result<T>>, T>(
        timeout: Duration,
        op: &'static str,
        fut: F,
    ) -> Result<T> {
        tokio::time::timeout(timeout, fut).await.map_err(|_| {
            Error::new(ErrorKind::Unexpected, "io operation timeout reached")
                .with_operation(op)
                .with_context("timeout", format!("{:?}", timeout))
                .set_temporary()
        })?
    }
}

impl<R: oio::Read> oio::Read for TimeoutWrapper<R> {
    async fn read(&mut self) -> Result<Buffer> {
        let fut = self.inner.read();
        Self::io_timeout(self.timeout, Operation::Read.into_static(), fut).await
    }
}

impl<R: oio::Write> oio::Write for TimeoutWrapper<R> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let fut = self.inner.write(bs);
        Self::io_timeout(self.timeout, Operation::Write.into_static(), fut).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        let fut = self.inner.close();
        Self::io_timeout(self.timeout, Operation::Write.into_static(), fut).await
    }

    async fn abort(&mut self) -> Result<()> {
        let fut = self.inner.abort();
        Self::io_timeout(self.timeout, Operation::Write.into_static(), fut).await
    }
}

impl<R: oio::List> oio::List for TimeoutWrapper<R> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        let fut = self.inner.next();
        Self::io_timeout(self.timeout, Operation::List.into_static(), fut).await
    }
}

impl<R: oio::Delete> oio::Delete for TimeoutWrapper<R> {
    async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        let fut = self.inner.delete(path, args);
        Self::io_timeout(self.timeout, Operation::Delete.into_static(), fut).await
    }

    async fn close(&mut self) -> Result<()> {
        let fut = self.inner.close();
        Self::io_timeout(self.timeout, Operation::Delete.into_static(), fut).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::pending;
    use tokio::time::sleep;

    #[derive(Debug, Clone, Default)]
    struct MockService;

    impl Access for MockService {
        type Reader = oio::Reader;
        type Writer = oio::Writer;
        type Lister = oio::Lister;
        type Deleter = oio::Deleter;

        fn info(&self) -> Arc<AccessorInfo> {
            let am = AccessorInfo::default();
            am.set_native_capability(Capability {
                read: true,
                delete: true,
                ..Default::default()
            });
            am.into()
        }

        async fn read(&self, _: &str, _: OpRead) -> Result<(RpRead, Self::Reader)> {
            Ok((RpRead::new(), Box::new(MockReader)))
        }

        async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
            sleep(Duration::from_secs(u64::MAX)).await;
            Ok((RpDelete::default(), Box::new(())))
        }
    }

    #[derive(Debug, Clone, Default)]
    struct MockReader;
    impl oio::Read for MockReader {
        fn read(&mut self) -> impl Future<Output = Result<Buffer>> {
            pending()
        }
    }

    #[tokio::test]
    async fn test_operation_timeout() {
        let srv = MockService;
        let op = Operator::from_inner(Arc::new(srv))
            .layer(TimeoutLayer::default().with_timeout(Duration::from_millis(10)));

        let res = op.delete("test").await;
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(err.to_string().contains("timeout"));
    }
}
