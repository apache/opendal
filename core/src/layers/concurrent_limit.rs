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

use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use futures::Stream;
use futures::StreamExt;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

use crate::raw::*;
use crate::*;

/// Add concurrent request limit.
///
/// # Notes
///
/// Users can control how many concurrent connections could be established
/// between OpenDAL and underlying storage services.
///
/// All operators wrapped by this layer will share a common semaphore. This
/// allows you to reuse the same layer across multiple operators, ensuring
/// that the total number of concurrent requests across the entire
/// application does not exceed the limit.
///
/// # Examples
///
/// Add a concurrent limit layer to the operator:
///
/// ```no_run
/// # use opendal::layers::ConcurrentLimitLayer;
/// # use opendal::services;
/// # use opendal::Operator;
/// # use opendal::Result;
/// # use opendal::Scheme;
///
/// # fn main() -> Result<()> {
/// use std::sync::Arc;
/// use tokio::sync::Semaphore;
///
/// let semaphore = Arc::new(Semaphore::new(1024));
/// let _ = Operator::new(services::Memory::default())?
///     .layer(ConcurrentLimitLayer::new(semaphore))
///     .finish();
/// Ok(())
/// # }
/// ```
///
/// Share a concurrent limit layer between the operators:
///
/// ```no_run
/// # use opendal::layers::ConcurrentLimitLayer;
/// # use opendal::services;
/// # use opendal::Operator;
/// # use opendal::Result;
/// # use opendal::Scheme;
///
/// # fn main() -> Result<()> {
/// use std::sync::Arc;
/// use tokio::sync::Semaphore;
///
/// let semaphore = Arc::new(Semaphore::new(1024));
/// let limit = ConcurrentLimitLayer::new(semaphore);
///
/// let _operator_a = Operator::new(services::Memory::default())?
///     .layer(limit.clone())
///     .finish();
/// let _operator_b = Operator::new(services::Memory::default())?
///     .layer(limit.clone())
///     .finish();
///
/// Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct ConcurrentLimitLayer {
    operation_semaphore: Arc<Semaphore>,
    http_semaphore: Option<Arc<Semaphore>>,
}

impl ConcurrentLimitLayer {
    /// Create a new `ConcurrentLimitLayer` with a custom semaphore.
    ///
    /// The provided semaphore will be shared by every operator wrapped by this
    /// layer, giving callers full control over its configuration.
    pub fn new(operation_semaphore: Arc<Semaphore>) -> Self {
        Self {
            operation_semaphore,
            http_semaphore: None,
        }
    }

    /// Create a new `ConcurrentLimitLayer` with the specified number of
    /// permits.
    pub fn with_permits(permits: usize) -> Self {
        Self::new(Arc::new(Semaphore::new(permits)))
    }

    /// Set a concurrent limit for HTTP requests.
    ///
    /// This convenience helper constructs a new semaphore with the specified
    /// number of permits and calls [`ConcurrentLimitLayer::with_http_semaphore`].
    /// Use [`ConcurrentLimitLayer::with_http_semaphore`] directly when reusing
    /// a shared semaphore.
    pub fn with_http_concurrent_limit(self, permits: usize) -> Self {
        self.with_http_semaphore(Arc::new(Semaphore::new(permits)))
    }

    /// Provide a custom semaphore to limit HTTP request concurrency.
    ///
    /// Sharing the same semaphore across layers allows coordinating HTTP
    /// usage with other parts of the application.
    pub fn with_http_semaphore(mut self, semaphore: Arc<Semaphore>) -> Self {
        self.http_semaphore = Some(semaphore);
        self
    }

    /// Get the semaphore used to limit operation concurrency.
    ///
    /// The returned semaphore can be shared with other parts of your
    /// application to coordinate with OpenDAL's concurrency limits. External
    /// users should only acquire permits from this semaphore. Changing the
    /// number of permits (for example by calling [`Semaphore::add_permits`])
    /// will affect OpenDAL's internal behavior.
    pub fn operation_semaphore(&self) -> Arc<Semaphore> {
        self.operation_semaphore.clone()
    }

    /// Get the semaphore controlling HTTP concurrency, if any.
    ///
    /// Returns `None` when HTTP concurrency has not been configured via
    /// [`ConcurrentLimitLayer::with_http_concurrent_limit`] or
    /// [`ConcurrentLimitLayer::with_http_semaphore`].
    pub fn http_semaphore(&self) -> Option<Arc<Semaphore>> {
        self.http_semaphore.clone()
    }
}

impl<A: Access> Layer<A> for ConcurrentLimitLayer {
    type LayeredAccess = ConcurrentLimitAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        let info = inner.info();

        // Update http client with metrics http fetcher.
        info.update_http_client(|client| {
            HttpClient::with(ConcurrentLimitHttpFetcher {
                inner: client.into_inner(),
                http_semaphore: self.http_semaphore.clone(),
            })
        });

        ConcurrentLimitAccessor {
            inner,
            semaphore: self.operation_semaphore.clone(),
        }
    }
}

pub struct ConcurrentLimitHttpFetcher {
    inner: HttpFetcher,
    http_semaphore: Option<Arc<Semaphore>>,
}

impl HttpFetch for ConcurrentLimitHttpFetcher {
    async fn fetch(&self, req: http::Request<Buffer>) -> Result<http::Response<HttpBody>> {
        let Some(semaphore) = self.http_semaphore.clone() else {
            return self.inner.fetch(req).await;
        };

        let permit = semaphore
            .acquire_owned()
            .await
            .expect("semaphore must be valid");

        let resp = self.inner.fetch(req).await?;
        let (parts, body) = resp.into_parts();
        let body = body.map_inner(|s| {
            Box::new(ConcurrentLimitStream {
                inner: s,
                _permit: permit,
            })
        });
        Ok(http::Response::from_parts(parts, body))
    }
}

pub struct ConcurrentLimitStream<S> {
    inner: S,
    // Hold on this permit until this reader has been dropped.
    _permit: OwnedSemaphorePermit,
}

impl<S> Stream for ConcurrentLimitStream<S>
where
    S: Stream<Item = Result<Buffer>> + Unpin + 'static,
{
    type Item = Result<Buffer>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

#[derive(Debug, Clone)]
pub struct ConcurrentLimitAccessor<A: Access> {
    inner: A,
    semaphore: Arc<Semaphore>,
}

impl<A: Access> LayeredAccess for ConcurrentLimitAccessor<A> {
    type Inner = A;
    type Reader = ConcurrentLimitWrapper<A::Reader>;
    type Writer = ConcurrentLimitWrapper<A::Writer>;
    type Lister = ConcurrentLimitWrapper<A::Lister>;
    type Deleter = ConcurrentLimitWrapper<A::Deleter>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .expect("semaphore must be valid");

        self.inner.create_dir(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore must be valid");

        self.inner
            .read(path, args)
            .await
            .map(|(rp, r)| (rp, ConcurrentLimitWrapper::new(r, permit)))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore must be valid");

        self.inner
            .write(path, args)
            .await
            .map(|(rp, w)| (rp, ConcurrentLimitWrapper::new(w, permit)))
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .expect("semaphore must be valid");

        self.inner.stat(path, args).await
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore must be valid");

        self.inner
            .delete()
            .await
            .map(|(rp, w)| (rp, ConcurrentLimitWrapper::new(w, permit)))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore must be valid");

        self.inner
            .list(path, args)
            .await
            .map(|(rp, s)| (rp, ConcurrentLimitWrapper::new(s, permit)))
    }
}

pub struct ConcurrentLimitWrapper<R> {
    inner: R,

    // Hold on this permit until this reader has been dropped.
    _permit: OwnedSemaphorePermit,
}

impl<R> ConcurrentLimitWrapper<R> {
    fn new(inner: R, permit: OwnedSemaphorePermit) -> Self {
        Self {
            inner,
            _permit: permit,
        }
    }
}

impl<R: oio::Read> oio::Read for ConcurrentLimitWrapper<R> {
    async fn read(&mut self) -> Result<Buffer> {
        self.inner.read().await
    }
}

impl<R: oio::Write> oio::Write for ConcurrentLimitWrapper<R> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        self.inner.write(bs).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.inner.close().await
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}

impl<R: oio::List> oio::List for ConcurrentLimitWrapper<R> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        self.inner.next().await
    }
}

impl<R: oio::Delete> oio::Delete for ConcurrentLimitWrapper<R> {
    fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        self.inner.delete(path, args)
    }

    async fn flush(&mut self) -> Result<usize> {
        self.inner.flush().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services;
    use crate::Operator;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Semaphore;
    use tokio::time::timeout;

    #[tokio::test]
    async fn operation_semaphore_can_be_shared() {
        let layer = ConcurrentLimitLayer::with_permits(1);
        let semaphore = layer.operation_semaphore();

        // Hold the only permit so operations queued through the layer must wait.
        let permit = semaphore.acquire_owned().await.expect("semaphore valid");

        let op = Operator::new(services::Memory::default())
            .expect("operator must build")
            .layer(layer)
            .finish();

        // While the permit is held externally, the operation should block and the timeout expire.
        let blocked = timeout(Duration::from_millis(50), op.stat("any")).await;
        assert!(
            blocked.is_err(),
            "operation should be limited by shared semaphore"
        );

        drop(permit);

        // After releasing the permit, the operation can proceed (the result may still be an error
        // because the object doesn't exist, but the future completes before the timeout).
        let completed = timeout(Duration::from_millis(50), op.stat("any")).await;
        assert!(
            completed.is_ok(),
            "operation should proceed once permit is released"
        );
    }

    #[test]
    fn http_semaphore_reflects_configuration() {
        let base = ConcurrentLimitLayer::with_permits(2);
        assert!(base.http_semaphore().is_none());

        let configured = base.clone().with_http_concurrent_limit(3);
        let http_sem = configured
            .http_semaphore()
            .expect("http semaphore should be configured");
        assert_eq!(http_sem.available_permits(), 3);

        let shared = Arc::new(Semaphore::new(4));
        let shared_configured = base.with_http_semaphore(shared.clone());
        let shared_http = shared_configured
            .http_semaphore()
            .expect("shared semaphore should be returned");
        assert!(Arc::ptr_eq(&shared, &shared_http));
    }
}
