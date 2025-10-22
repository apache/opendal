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

use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use futures::future::BoxFuture;
use futures::Stream;
use futures::StreamExt;
use std::any::Any;
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
/// ConcurrencySemaphore abstracts a semaphore-like concurrency primitive
/// that yields an owned permit released on drop. It mirrors RetryLayer's
/// interceptor pattern by serving as a generic extension point for the layer.
pub trait ConcurrencySemaphore: Send + Sync + Any + Clone + 'static {
    /// The owned permit type associated with the semaphore. Dropping it
    /// must release the permit back to the semaphore.
    type Permit: Send + Sync + 'static;

    /// Acquire an owned permit asynchronously.
    fn acquire_owned(&self) -> BoxFuture<'static, Self::Permit>;
}

/// An adapter for `tokio::sync::Semaphore` that implements
/// `ConcurrencySemaphore`.
#[derive(Clone)]
pub struct TokioSemaphore(Arc<Semaphore>);

impl TokioSemaphore {
    fn new(inner: Arc<Semaphore>) -> Self {
        Self(inner)
    }

    #[cfg(test)]
    fn inner(&self) -> &Arc<Semaphore> {
        &self.0
    }
}

impl ConcurrencySemaphore for TokioSemaphore {
    type Permit = tokio::sync::OwnedSemaphorePermit;

    fn acquire_owned(&self) -> BoxFuture<'static, Self::Permit> {
        let sem = self.0.clone();
        Box::pin(async move { sem.acquire_owned().await.expect("semaphore must be valid") })
    }
}

/// Add concurrent request limit.
#[derive(Clone)]
pub struct ConcurrentLimitLayer<S: ConcurrencySemaphore = TokioSemaphore> {
    operation_semaphore: Arc<S>,
    http_semaphore: Option<Arc<S>>,
}

impl ConcurrentLimitLayer<TokioSemaphore> {
    /// Create a new `ConcurrentLimitLayer` with a custom semaphore.
    ///
    /// The provided semaphore will be shared by every operator wrapped by this
    /// layer, giving callers full control over its configuration.
    pub fn new(operation_semaphore: Arc<Semaphore>) -> Self {
        Self::with_semaphore(Arc::new(TokioSemaphore::new(operation_semaphore)))
    }

    // with_semaphore is provided by the generic impl below for the default Tokio adapter.

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
        self.http_semaphore = Some(Arc::new(TokioSemaphore::new(semaphore)));
        self
    }

    // with_http_concurrency is provided by the generic impl below.
}

impl<S: ConcurrencySemaphore> ConcurrentLimitLayer<S> {
    /// Create a layer with any ConcurrencySemaphore implementation.
    pub fn with_semaphore(operation_semaphore: Arc<S>) -> Self {
        Self {
            operation_semaphore,
            http_semaphore: None,
        }
    }

    /// Provide a custom HTTP concurrency semaphore instance.
    pub fn with_http_concurrency(mut self, semaphore: Arc<S>) -> Self {
        self.http_semaphore = Some(semaphore);
        self
    }
}

impl<A: Access, S: ConcurrencySemaphore> Layer<A> for ConcurrentLimitLayer<S>
where
    S::Permit: Unpin,
{
    type LayeredAccess = ConcurrentLimitAccessor<A, S>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        let info = inner.info();

        // Update http client with metrics http fetcher.
        info.update_http_client(|client| {
            HttpClient::with(ConcurrentLimitHttpFetcher::<S> {
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

pub struct ConcurrentLimitHttpFetcher<S: ConcurrencySemaphore> {
    inner: HttpFetcher,
    http_semaphore: Option<Arc<S>>,
}

impl<S: ConcurrencySemaphore> HttpFetch for ConcurrentLimitHttpFetcher<S>
where
    S::Permit: Unpin,
{
    async fn fetch(&self, req: http::Request<Buffer>) -> Result<http::Response<HttpBody>> {
        let Some(semaphore) = self.http_semaphore.clone() else {
            return self.inner.fetch(req).await;
        };

        let permit = semaphore.acquire_owned().await;

        let resp = self.inner.fetch(req).await?;
        let (parts, body) = resp.into_parts();
        let body = body.map_inner(|s| {
            Box::new(ConcurrentLimitStream::<_, S::Permit> {
                inner: s,
                _permit: permit,
            })
        });
        Ok(http::Response::from_parts(parts, body))
    }
}

pub struct ConcurrentLimitStream<S, P> {
    inner: S,
    // Hold on this permit until this reader has been dropped.
    _permit: P,
}

impl<S, P> Stream for ConcurrentLimitStream<S, P>
where
    S: Stream<Item = Result<Buffer>> + Unpin + 'static,
    P: Unpin,
{
    type Item = Result<Buffer>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Safe due to Unpin bounds on S and P (thus on Self).
        let this = self.get_mut();
        this.inner.poll_next_unpin(cx)
    }
}

#[derive(Clone)]
pub struct ConcurrentLimitAccessor<A: Access, S: ConcurrencySemaphore> {
    inner: A,
    semaphore: Arc<S>,
}

impl<A: Access, S: ConcurrencySemaphore> std::fmt::Debug for ConcurrentLimitAccessor<A, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConcurrentLimitAccessor")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl<A: Access, S: ConcurrencySemaphore> LayeredAccess for ConcurrentLimitAccessor<A, S>
where
    S::Permit: Unpin,
{
    type Inner = A;
    type Reader = ConcurrentLimitWrapper<A::Reader, S::Permit>;
    type Writer = ConcurrentLimitWrapper<A::Writer, S::Permit>;
    type Lister = ConcurrentLimitWrapper<A::Lister, S::Permit>;
    type Deleter = ConcurrentLimitWrapper<A::Deleter, S::Permit>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        let _permit = self.semaphore.acquire_owned().await;

        self.inner.create_dir(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let permit = self.semaphore.acquire_owned().await;

        self.inner
            .read(path, args)
            .await
            .map(|(rp, r)| (rp, ConcurrentLimitWrapper::new(r, permit)))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let permit = self.semaphore.acquire_owned().await;

        self.inner
            .write(path, args)
            .await
            .map(|(rp, w)| (rp, ConcurrentLimitWrapper::new(w, permit)))
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let _permit = self.semaphore.acquire_owned().await;

        self.inner.stat(path, args).await
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        let permit = self.semaphore.acquire_owned().await;

        self.inner
            .delete()
            .await
            .map(|(rp, w)| (rp, ConcurrentLimitWrapper::new(w, permit)))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let permit = self.semaphore.acquire_owned().await;

        self.inner
            .list(path, args)
            .await
            .map(|(rp, s)| (rp, ConcurrentLimitWrapper::new(s, permit)))
    }
}

pub struct ConcurrentLimitWrapper<R, P> {
    inner: R,

    // Hold on this permit until this reader has been dropped.
    _permit: P,
}

impl<R, P> ConcurrentLimitWrapper<R, P> {
    fn new(inner: R, permit: P) -> Self {
        Self {
            inner,
            _permit: permit,
        }
    }
}

impl<R: oio::Read, P: Send + Sync + 'static + Unpin> oio::Read for ConcurrentLimitWrapper<R, P> {
    async fn read(&mut self) -> Result<Buffer> {
        self.inner.read().await
    }
}

impl<R: oio::Write, P: Send + Sync + 'static + Unpin> oio::Write for ConcurrentLimitWrapper<R, P> {
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

impl<R: oio::List, P: Send + Sync + 'static + Unpin> oio::List for ConcurrentLimitWrapper<R, P> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        self.inner.next().await
    }
}

impl<R: oio::Delete, P: Send + Sync + 'static + Unpin> oio::Delete
    for ConcurrentLimitWrapper<R, P>
{
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
    use tokio::time::timeout;

    use futures::channel::oneshot;
    use std::any::Any as StdAny;
    use std::collections::VecDeque;
    use std::sync::Mutex as StdMutex;
    use tokio::sync::Semaphore as TokioSemaphoreInner;

    #[derive(Clone)]
    struct TestSemaphore {
        inner: Arc<TokioSemaphoreInner>,
    }

    impl ConcurrencySemaphore for TestSemaphore {
        type Permit = tokio::sync::OwnedSemaphorePermit;
        fn acquire_owned(&self) -> BoxFuture<'static, Self::Permit> {
            let inner = self.inner.clone();
            Box::pin(async move {
                inner
                    .acquire_owned()
                    .await
                    .expect("semaphore must be valid")
            })
        }
    }

    // A minimal non-tokio semaphore for testing the trait abstraction.
    #[derive(Clone)]
    struct SimpleSemaphore {
        state: Arc<StdMutex<SimpleState>>,
    }

    struct SimpleState {
        available: usize,
        waiters: VecDeque<oneshot::Sender<()>>,
    }

    impl SimpleSemaphore {
        fn new(permits: usize) -> Self {
            Self {
                state: Arc::new(StdMutex::new(SimpleState {
                    available: permits,
                    waiters: VecDeque::new(),
                })),
            }
        }
    }

    struct SimplePermit(Arc<StdMutex<SimpleState>>);
    impl Drop for SimplePermit {
        fn drop(&mut self) {
            let mut st = self.0.lock().unwrap();
            st.available += 1;
            if let Some(tx) = st.waiters.pop_front() {
                let _ = tx.send(());
            }
        }
    }
    impl ConcurrencySemaphore for SimpleSemaphore {
        type Permit = SimplePermit;
        fn acquire_owned(&self) -> BoxFuture<'static, Self::Permit> {
            let state = self.state.clone();
            Box::pin(async move {
                loop {
                    // fast path
                    if let Some(permit) = {
                        let mut st = state.lock().unwrap();
                        if st.available > 0 {
                            st.available -= 1;
                            Some(())
                        } else {
                            None
                        }
                    } {
                        let _ = permit; // consumed
                        return SimplePermit(state.clone());
                    }

                    // slow path: park and wait for a drop
                    let (tx, rx) = oneshot::channel();
                    {
                        let mut st = state.lock().unwrap();
                        st.waiters.push_back(tx);
                    }
                    let _ = rx.await;
                    // loop to try acquire again
                }
            })
        }
    }

    #[tokio::test]
    async fn operation_semaphore_can_be_shared() {
        // Prepare a shared semaphore and feed it into the layer.
        let tokio_sem = Arc::new(Semaphore::new(1));
        let layer = ConcurrentLimitLayer::new(tokio_sem.clone());

        // Hold the only permit so operations queued through the layer must wait.
        let permit = tokio_sem.acquire_owned().await.expect("semaphore valid");

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

    #[tokio::test]
    async fn custom_semaphore_is_honored() {
        let custom = Arc::new(SimpleSemaphore::new(1));
        let layer = ConcurrentLimitLayer::with_semaphore(custom);

        let op = Operator::new(services::Memory::default())
            .expect("operator must build")
            .layer(layer)
            .finish();

        // Acquire a permit via lister and hold it by not advancing the stream.
        let _l = op.lister("").await.expect("list should start");

        // While the permit is held, a new op should be blocked and timeout.
        let blocked = timeout(Duration::from_millis(50), op.stat("any")).await;
        assert!(
            blocked.is_err(),
            "operation should be limited by custom semaphore"
        );
    }
}
