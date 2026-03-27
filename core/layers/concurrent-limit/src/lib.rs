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

//! Concurrent request limit layer implementation for Apache OpenDAL.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use futures::Stream;
use futures::StreamExt;
use mea::semaphore::OwnedSemaphorePermit;
use mea::semaphore::Semaphore;
use opendal_core::raw::*;
use opendal_core::*;

/// ConcurrentLimitSemaphore abstracts a semaphore-like concurrency primitive
/// that yields an owned permit released on drop.
pub trait ConcurrentLimitSemaphore: Send + Sync + Clone + Unpin + 'static {
    /// The owned permit type associated with the semaphore. Dropping it
    /// must release the permit back to the semaphore.
    type Permit: Send + Sync + 'static;

    /// Acquire an owned permit asynchronously.
    fn acquire(&self) -> impl Future<Output = Self::Permit> + MaybeSend;
}

impl ConcurrentLimitSemaphore for Arc<Semaphore> {
    type Permit = OwnedSemaphorePermit;

    async fn acquire(&self) -> Self::Permit {
        self.clone().acquire_owned(1).await
    }
}

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
/// # use opendal_core::services;
/// # use opendal_core::Operator;
/// # use opendal_core::Result;
/// # use opendal_layer_concurrent_limit::ConcurrentLimitLayer;
/// #
/// # fn main() -> Result<()> {
/// let _ = Operator::new(services::Memory::default())?
///     .layer(ConcurrentLimitLayer::new(1024))
///     .finish();
/// # Ok(())
/// # }
/// ```
///
/// Set per-operation concurrent limits to control different operations
/// independently:
///
/// ```no_run
/// # use opendal_core::services;
/// # use opendal_core::Operator;
/// # use opendal_core::Result;
/// # use opendal_core::raw::Operation;
/// # use opendal_layer_concurrent_limit::ConcurrentLimitLayer;
/// #
/// # fn main() -> Result<()> {
/// let _ = Operator::new(services::Memory::default())?
///     .layer(
///         ConcurrentLimitLayer::new(1024)
///             .with_operation_limit(Operation::Read, 64)
///             .with_operation_limit(Operation::Write, 32),
///     )
///     .finish();
/// # Ok(())
/// # }
/// ```
///
/// Share a concurrent limit layer between the operators:
///
/// ```no_run
/// # use opendal_core::services;
/// # use opendal_core::Operator;
/// # use opendal_core::Result;
/// # use opendal_layer_concurrent_limit::ConcurrentLimitLayer;
/// #
/// # fn main() -> Result<()> {
/// let limit = ConcurrentLimitLayer::new(1024);
///
/// let _operator_a = Operator::new(services::Memory::default())?
///     .layer(limit.clone())
///     .finish();
/// let _operator_b = Operator::new(services::Memory::default())?
///     .layer(limit.clone())
///     .finish();
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct ConcurrentLimitLayer<S: ConcurrentLimitSemaphore = Arc<Semaphore>> {
    operation_semaphore: S,
    http_semaphore: Option<S>,
    operation_limits: Option<Arc<HashMap<Operation, Arc<Semaphore>>>>,
}

impl ConcurrentLimitLayer<Arc<Semaphore>> {
    /// Create a new `ConcurrentLimitLayer` with the specified number of
    /// permits.
    ///
    /// These permits will be applied to all operations.
    pub fn new(permits: usize) -> Self {
        Self::with_semaphore(Arc::new(Semaphore::new(permits)))
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

    /// Set a concurrent limit for a specific operation type.
    ///
    /// When a per-operation limit is configured, that operation will acquire
    /// a permit from its dedicated semaphore instead of the global one. This
    /// allows fine-grained control over concurrency for different operation
    /// types.
    ///
    /// Operations without a dedicated limit will continue to use the global
    /// semaphore.
    ///
    /// # Examples
    ///
    /// Limit read and write concurrency while leaving metadata operations
    /// unrestricted by the global limit:
    ///
    /// ```no_run
    /// # use opendal_core::services;
    /// # use opendal_core::Operator;
    /// # use opendal_core::Result;
    /// # use opendal_core::raw::Operation;
    /// # use opendal_layer_concurrent_limit::ConcurrentLimitLayer;
    /// #
    /// # fn main() -> Result<()> {
    /// let _ = Operator::new(services::Memory::default())?
    ///     .layer(
    ///         ConcurrentLimitLayer::new(1024)
    ///             .with_operation_limit(Operation::Read, 64)
    ///             .with_operation_limit(Operation::Write, 32),
    ///     )
    ///     .finish();
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_operation_limit(mut self, op: Operation, permits: usize) -> Self {
        let limits = self
            .operation_limits
            .get_or_insert_with(|| Arc::new(HashMap::new()));
        Arc::make_mut(limits).insert(op, Arc::new(Semaphore::new(permits)));
        self
    }
}

impl<S: ConcurrentLimitSemaphore> ConcurrentLimitLayer<S> {
    /// Create a layer with any ConcurrentLimitSemaphore implementation.
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use mea::semaphore::Semaphore;
    /// # use opendal_layer_concurrent_limit::ConcurrentLimitLayer;
    /// let semaphore = Arc::new(Semaphore::new(1024));
    /// let _layer = ConcurrentLimitLayer::with_semaphore(semaphore);
    /// ```
    pub fn with_semaphore(operation_semaphore: S) -> Self {
        Self {
            operation_semaphore,
            http_semaphore: None,
            operation_limits: None,
        }
    }

    /// Provide a custom HTTP concurrency semaphore instance.
    pub fn with_http_semaphore(mut self, semaphore: S) -> Self {
        self.http_semaphore = Some(semaphore);
        self
    }
}

impl<A: Access, S: ConcurrentLimitSemaphore> Layer<A> for ConcurrentLimitLayer<S>
where
    S::Permit: Unpin,
{
    type LayeredAccess = ConcurrentLimitAccessor<A, S>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        let info = inner.info();

        // Update http client with concurrent limit http fetcher.
        info.update_http_client(|client| {
            HttpClient::with(ConcurrentLimitHttpFetcher::<S> {
                inner: client.into_inner(),
                http_semaphore: self.http_semaphore.clone(),
            })
        });

        ConcurrentLimitAccessor {
            inner,
            semaphore: self.operation_semaphore.clone(),
            operation_limits: self.operation_limits.clone(),
        }
    }
}

/// A permit that can come from either the global semaphore (generic `S`) or
/// a per-operation `Arc<Semaphore>`.
#[doc(hidden)]
pub enum ConcurrentLimitPermit<P> {
    /// Permit from the global semaphore.
    Global(P),
    /// Permit from a per-operation semaphore.
    PerOperation(OwnedSemaphorePermit),
}

#[doc(hidden)]
pub struct ConcurrentLimitHttpFetcher<S: ConcurrentLimitSemaphore> {
    inner: HttpFetcher,
    http_semaphore: Option<S>,
}

impl<S: ConcurrentLimitSemaphore> HttpFetch for ConcurrentLimitHttpFetcher<S>
where
    S::Permit: Unpin,
{
    async fn fetch(&self, req: http::Request<Buffer>) -> Result<http::Response<HttpBody>> {
        let Some(semaphore) = self.http_semaphore.clone() else {
            return self.inner.fetch(req).await;
        };

        let permit = semaphore.acquire().await;

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

struct ConcurrentLimitStream<S, P> {
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

#[doc(hidden)]
#[derive(Clone)]
pub struct ConcurrentLimitAccessor<A: Access, S: ConcurrentLimitSemaphore> {
    inner: A,
    semaphore: S,
    operation_limits: Option<Arc<HashMap<Operation, Arc<Semaphore>>>>,
}

impl<A: Access, S: ConcurrentLimitSemaphore> std::fmt::Debug for ConcurrentLimitAccessor<A, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConcurrentLimitAccessor")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl<A: Access, S: ConcurrentLimitSemaphore> ConcurrentLimitAccessor<A, S> {
    /// Acquire a permit for the given operation. If a per-operation semaphore
    /// is configured for this operation, acquire from it; otherwise fall back
    /// to the global semaphore.
    async fn acquire_for(&self, op: Operation) -> ConcurrentLimitPermit<S::Permit> {
        if let Some(limits) = &self.operation_limits {
            if let Some(sem) = limits.get(&op) {
                return ConcurrentLimitPermit::PerOperation(sem.clone().acquire_owned(1).await);
            }
        }
        ConcurrentLimitPermit::Global(self.semaphore.acquire().await)
    }
}

impl<A: Access, S: ConcurrentLimitSemaphore> LayeredAccess for ConcurrentLimitAccessor<A, S>
where
    S::Permit: Unpin,
{
    type Inner = A;
    type Reader = ConcurrentLimitWrapper<A::Reader, ConcurrentLimitPermit<S::Permit>>;
    type Writer = ConcurrentLimitWrapper<A::Writer, ConcurrentLimitPermit<S::Permit>>;
    type Lister = ConcurrentLimitWrapper<A::Lister, ConcurrentLimitPermit<S::Permit>>;
    type Deleter = ConcurrentLimitWrapper<A::Deleter, ConcurrentLimitPermit<S::Permit>>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        let _permit = self.acquire_for(Operation::CreateDir).await;

        self.inner.create_dir(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let permit = self.acquire_for(Operation::Read).await;

        self.inner
            .read(path, args)
            .await
            .map(|(rp, r)| (rp, ConcurrentLimitWrapper::new(r, permit)))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let permit = self.acquire_for(Operation::Write).await;

        self.inner
            .write(path, args)
            .await
            .map(|(rp, w)| (rp, ConcurrentLimitWrapper::new(w, permit)))
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let _permit = self.acquire_for(Operation::Stat).await;

        self.inner.stat(path, args).await
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        let permit = self.acquire_for(Operation::Delete).await;

        self.inner
            .delete()
            .await
            .map(|(rp, w)| (rp, ConcurrentLimitWrapper::new(w, permit)))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let permit = self.acquire_for(Operation::List).await;

        self.inner
            .list(path, args)
            .await
            .map(|(rp, s)| (rp, ConcurrentLimitWrapper::new(s, permit)))
    }
}

#[doc(hidden)]
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
    async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        self.inner.delete(path, args).await
    }

    async fn close(&mut self) -> Result<()> {
        self.inner.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal_core::Operator;
    use opendal_core::services;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::timeout;

    use futures::stream;
    use http::Response;

    #[tokio::test]
    async fn operation_semaphore_can_be_shared() {
        let semaphore = Arc::new(Semaphore::new(1));
        let layer = ConcurrentLimitLayer::with_semaphore(semaphore.clone());

        let permit = semaphore.clone().acquire_owned(1).await;

        let op = Operator::new(services::Memory::default())
            .expect("operator must build")
            .layer(layer)
            .finish();

        let blocked = timeout(Duration::from_millis(50), op.stat("any")).await;
        assert!(
            blocked.is_err(),
            "operation should be limited by shared semaphore"
        );

        drop(permit);

        let completed = timeout(Duration::from_millis(50), op.stat("any")).await;
        assert!(
            completed.is_ok(),
            "operation should proceed once permit is released"
        );
    }

    #[tokio::test]
    async fn per_operation_limit_isolates_operations() {
        // Stat has its own per-operation semaphore (1 permit).
        // Exhausting the global semaphore should NOT block stat.
        let global_sem = Arc::new(Semaphore::new(1));
        let layer = ConcurrentLimitLayer::with_semaphore(global_sem.clone())
            .with_operation_limit(Operation::Stat, 1);

        let op = Operator::new(services::Memory::default())
            .expect("operator must build")
            .layer(layer)
            .finish();

        // Exhaust the global semaphore externally.
        let _permit = global_sem.clone().acquire_owned(1).await;

        // Stat should still work because it uses its dedicated per-operation
        // semaphore, not the exhausted global one.
        let stat_result = timeout(Duration::from_millis(200), op.stat("any")).await;
        assert!(
            stat_result.is_ok(),
            "stat should not be blocked by exhausted global semaphore"
        );
    }

    #[tokio::test]
    async fn per_operation_limit_blocks_same_operation() {
        // Stat has its own per-operation limit of 1.
        // Externally exhaust the per-operation stat semaphore, then verify
        // that stat blocks.
        let layer = ConcurrentLimitLayer::new(1024).with_operation_limit(Operation::Stat, 1);

        // Grab a reference to the per-operation semaphore so we can
        // externally exhaust it. Build the layer, then extract the
        // semaphore from the operation_limits map.
        let op = Operator::new(services::Memory::default())
            .expect("operator must build")
            .layer(layer.clone())
            .finish();

        // Exhaust the per-operation stat semaphore by cloning the Arc from
        // the layer's internal map.
        let stat_sem = layer
            .operation_limits
            .as_ref()
            .expect("operation_limits must exist")
            .get(&Operation::Stat)
            .expect("stat semaphore must exist")
            .clone();
        let _permit = stat_sem.acquire_owned(1).await;

        // Stat should block because its per-operation semaphore is exhausted.
        let blocked = timeout(Duration::from_millis(50), op.stat("any")).await;
        assert!(
            blocked.is_err(),
            "stat should be blocked by exhausted per-operation semaphore"
        );
    }

    #[tokio::test]
    async fn operations_without_per_op_limit_use_global() {
        // Only stat gets a per-operation limit. Other operations (like
        // create_dir) should fall back to the global semaphore.
        let global_sem = Arc::new(Semaphore::new(1));
        let layer = ConcurrentLimitLayer::with_semaphore(global_sem.clone())
            .with_operation_limit(Operation::Stat, 10);

        let op = Operator::new(services::Memory::default())
            .expect("operator must build")
            .layer(layer)
            .finish();

        // Exhaust the global semaphore externally.
        let _permit = global_sem.clone().acquire_owned(1).await;

        // Stat should still work because it has a dedicated per-operation
        // semaphore with 10 permits, bypassing the exhausted global one.
        let stat_result = timeout(Duration::from_millis(200), op.stat("any")).await;
        assert!(
            stat_result.is_ok(),
            "stat should use per-operation semaphore, not the exhausted global one"
        );

        // create_dir has no per-operation limit, so it falls back to the
        // global semaphore which is exhausted -- it should block.
        let blocked = timeout(Duration::from_millis(50), op.create_dir("blocked/")).await;
        assert!(
            blocked.is_err(),
            "create_dir should be blocked by exhausted global semaphore"
        );
    }

    #[tokio::test]
    async fn http_semaphore_holds_until_body_dropped() {
        struct DummyFetcher;

        impl HttpFetch for DummyFetcher {
            async fn fetch(&self, _req: http::Request<Buffer>) -> Result<Response<HttpBody>> {
                let body = HttpBody::new(stream::empty(), None);
                Ok(Response::builder()
                    .status(http::StatusCode::OK)
                    .body(body)
                    .expect("response must build"))
            }
        }

        let semaphore = Arc::new(Semaphore::new(1));
        let layer = ConcurrentLimitLayer::new(1).with_http_semaphore(semaphore.clone());
        let fetcher = ConcurrentLimitHttpFetcher::<Arc<Semaphore>> {
            inner: HttpClient::with(DummyFetcher).into_inner(),
            http_semaphore: layer.http_semaphore.clone(),
        };

        let request = http::Request::builder()
            .uri("http://example.invalid/")
            .body(Buffer::new())
            .expect("request must build");
        let _resp = fetcher
            .fetch(request)
            .await
            .expect("first fetch should succeed");

        let request = http::Request::builder()
            .uri("http://example.invalid/")
            .body(Buffer::new())
            .expect("request must build");
        let blocked = timeout(Duration::from_millis(50), fetcher.fetch(request)).await;
        assert!(
            blocked.is_err(),
            "http fetch should block while the body holds the permit"
        );
    }
}
