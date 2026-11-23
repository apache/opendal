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

#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, doc(auto_cfg))]
#![deny(missing_docs)]

use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;

use opendal_core::raw::{BoxedStaticFuture, Layer, Operation, Servicer};
use opendal_core::{Buffer, HttpBody, HttpTransport, HttpTransporter, OperationContext, Result};

mod bucket;
mod config;
mod service;

use bucket::Bucket;
use config::AimdConfig;
pub use config::AimdLayerBuilder;

/// Adaptively pace HTTP storage requests using additive increase and
/// multiplicative decrease (AIMD).
///
/// Each request carrying an OpenDAL [`Operation`] consumes one token from its
/// read, write, delete, or list budget. Read and stat share the read budget;
/// create_dir, write, copy, compose, rename, and restore share the write budget.
/// Unmarked requests, presigning, and non-HTTP I/O are not paced.
///
/// A window with an observed [`opendal_core::ErrorKind::RateLimited`] multiplies
/// its rate by `decrease_factor`. Otherwise, a window with HTTP responses adds
/// `additive_increment`. Empty windows leave the rate unchanged. Changes are
/// applied lazily at the next admission or feedback event, at most once per
/// window. Rates remain between `min_rate` and `max_rate`.
///
/// HTTP responses establish activity; service and I/O wrappers observe parsed
/// errors. Feedback can therefore arrive after the corresponding HTTP response
/// and only includes errors propagated to those wrappers. HTTP status codes and
/// error messages are not interpreted by this layer. Other errors pass through
/// unchanged and do not trigger a decrease.
/// Feedback is attributed to the enclosing storage operation's budget. A
/// composite operation can also send requests in other categories, such as
/// stat requests during a copy; this layer does not correlate an error with
/// the individual HTTP request that produced it.
///
/// This layer does not retry. Add it before a retry layer so it observes errors
/// from each attempt. Add layers that produce local rate-limit errors, such as
/// a bandwidth throttle, outside it to avoid treating those errors as backend
/// congestion. An inner layer that consumes errors can hide feedback.
/// Add a layer that limits HTTP concurrency before AIMD so queued requests
/// wait for admission before acquiring an HTTP concurrency permit.
///
/// Clones share all four budgets, including across operators and context
/// replacement. Separately constructed layers have independent budgets. Choose
/// sharing boundaries that match the backend's quota scope; this layer does not
/// discover quotas or coordinate across processes.
///
/// Waiting uses Tokio timers by default. Use [`AimdLayerBuilder::with_sleep`] to supply a timer.
/// Pacing happens after the service builds and signs a request. Bound operation
/// duration when queued requests could outlive a backend's signature validity period. This layer
/// limits dispatches to the next transport, not redirects or retries hidden
/// inside that transport. Transport wrappers must forward to their inner
/// transport to preserve pacing.
#[derive(Clone)]
pub struct AimdLayer {
    buckets: Arc<Buckets>,
    sleep: SleepFn,
}

type SleepFn = Arc<dyn Fn(Duration) -> BoxedStaticFuture<()> + Send + Sync>;

impl Debug for AimdLayer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AimdLayer")
            .field("buckets", &self.buckets)
            .finish_non_exhaustive()
    }
}

impl AimdLayer {
    /// Configure the rates, burst capacity, feedback window, and admission timer.
    pub fn builder() -> AimdLayerBuilder {
        AimdLayerBuilder::default()
    }
}

impl Default for AimdLayer {
    fn default() -> Self {
        Self::builder()
            .build()
            .expect("default AIMD configuration is valid")
    }
}

impl Layer for AimdLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(service::AimdService {
            inner,
            buckets: self.buckets.clone(),
        })
    }

    fn apply_context(&self, _: Servicer, inner: OperationContext) -> OperationContext {
        inner.with_http_transport(HttpTransporter::new(AimdTransport {
            inner: inner.http_transport().clone(),
            sleep: self.sleep.clone(),
            buckets: self.buckets.clone(),
        }))
    }
}

#[derive(Debug)]
struct Buckets {
    read: Arc<Bucket>,
    write: Arc<Bucket>,
    delete: Arc<Bucket>,
    list: Arc<Bucket>,
}

impl Buckets {
    fn for_operation(&self, op: Operation) -> Option<&Arc<Bucket>> {
        match op {
            Operation::Read | Operation::Stat => Some(&self.read),
            Operation::Write
            | Operation::Copy
            | Operation::Compose
            | Operation::Rename
            | Operation::CreateDir
            | Operation::Restore => Some(&self.write),
            Operation::Delete => Some(&self.delete),
            Operation::List => Some(&self.list),
            _ => None,
        }
    }
}

struct AimdTransport {
    inner: HttpTransporter,
    sleep: SleepFn,
    buckets: Arc<Buckets>,
}

impl HttpTransport for AimdTransport {
    async fn fetch(&self, req: http::Request<Buffer>) -> Result<http::Response<HttpBody>> {
        let bucket = req
            .extensions()
            .get::<Operation>()
            .and_then(|op| self.buckets.for_operation(*op));
        let Some(bucket) = bucket else {
            return self.inner.fetch(req).await;
        };
        bucket.acquire(&*self.sleep).await;
        let result = self.inner.fetch(req).await;
        if result.is_ok() {
            bucket.complete_http();
        }
        result
    }
}

#[cfg(test)]
mod tests;
