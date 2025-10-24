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

use std::time::Duration;

use napi::bindgen_prelude::External;

pub trait NodeLayer: Send + Sync {
    fn layer(&self, op: opendal::Operator) -> opendal::Operator;
}

/// A public layer wrapper
#[napi]
pub struct Layer {
    pub(crate) inner: Box<dyn NodeLayer>,
}

impl NodeLayer for opendal::layers::RetryLayer {
    fn layer(&self, op: opendal::Operator) -> opendal::Operator {
        op.layer(self.clone())
    }
}

/// Retry layer
///
/// Add retry for temporary failed operations.
///
/// # Notes
///
/// This layer will retry failed operations when [`Error::is_temporary`]
/// returns true.
/// If the operation still failed, this layer will set error to
/// `Persistent` which means error has been retried.
///
/// `write` and `blocking_write` don't support retry so far,
/// visit [this issue](https://github.com/apache/opendal/issues/1223) for more details.
///
/// # Examples
///
/// ```javascript
/// const op = new Operator("fs", { root: "/tmp" })
///
/// const retry = new RetryLayer();
/// retry.max_times = 3;
/// retry.jitter = true;
///
/// op.layer(retry.build());
/// ```
#[derive(Default)]
#[napi]
pub struct RetryLayer {
    jitter: bool,
    max_times: Option<u32>,
    factor: Option<f64>,
    max_delay: Option<f64>,
    min_delay: Option<f64>,
}

#[napi]
impl RetryLayer {
    #[napi(constructor)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set jitter of current backoff.
    ///
    /// If jitter is enabled, ExponentialBackoff will add a random jitter in `[0, min_delay)`
    /// to current delay.
    #[napi(setter)]
    pub fn jitter(&mut self, v: bool) {
        self.jitter = v;
    }

    /// Set max_times of current backoff.
    ///
    /// Backoff will return `None` if max times are reached.
    #[napi(setter)]
    pub fn max_times(&mut self, v: u32) {
        self.max_times = Some(v);
    }

    /// Set factor of current backoff.
    ///
    /// # Panics
    ///
    /// This function will panic if the input factor is smaller than `1.0`.
    #[napi(setter)]
    pub fn factor(&mut self, v: f64) {
        self.factor = Some(v);
    }

    /// Set max_delay of current backoff.
    ///
    /// Delay will not increase if the current delay is larger than max_delay.
    ///
    /// # Notes
    ///
    /// - The unit of max_delay is millisecond.
    #[napi(setter)]
    pub fn max_delay(&mut self, v: f64) {
        self.max_delay = Some(v);
    }

    /// Set min_delay of current backoff.
    ///
    /// # Notes
    ///
    /// - The unit of min_delay is millisecond.
    #[napi(setter)]
    pub fn min_delay(&mut self, v: f64) {
        self.min_delay = Some(v);
    }

    #[napi]
    pub fn build(&self) -> External<Layer> {
        let mut l = opendal::layers::RetryLayer::default();
        if self.jitter {
            l = l.with_jitter();
        }
        if let Some(max_times) = self.max_times {
            l = l.with_max_times(max_times as usize);
        }
        if let Some(factor) = self.factor {
            l = l.with_factor(factor as f32);
        }
        if let Some(max_delay) = self.max_delay {
            l = l.with_max_delay(Duration::from_millis(max_delay as u64));
        }
        if let Some(min_delay) = self.min_delay {
            l = l.with_min_delay(Duration::from_millis(min_delay as u64));
        }

        External::new(Layer { inner: Box::new(l) })
    }
}

impl NodeLayer for opendal::layers::ConcurrentLimitLayer {
    fn layer(&self, op: opendal::Operator) -> opendal::Operator {
        op.layer(self.clone())
    }
}

/// Concurrent limit layer
///
/// Add concurrent request limit.
///
/// # Notes
///
/// Users can control how many concurrent connections could be established
/// between OpenDAL and underlying storage services.
///
/// All operators wrapped by this layer will share a common semaphore.
///
/// # Examples
///
/// ```javascript
/// const op = new Operator("fs", { root: "/tmp" })
///
/// // Create a concurrent limit layer with 1024 permits
/// const limit = new ConcurrentLimitLayer(1024);
/// op.layer(limit.build());
/// ```
///
/// With HTTP concurrent limit:
///
/// ```javascript
/// const limit = new ConcurrentLimitLayer(1024);
/// limit.httpPermits = 512;
/// op.layer(limit.build());
/// ```
#[napi]
pub struct ConcurrentLimitLayer {
    permits: i64,
    http_permits: Option<i64>,
}

#[napi]
impl ConcurrentLimitLayer {
    /// Create a new ConcurrentLimitLayer with specified permits.
    ///
    /// This permits will be applied to all operations.
    ///
    /// # Arguments
    ///
    /// * `permits` - The maximum number of concurrent operations allowed.
    #[napi(constructor)]
    pub fn new(permits: i64) -> Self {
        Self {
            permits,
            http_permits: None,
        }
    }

    /// Set a concurrent limit for HTTP requests.
    ///
    /// This will limit the number of concurrent HTTP requests made by the operator.
    ///
    /// # Arguments
    ///
    /// * `v` - The maximum number of concurrent HTTP requests allowed.
    #[napi(setter)]
    pub fn http_permits(&mut self, v: i64) {
        self.http_permits = Some(v);
    }

    /// Build the layer.
    ///
    /// Returns an `External<Layer>` that can be used with `Operator.layer()`.
    #[napi]
    pub fn build(&self) -> External<Layer> {
        let permits = self.permits;
        let mut l = opendal::layers::ConcurrentLimitLayer::new(permits as usize);

        if let Some(http_permits) = self.http_permits {
            l = l.with_http_concurrent_limit(http_permits as usize);
        }

        External::new(Layer { inner: Box::new(l) })
    }
}
