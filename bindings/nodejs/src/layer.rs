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

use napi::bindgen_prelude::Error;
use napi::bindgen_prelude::External;
use napi::bindgen_prelude::Result;
use napi::bindgen_prelude::Status;

use crate::validate::parse_count;
use crate::validate::parse_millis;
use crate::validate::parse_timeout_millis;

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

impl NodeLayer for opendal::layers::CapabilityOverrideLayer {
    fn layer(&self, op: opendal::Operator) -> opendal::Operator {
        op.layer(self.clone())
    }
}

/// Capability override layer
///
/// Override the full capability exposed by an operator.
#[napi]
pub struct CapabilityOverrideLayer {
    overrides: String,
}

#[napi]
impl CapabilityOverrideLayer {
    /// Create a new CapabilityOverrideLayer from capability override entries.
    #[napi(constructor)]
    pub fn new(overrides: String) -> Self {
        Self { overrides }
    }

    #[napi]
    pub fn build(&self) -> Result<External<Layer>> {
        let l = opendal::layers::CapabilityOverrideLayer::from_overrides(&self.overrides)
            .map_err(|err| napi::Error::from_reason(format!("{err}")))?;

        Ok(External::new(Layer { inner: Box::new(l) }))
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
    max_times: Option<usize>,
    factor: Option<f32>,
    max_delay: Option<Duration>,
    min_delay: Option<Duration>,
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
    ///
    /// The value must be an integer between 0 and 4294967295. `0` disables
    /// retries.
    #[napi(setter)]
    pub fn max_times(&mut self, v: f64) -> Result<()> {
        self.max_times = Some(parse_count(v, "maxTimes", 0)?);
        Ok(())
    }

    /// Set factor of current backoff.
    ///
    /// The value must be a finite number between `1.0` and `340282350000000000000000000000000000000`.
    #[napi(setter)]
    pub fn factor(&mut self, v: f64) -> Result<()> {
        if !(1.0..=f64::from(f32::MAX)).contains(&v) {
            return Err(Error::new(
                Status::InvalidArg,
                format!(
                    "factor must be a finite number between 1.0 and {}",
                    f32::MAX
                ),
            ));
        }

        self.factor = Some(v as f32);
        Ok(())
    }

    /// Set max_delay of current backoff.
    ///
    /// Delay will not increase if the current delay is larger than max_delay.
    ///
    /// # Notes
    ///
    /// - The unit of max_delay is millisecond.
    /// - The value must be a finite, non-negative number.
    #[napi(setter)]
    pub fn max_delay(&mut self, v: f64) -> Result<()> {
        self.max_delay = Some(parse_millis(v, "maxDelay")?);
        Ok(())
    }

    /// Set min_delay of current backoff.
    ///
    /// # Notes
    ///
    /// - The unit of min_delay is millisecond.
    /// - The value must be a finite, non-negative number.
    #[napi(setter)]
    pub fn min_delay(&mut self, v: f64) -> Result<()> {
        self.min_delay = Some(parse_millis(v, "minDelay")?);
        Ok(())
    }

    #[napi]
    pub fn build(&self) -> External<Layer> {
        let mut l = opendal::layers::RetryLayer::default();
        if self.jitter {
            l = l.with_jitter();
        }
        if let Some(max_times) = self.max_times {
            l = l.with_max_times(max_times);
        }
        if let Some(factor) = self.factor {
            l = l.with_factor(factor);
        }
        if let Some(max_delay) = self.max_delay {
            l = l.with_max_delay(max_delay);
        }
        if let Some(min_delay) = self.min_delay {
            l = l.with_min_delay(min_delay);
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
    permits: usize,
    http_permits: Option<usize>,
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
    ///
    /// # Errors
    ///
    /// Returns an error if `permits` is not an integer between 1 and 4294967295.
    #[napi(constructor)]
    pub fn new(permits: f64) -> Result<Self> {
        Ok(Self {
            permits: parse_count(permits, "permits", 1)?,
            http_permits: None,
        })
    }

    /// Set a concurrent limit for HTTP requests.
    ///
    /// This will limit the number of concurrent HTTP requests made by the operator.
    ///
    /// # Arguments
    ///
    /// * `v` - The maximum number of concurrent HTTP requests allowed.
    ///
    /// # Errors
    ///
    /// Returns an error if `v` is not an integer between 1 and 4294967295.
    #[napi(setter)]
    pub fn http_permits(&mut self, v: f64) -> Result<()> {
        self.http_permits = Some(parse_count(v, "httpPermits", 1)?);
        Ok(())
    }

    /// Build the layer.
    ///
    /// Returns an `External<Layer>` that can be used with `Operator.layer()`.
    #[napi]
    pub fn build(&self) -> External<Layer> {
        let mut l = opendal::layers::ConcurrentLimitLayer::new(self.permits);

        if let Some(http_permits) = self.http_permits {
            l = l.with_http_concurrent_limit(http_permits);
        }

        External::new(Layer { inner: Box::new(l) })
    }
}

impl NodeLayer for opendal::layers::TimeoutLayer {
    fn layer(&self, op: opendal::Operator) -> opendal::Operator {
        op.layer(self.clone())
    }
}

/// Timeout layer
///
/// Add timeout for every operation to avoid slow or unexpected hang operations.
///
/// # Notes
///
/// `TimeoutLayer` treats all operations in two kinds:
///
/// - Non IO Operation like `stat`, `delete` they operate on a single file. We control
///   them by setting `timeout`.
/// - IO Operation like `read`, `Reader::read` and `Writer::write`, they operate on data directly, we
///   control them by setting `io_timeout`.
///
/// # Default
///
/// - timeout: 60 seconds
/// - io_timeout: 10 seconds
///
/// # Examples
///
/// ```javascript
/// const op = new Operator("fs", { root: "/tmp" })
///
/// const timeout = new TimeoutLayer();
/// timeout.timeout = 10000;      // 10 seconds for non-IO ops (in milliseconds)
/// timeout.ioTimeout = 3000;     // 3 seconds for IO ops (in milliseconds)
///
/// op.layer(timeout.build());
/// ```
#[derive(Default)]
#[napi]
pub struct TimeoutLayer {
    timeout: Option<Duration>,
    io_timeout: Option<Duration>,
}

#[napi]
impl TimeoutLayer {
    #[napi(constructor)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set timeout for non-IO operations (stat, delete, etc.)
    ///
    /// # Notes
    ///
    /// - The unit is millisecond.
    /// - Default is 60000ms (60 seconds).
    /// - The value must be finite and greater than 0. A zero timeout is
    ///   rejected because it would time out every operation immediately.
    #[napi(setter)]
    pub fn timeout(&mut self, v: f64) -> Result<()> {
        self.timeout = Some(parse_timeout_millis(v, "timeout")?);
        Ok(())
    }

    /// Set timeout for IO operations (read, write, etc.)
    ///
    /// # Notes
    ///
    /// - The unit is millisecond.
    /// - Default is 10000ms (10 seconds).
    /// - The value must be finite and greater than 0. A zero timeout is
    ///   rejected because it would time out every operation immediately.
    #[napi(setter)]
    pub fn io_timeout(&mut self, v: f64) -> Result<()> {
        self.io_timeout = Some(parse_timeout_millis(v, "ioTimeout")?);
        Ok(())
    }

    #[napi]
    pub fn build(&self) -> External<Layer> {
        let mut l = opendal::layers::TimeoutLayer::default();

        if let Some(timeout) = self.timeout {
            l = l.with_timeout(timeout);
        }
        if let Some(io_timeout) = self.io_timeout {
            l = l.with_io_timeout(io_timeout);
        }

        External::new(Layer { inner: Box::new(l) })
    }
}

impl NodeLayer for opendal::layers::LoggingLayer {
    fn layer(&self, op: opendal::Operator) -> opendal::Operator {
        op.layer(opendal::layers::LoggingLayer::default())
    }
}

/// Logging layer
///
/// Add log for every operation.
///
/// # Logging
///
/// - OpenDAL will log in structural way.
/// - Every operation will start with a `started` log entry.
/// - Every operation will finish with the following status:
///   - `succeeded`: the operation is successful, but might have more to take.
///   - `finished`: the whole operation is finished.
///   - `failed`: the operation returns an unexpected error.
/// - The default log level while expected error happened is `Warn`.
/// - The default log level while unexpected failure happened is `Error`.
///
/// # Examples
///
/// ```javascript
/// const op = new Operator("fs", { root: "/tmp" })
///
/// const logging = new LoggingLayer();
/// op.layer(logging.build());
/// ```
///
/// # Output
///
/// To enable logging output, set the `RUST_LOG` environment variable:
///
/// ```shell
/// RUST_LOG=debug node app.js
/// ```
#[napi]
pub struct LoggingLayer;

impl Default for LoggingLayer {
    fn default() -> Self {
        Self::new()
    }
}

#[napi]
impl LoggingLayer {
    #[napi(constructor)]
    pub fn new() -> Self {
        Self
    }

    #[napi]
    pub fn build(&self) -> External<Layer> {
        let l = opendal::layers::LoggingLayer::default();
        External::new(Layer { inner: Box::new(l) })
    }
}

impl NodeLayer for opendal::layers::ThrottleLayer {
    fn layer(&self, op: opendal::Operator) -> opendal::Operator {
        op.layer(self.clone())
    }
}

/// Throttle layer
///
/// Add a bandwidth rate limiter to the underlying services.
///
/// # Throttle
///
/// There are several algorithms when it come to rate limiting techniques.
/// This throttle layer uses Generic Cell Rate Algorithm (GCRA) provided by Governor.
/// By setting the `bandwidth` and `burst`, we can control the byte flow rate of underlying services.
///
/// # Note
///
/// When setting the ThrottleLayer, always consider the largest possible operation size as the burst size,
/// as **the burst size should be larger than any possible byte length to allow it to pass through**.
///
/// # Examples
///
/// This example limits bandwidth to 10 KiB/s and burst size to 10 MiB.
///
/// ```javascript
/// const op = new Operator("fs", { root: "/tmp" })
///
/// const throttle = new ThrottleLayer(10 * 1024, 10000 * 1024);
/// op.layer(throttle.build());
/// ```
#[napi]
pub struct ThrottleLayer {
    bandwidth: u32,
    burst: u32,
}

#[napi]
impl ThrottleLayer {
    /// Create a new `ThrottleLayer` with given bandwidth and burst.
    ///
    /// # Arguments
    ///
    /// - `bandwidth`: the maximum number of bytes allowed to pass through per second.
    /// - `burst`: the maximum number of bytes allowed to pass through at once.
    ///
    /// # Notes
    ///
    /// `bandwidth` and `burst` must be integers between 1 and 4294967295.
    /// Fractional values, and values that N-API would truncate or wrap, are
    /// rejected instead of silently changing the rate limit.
    #[napi(constructor)]
    pub fn new(bandwidth: f64, burst: f64) -> Result<Self> {
        // `parse_count` bounds both values to `u32::MAX`, so the casts are exact.
        Ok(Self {
            bandwidth: parse_count(bandwidth, "bandwidth", 1)? as u32,
            burst: parse_count(burst, "burst", 1)? as u32,
        })
    }

    #[napi]
    pub fn build(&self) -> External<Layer> {
        let l = opendal::layers::ThrottleLayer::new(self.bandwidth, self.burst);
        External::new(Layer { inner: Box::new(l) })
    }
}
