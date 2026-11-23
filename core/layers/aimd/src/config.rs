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

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use crate::{AimdLayer, Bucket, Buckets, SleepFn};
use opendal_core::raw::{Instant, MaybeSend};
use opendal_core::{Error, ErrorKind, Result};

/// Configures the four independent request budgets of an [`AimdLayer`].
/// Each budget receives the same settings and tracks its own rate.
#[derive(Clone)]
pub struct AimdLayerBuilder {
    config: AimdConfig,
    initial_rate: Option<f64>,
    sleep: SleepFn,
}

impl Default for AimdLayerBuilder {
    fn default() -> Self {
        Self {
            config: AimdConfig::default(),
            initial_rate: None,
            sleep: Arc::new(|duration| Box::pin(tokio::time::sleep(duration))),
        }
    }
}

impl AimdLayerBuilder {
    /// Set the initial requests per second for each budget. Defaults to 2000,
    /// limited to the configured range. An explicit value must be in range.
    pub fn initial_rate(mut self, initial_rate: f64) -> Self {
        self.initial_rate = Some(initial_rate);
        self
    }

    /// Set the minimum requests per second for each budget. Defaults to 1.
    pub fn min_rate(mut self, min_rate: f64) -> Self {
        self.config.min_rate = min_rate;
        self
    }

    /// Set the maximum requests per second for each budget. Defaults to 5000.
    pub fn max_rate(mut self, max_rate: f64) -> Self {
        self.config.max_rate = max_rate;
        self
    }

    /// Set the rate multiplier after a throttled window. Defaults to 0.5.
    pub fn decrease_factor(mut self, decrease_factor: f64) -> Self {
        self.config.decrease_factor = decrease_factor;
        self
    }

    /// Set the requests per second added after an active window without throttling. Defaults to 300.
    pub fn additive_increment(mut self, additive_increment: f64) -> Self {
        self.config.additive_increment = additive_increment;
        self
    }

    /// Set the feedback window duration. Defaults to one second.
    pub fn window(mut self, window: Duration) -> Self {
        self.config.window = window;
        self
    }

    /// Set the token capacity for each budget. Buckets start full. Defaults to 100.
    pub fn burst(mut self, burst: u32) -> Self {
        self.config.burst = burst;
        self
    }

    /// Replace the function that creates timers for admission waits.
    ///
    /// The function must return a nonblocking future that completes after the
    /// supplied duration. Waiting may be cancelled when feedback changes or a
    /// request is dropped, so dropping the future must safely cancel the wait.
    /// The future must be `Send` on non-Wasm targets.
    ///
    /// Rate calculations use a monotonic clock independently of the sleep function.
    ///
    /// ```
    /// use opendal_layer_aimd::AimdLayer;
    ///
    /// let layer = AimdLayer::builder().with_sleep(tokio::time::sleep).build()?;
    /// # Ok::<(), opendal_core::Error>(())
    /// ```
    pub fn with_sleep<F, Fut>(mut self, sleep: F) -> Self
    where
        F: Fn(Duration) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + MaybeSend + 'static,
    {
        self.sleep = Arc::new(move |duration| Box::pin(sleep(duration)));
        self
    }

    /// Create a layer with four independent budgets using these settings.
    ///
    /// Returns [`ErrorKind::ConfigInvalid`] for nonpositive or non-finite rates,
    /// inconsistent rate bounds, a nonpositive or non-finite additive increment,
    /// a decrease factor outside `(0, 1)`, a zero burst
    /// or window, or a window or token interval outside the timer range.
    pub fn build(self) -> Result<AimdLayer> {
        let config = self.config;
        let initial_rate = self
            .initial_rate
            .unwrap_or_else(|| 2000.0_f64.max(config.min_rate).min(config.max_rate));
        config.validate(initial_rate)?;
        Ok(AimdLayer {
            sleep: self.sleep,
            buckets: Arc::new(Buckets {
                read: Arc::new(Bucket::new(config.clone(), initial_rate)),
                write: Arc::new(Bucket::new(config.clone(), initial_rate)),
                delete: Arc::new(Bucket::new(config.clone(), initial_rate)),
                list: Arc::new(Bucket::new(config, initial_rate)),
            }),
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AimdConfig {
    pub(crate) min_rate: f64,
    pub(crate) max_rate: f64,
    pub(crate) decrease_factor: f64,
    pub(crate) additive_increment: f64,
    pub(crate) window: Duration,
    pub(crate) burst: u32,
}

impl Default for AimdConfig {
    fn default() -> Self {
        Self {
            min_rate: 1.0,
            max_rate: 5000.0,
            decrease_factor: 0.5,
            additive_increment: 300.0,
            window: Duration::from_secs(1),
            burst: 100,
        }
    }
}

impl AimdConfig {
    fn validate(&self, initial_rate: f64) -> Result<()> {
        for (name, value) in [
            ("initial_rate", initial_rate),
            ("min_rate", self.min_rate),
            ("max_rate", self.max_rate),
            ("decrease_factor", self.decrease_factor),
            ("additive_increment", self.additive_increment),
        ] {
            if !value.is_finite() || value <= 0.0 {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "AIMD parameter must be finite and positive",
                )
                .with_context("parameter", name));
            }
        }
        if self.min_rate > initial_rate || initial_rate > self.max_rate {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "AIMD rates must satisfy min_rate <= initial_rate <= max_rate",
            ));
        }
        if self.decrease_factor >= 1.0 {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "AIMD decrease_factor must be less than one",
            ));
        }
        if self.window.is_zero() || self.burst == 0 {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "AIMD window and burst must be nonzero",
            ));
        }
        let now = Instant::now();
        let interval = Duration::try_from_secs_f64(1.0 / self.min_rate).ok();
        if now.checked_add(self.window).is_none()
            || interval
                .and_then(|duration| now.checked_add(duration))
                .is_none()
        {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "AIMD window and token interval must fit the timer range",
            ));
        }
        Ok(())
    }
}
