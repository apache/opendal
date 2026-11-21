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

use opendal_core::{Error, ErrorKind, Result};

/// Parameters applied independently to the read, write, delete, and list budgets.
///
/// Rates count HTTP requests per second, not bytes or returned entries.
/// [`AimdLayer::new`](crate::AimdLayer::new) validates these parameters before
/// creating the budgets.
#[derive(Clone, Debug)]
pub struct AimdConfig {
    /// Initial request rate. Defaults to 2000 requests/s.
    pub initial_rate: f64,
    /// Minimum request rate. Defaults to 1 request/s.
    pub min_rate: f64,
    /// Maximum request rate. Defaults to 5000 requests/s.
    pub max_rate: f64,
    /// Multiplier after a window observes rate limiting. Defaults to 0.5.
    pub decrease_factor: f64,
    /// Requests/s added after an active window without rate limiting. Defaults to 300.
    pub additive_increment: f64,
    /// Feedback window duration. Defaults to one second.
    pub window: Duration,
    /// Maximum accumulated tokens per budget. Defaults to 100.
    ///
    /// Buckets start full. Each admitted HTTP request consumes one token.
    pub burst: u32,
}

impl Default for AimdConfig {
    fn default() -> Self {
        Self {
            initial_rate: 2000.0,
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
    pub(crate) fn validate(&self) -> Result<()> {
        for (name, value) in [
            ("initial_rate", self.initial_rate),
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
        if self.min_rate > self.initial_rate || self.initial_rate > self.max_rate {
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
        let now = std::time::Instant::now();
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
