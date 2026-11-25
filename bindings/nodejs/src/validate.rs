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
use napi::bindgen_prelude::Result;
use napi::bindgen_prelude::Status;

/// Parse a JavaScript count such as a concurrency limit or a retry count.
///
/// N-API hands numbers over as `f64` and truncates or wraps them when it
/// converts to a fixed-width integer, so a fractional `1.5` used to become `1`
/// and a negative `-1` used to become the largest possible count. Accept only
/// integers within `min..=u32::MAX` instead of silently changing the request.
pub(crate) fn parse_count(value: f64, field: &str, min: u32) -> Result<usize> {
    // The range comparison also rejects `NaN` and both infinities.
    if !(f64::from(min)..=f64::from(u32::MAX)).contains(&value) || value.fract() != 0.0 {
        return Err(Error::new(
            Status::InvalidArg,
            format!("{field} must be an integer between {min} and {}", u32::MAX),
        ));
    }

    Ok(value as usize)
}

/// Parse the `expires` argument of a presign call, which is given in seconds.
///
/// The value must be a whole number of seconds between 1 and 4294967295.
/// N-API used to wrap a negative value into a request that stays valid for
/// more than a century, so an invalid expiry is reported instead.
pub(crate) fn parse_expires_secs(value: f64) -> Result<Duration> {
    let seconds = parse_count(value, "expires", 1)?;
    Ok(Duration::from_secs(seconds as u64))
}

/// Convert milliseconds while preserving fractional precision and legacy saturation.
pub(crate) fn duration_from_millis(value: f64) -> Duration {
    Duration::try_from_secs_f64(value / 1000.0)
        .unwrap_or_else(|_| Duration::from_millis(value as u64))
}
