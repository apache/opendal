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
use std::time::UNIX_EPOCH;

use chrono::DateTime;
use chrono::Utc;

use crate::*;

/// Parse dateimt from rfc2822.
///
/// For example: `Fri, 28 Nov 2014 21:00:09 +0900`
pub fn parse_datetime_from_rfc2822(s: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc2822(s)
        .map(|v| v.into())
        .map_err(|e| {
            Error::new(ErrorKind::Unexpected, "parse datetime from rfc2822 failed").set_source(e)
        })
}

/// Parse dateimt from rfc3339.
///
/// For example: `2014-11-28T21:00:09+09:00`
pub fn parse_datetime_from_rfc3339(s: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|v| v.into())
        .map_err(|e| {
            Error::new(ErrorKind::Unexpected, "parse datetime from rfc3339 failed").set_source(e)
        })
}

/// parse datetime from given timestamp_millis
pub fn parse_datetime_from_from_timestamp_millis(s: i64) -> Result<DateTime<Utc>> {
    let st = UNIX_EPOCH
        .checked_add(Duration::from_millis(s as u64))
        .ok_or_else(|| Error::new(ErrorKind::Unexpected, "input timestamp overflow"))?;

    Ok(st.into())
}

/// parse datetime from given timestamp
pub fn parse_datetime_from_from_timestamp(s: i64) -> Result<DateTime<Utc>> {
    let st = UNIX_EPOCH
        .checked_add(Duration::from_secs(s as u64))
        .ok_or_else(|| Error::new(ErrorKind::Unexpected, "input timestamp overflow"))?;

    Ok(st.into())
}
