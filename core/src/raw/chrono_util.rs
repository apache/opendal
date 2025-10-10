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

use crate::*;
use std::time::SystemTime;

use jiff::Timestamp;

/// Parse datetime from rfc2822.
///
/// For example: `Fri, 28 Nov 2014 21:00:09 +0900`
pub fn parse_datetime_from_rfc2822(s: &str) -> Result<Timestamp> {
    match jiff::fmt::rfc2822::parse(s) {
        Ok(zoned) => Ok(zoned.timestamp()),
        Err(err) => Err(Error::new(
            ErrorKind::Unexpected,
            format!("parse '{s}' from rfc2822 failed"),
        )
        .set_source(err)),
    }
}

/// Parse datetime from rfc3339.
///
/// # Examples
///
/// With a time zone:
///
/// ```
/// use opendal::raw::parse_datetime_from_rfc3339;
/// use opendal::Error;
///
/// let date_time = parse_datetime_from_rfc3339("2014-11-28T21:00:09+09:00")?;
/// assert_eq!(date_time.date_naive().day(), 28);
/// # Ok::<(), Error>(())
/// ```
///
/// With the UTC offset of 00:00:
///
/// ```
/// # use opendal::Error;
/// # use opendal::raw::parse_datetime_from_rfc3339;
///
/// let date_time = parse_datetime_from_rfc3339("2014-11-28T21:00:09Z")?;
/// assert_eq!(date_time.hour(), 21);
/// # Ok::<(), Error>(())
/// ```
pub fn parse_datetime_from_rfc3339(s: &str) -> Result<Timestamp> {
    match s.parse() {
        Ok(t) => Ok(t),
        Err(err) => Err(Error::new(
            ErrorKind::Unexpected,
            format!("parse '{s}' into timestamp failed"),
        )
        .set_source(err)),
    }
}

/// parse datetime from given timestamp_millis
pub fn parse_datetime_from_from_timestamp_millis(millis: i64) -> Result<Timestamp> {
    Timestamp::from_millisecond(millis).map_err(|err| {
        Error::new(ErrorKind::Unexpected, "input timestamp overflow").set_source(err)
    })
}

/// parse datetime from given timestamp_secs
pub fn parse_datetime_from_from_timestamp(secs: i64) -> Result<Timestamp> {
    Timestamp::from_second(secs).map_err(|err| {
        Error::new(ErrorKind::Unexpected, "input timestamp overflow").set_source(err)
    })
}

/// parse datetime from given system time
pub fn parse_datetime_from_from_system_time(t: SystemTime) -> Result<Timestamp> {
    Timestamp::try_from(t).map_err(|err| {
        Error::new(ErrorKind::Unexpected, "input timestamp overflow").set_source(err)
    })
}

/// format datetime into http date, this format is required by:
/// https://httpwg.org/specs/rfc9110.html#field.if-modified-since
pub fn format_datetime_into_http_date(s: Timestamp) -> String {
    s.strftime("%a, %d %b %Y %H:%M:%S GMT").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_datetime_into_http_date() {
        let s = "Sat, 29 Oct 1994 19:43:31 +0000";
        let v = parse_datetime_from_rfc2822(s).unwrap();
        assert_eq!(
            format_datetime_into_http_date(v),
            "Sat, 29 Oct 1994 19:43:31 GMT"
        );
    }
}
