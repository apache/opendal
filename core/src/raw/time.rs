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

//! Time related utils.

use crate::*;
use std::fmt;
use std::ops::{Add, AddAssign, Sub, SubAssign};
use std::str::FromStr;
use std::time::{Duration, SystemTime};

/// An instant in time represented as the number of nanoseconds since the Unix epoch.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(jiff::Timestamp);

impl FromStr for Timestamp {
    type Err = Error;

    /// Parse a timestamp by the default [`DateTimeParser`].
    ///
    /// All of them are valid time:
    ///
    /// - `2022-03-13T07:20:04Z`
    /// - `2022-03-01T08:12:34+00:00`
    /// - `2022-03-01T08:12:34.00+00:00`
    /// - `2022-07-08T02:14:07+02:00[Europe/Paris]`
    ///
    /// [`DateTimeParser`]: jiff::fmt::temporal::DateTimeParser
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.parse() {
            Ok(t) => Ok(Timestamp(t)),
            Err(err) => Err(Error::new(
                ErrorKind::Unexpected,
                format!("parse '{s}' into timestamp failed"),
            )
            .set_source(err)),
        }
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Timestamp {
    /// The minimum timestamp value.
    pub const MIN: Self = Self(jiff::Timestamp::MIN);

    /// The maximum timestamp value.
    pub const MAX: Self = Self(jiff::Timestamp::MAX);

    /// Create the timestamp of now.
    pub fn now() -> Self {
        Self(jiff::Timestamp::now())
    }

    /// Format the timestamp into date: `20220301`
    pub fn format_date(self) -> String {
        self.0.strftime("%Y%m%d").to_string()
    }

    /// Format the timestamp into ISO8601: `20220313T072004Z`
    pub fn format_iso8601(self) -> String {
        self.0.strftime("%Y%m%dT%H%M%SZ").to_string()
    }

    /// Format the timestamp into http date: `Sun, 06 Nov 1994 08:49:37 GMT`
    ///
    /// ## Note
    ///
    /// HTTP date is slightly different from RFC2822.
    ///
    /// - Timezone is fixed to GMT.
    /// - Day must be 2 digit.
    pub fn format_http_date(self) -> String {
        self.0.strftime("%a, %d %b %Y %T GMT").to_string()
    }

    /// Format the timestamp into RFC3339 in Zulu: `2022-03-13T07:20:04Z`
    pub fn format_rfc3339_zulu(self) -> String {
        self.0.strftime("%FT%TZ").to_string()
    }

    /// Returns this timestamp as a number of seconds since the Unix epoch.
    ///
    /// This only returns the number of whole seconds. That is, if there are
    /// any fractional seconds in this timestamp, then they are truncated.
    pub fn as_second(self) -> i64 {
        self.0.as_second()
    }

    /// Returns the fractional second component of this timestamp in units of
    /// nanoseconds.
    ///
    /// It is guaranteed that this will never return a value that is greater
    /// than 1 second (or less than -1 second).
    pub fn subsec_nanosecond(self) -> i32 {
        self.0.subsec_nanosecond()
    }

    /// Convert to `SystemTime`.
    pub fn as_system_time(self) -> SystemTime {
        SystemTime::from(self.0)
    }

    /// Creates a new instant in time from the number of milliseconds elapsed
    /// since the Unix epoch.
    ///
    /// When `millisecond` is negative, it corresponds to an instant in time
    /// before the Unix epoch. A smaller number corresponds to an instant in
    /// time further into the past.
    pub fn from_millisecond(millis: i64) -> Result<Self> {
        match jiff::Timestamp::from_millisecond(millis) {
            Ok(t) => Ok(Timestamp(t)),
            Err(err) => Err(Error::new(
                ErrorKind::Unexpected,
                format!("convert '{millis}' milliseconds into timestamp failed"),
            )
            .set_source(err)),
        }
    }

    /// Creates a new instant in time from the number of seconds elapsed since
    /// the Unix epoch.
    ///
    /// When `second` is negative, it corresponds to an instant in time before
    /// the Unix epoch. A smaller number corresponds to an instant in time
    /// further into the past.
    pub fn from_second(second: i64) -> Result<Self> {
        match jiff::Timestamp::from_second(second) {
            Ok(t) => Ok(Timestamp(t)),
            Err(err) => Err(Error::new(
                ErrorKind::Unexpected,
                format!("convert '{second}' seconds into timestamp failed"),
            )
            .set_source(err)),
        }
    }

    /// Parse a timestamp from RFC2822.
    ///
    /// All of them are valid time:
    ///
    /// - `Sat, 13 Jul 2024 15:09:59 -0400`
    /// - `Mon, 15 Aug 2022 16:50:12 GMT`
    pub fn parse_rfc2822(s: &str) -> Result<Timestamp> {
        match jiff::fmt::rfc2822::parse(s) {
            Ok(zoned) => Ok(Timestamp(zoned.timestamp())),
            Err(err) => Err(Error::new(
                ErrorKind::Unexpected,
                format!("parse '{s}' into rfc2822 failed"),
            )
            .set_source(err)),
        }
    }

    /// Parse the string format "2023-10-31 21:59:10.000000".
    pub fn parse_datetime_utc(s: &str) -> Result<Timestamp> {
        let dt = s.parse::<jiff::civil::DateTime>().map_err(|err| {
            Error::new(
                ErrorKind::Unexpected,
                format!("parse '{s}' into datetime failed"),
            )
            .set_source(err)
        })?;

        let ts = jiff::tz::TimeZone::UTC.to_timestamp(dt).map_err(|err| {
            Error::new(
                ErrorKind::Unexpected,
                format!("convert '{s}' into timestamp failed"),
            )
            .set_source(err)
        })?;

        Ok(Timestamp(ts))
    }

    /// Convert to inner jiff::Timestamp for compatibility.
    ///
    /// This method is provided for accessing the underlying jiff::Timestamp
    /// when needed for interoperability with jiff-specific APIs.
    pub fn into_inner(self) -> jiff::Timestamp {
        self.0
    }

    /// Convert to a Zoned datetime in the given timezone.
    pub fn to_zoned(self, tz: jiff::tz::TimeZone) -> jiff::Zoned {
        self.0.to_zoned(tz)
    }

    /// Format the timestamp using `strftime` format string.
    ///
    /// Common formats:
    /// - `"%Y-%m-%d"` - Date like `2022-03-01`
    /// - `"%a, %d %b %Y %H:%M:%S GMT"` - HTTP date
    ///
    /// For full format documentation, see [jiff::fmt::strtime](https://docs.rs/jiff/latest/jiff/fmt/strtime/index.html)
    pub fn strftime(self, format: &str) -> String {
        self.0.strftime(format).to_string()
    }
}

impl From<jiff::Timestamp> for Timestamp {
    fn from(t: jiff::Timestamp) -> Self {
        Timestamp(t)
    }
}

impl TryFrom<SystemTime> for Timestamp {
    type Error = Error;

    fn try_from(t: SystemTime) -> Result<Self> {
        jiff::Timestamp::try_from(t).map(Timestamp).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "input timestamp overflow").set_source(err)
        })
    }
}

impl Add<Duration> for Timestamp {
    type Output = Timestamp;

    fn add(self, rhs: Duration) -> Timestamp {
        let ts = self
            .0
            .checked_add(rhs)
            .expect("adding unsigned duration to timestamp overflowed");

        Timestamp(ts)
    }
}

impl AddAssign<Duration> for Timestamp {
    fn add_assign(&mut self, rhs: Duration) {
        *self = *self + rhs
    }
}

impl Sub<Duration> for Timestamp {
    type Output = Timestamp;

    fn sub(self, rhs: Duration) -> Timestamp {
        let ts = self
            .0
            .checked_sub(rhs)
            .expect("subtracting unsigned duration from timestamp overflowed");

        Timestamp(ts)
    }
}

impl SubAssign<Duration> for Timestamp {
    fn sub_assign(&mut self, rhs: Duration) {
        *self = *self - rhs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_time() -> Timestamp {
        Timestamp("2022-03-01T08:12:34Z".parse().unwrap())
    }

    #[test]
    fn test_format_date() {
        let t = test_time();
        assert_eq!("20220301", t.format_date())
    }

    #[test]
    fn test_format_iso8601() {
        let t = test_time();
        assert_eq!("20220301T081234Z", t.format_iso8601())
    }

    #[test]
    fn test_format_http_date() {
        let t = test_time();
        assert_eq!("Tue, 01 Mar 2022 08:12:34 GMT", t.format_http_date())
    }

    #[test]
    fn test_format_rfc3339() {
        let t = test_time();
        assert_eq!("2022-03-01T08:12:34Z", t.format_rfc3339_zulu())
    }

    #[test]
    fn test_parse_rfc3339() {
        let t = test_time();

        for v in [
            "2022-03-01T08:12:34Z",
            "2022-03-01T08:12:34+00:00",
            "2022-03-01T08:12:34.00+00:00",
        ] {
            assert_eq!(t, v.parse().expect("must be valid time"));
        }
    }

    #[test]
    fn test_parse_rfc2822() {
        let s = "Sat, 29 Oct 1994 19:43:31 +0000";
        let v = Timestamp::parse_rfc2822(s).unwrap();
        assert_eq!("Sat, 29 Oct 1994 19:43:31 GMT", v.format_http_date());
    }
}
