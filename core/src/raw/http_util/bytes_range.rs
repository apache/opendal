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

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::ops::Bound;
use std::ops::RangeBounds;
use std::str::FromStr;

use crate::*;

/// BytesRange(offset, size) carries a range of content.
///
/// BytesRange implements `ToString` which can be used as `Range` HTTP header directly.
///
/// `<unit>` should always be `bytes`.
///
/// ```text
/// Range: bytes=<range-start>-
/// Range: bytes=<range-start>-<range-end>
/// ```
///
/// # Notes
///
/// We don't support tailing read like `Range: bytes=-<range-end>`
#[derive(Default, Debug, Clone, Copy, Eq, PartialEq)]
pub struct BytesRange {
    /// Offset of the range.
    offset: u64,
    /// Size of the range.
    size: u64,
}

impl BytesRange {
    /// Create a new `BytesRange`
    ///
    /// It better to use `BytesRange::from(1024..2048)` to construct.
    ///
    /// # Note
    ///
    /// The behavior for `None` and `Some(0)` is different.
    ///
    /// - offset=None => `bytes=-<size>`, read `<size>` bytes from end.
    /// - offset=Some(0) => `bytes=0-<size>`, read `<size>` bytes from start.
    pub fn new(offset: u64, size: u64) -> Self {
        BytesRange { offset, size }
    }

    /// Get offset of BytesRange.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Get size of BytesRange.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Advance the range by `n` bytes.
    ///
    /// # Panics
    ///
    /// Panic if input `n` is larger than the size of the range.
    pub fn advance(&mut self, n: u64) {
        self.offset += n;
        self.size = self.size - n;
    }

    /// Check if this range is full of this content.
    ///
    /// If this range is full, we don't need to specify it in http request.
    pub fn is_full(&self) -> bool {
        self.offset == 0 && self.size == 0
    }

    /// Convert bytes range into Range header.
    pub fn to_header(&self) -> String {
        format!("bytes={self}")
    }

    /// Convert bytes range into rust range.
    pub fn to_range(&self) -> impl RangeBounds<u64> {
        (
            Bound::Included(self.offset),
            Bound::Excluded(self.offset + self.size),
        )
    }

    /// Convert bytes range into rust range with usize.
    pub(crate) fn to_range_as_usize(self) -> impl RangeBounds<usize> {
        (
            Bound::Included(self.offset as usize),
            Bound::Excluded((self.offset + self.size) as usize),
        )
    }
}

impl Display for BytesRange {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.offset, self.offset + self.offset - 1)
    }
}

impl FromStr for BytesRange {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        let s = value.strip_prefix("bytes=").ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "header range is invalid")
                .with_operation("BytesRange::from_str")
                .with_context("value", value)
        })?;

        if s.contains(',') {
            return Err(Error::new(ErrorKind::Unexpected, "header range is invalid")
                .with_operation("BytesRange::from_str")
                .with_context("value", value));
        }

        let v = s.split('-').collect::<Vec<_>>();
        if v.len() != 2 {
            return Err(Error::new(ErrorKind::Unexpected, "header range is invalid")
                .with_operation("BytesRange::from_str")
                .with_context("value", value));
        }

        let parse_int_error = |e: std::num::ParseIntError| {
            Error::new(ErrorKind::Unexpected, "header range is invalid")
                .with_operation("BytesRange::from_str")
                .with_context("value", value)
                .set_source(e)
        };

        if v[1].is_empty() {
            // <range-start>-
            Err(Error::new(
                ErrorKind::Unexpected,
                "end range with tailing is not supported",
            )
            .with_operation("BytesRange::from_str")
            .with_context("value", value))
        } else if v[0].is_empty() {
            // -<suffix-length>
            Err(Error::new(
                ErrorKind::Unexpected,
                "header range with tailing is not supported",
            )
            .with_operation("BytesRange::from_str")
            .with_context("value", value))
        } else {
            // <range-start>-<range-end>
            let start: u64 = v[0].parse().map_err(parse_int_error)?;
            let end: u64 = v[1].parse().map_err(parse_int_error)?;
            Ok(BytesRange::new(start, end - start + 1))
        }
    }
}

impl<T> From<T> for BytesRange
where
    T: RangeBounds<u64>,
{
    fn from(range: T) -> Self {
        let offset = match range.start_bound().cloned() {
            Bound::Included(n) => n,
            Bound::Excluded(n) => n + 1,
            Bound::Unbounded => 0,
        };

        let size = match range.end_bound().cloned() {
            Bound::Included(n) => n - offset + 1,
            Bound::Excluded(n) => n - offset,
            Bound::Unbounded => panic!("end of range must be specified"),
        };

        BytesRange { offset, size }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_range_to_string() {
        let h = BytesRange::new(0, 1024);
        assert_eq!(h.to_string(), "0-1023");

        let h = BytesRange::new(1024, 1024);
        assert_eq!(h.to_string(), "1024-2047");
    }

    #[test]
    fn test_bytes_range_to_header() {
        let h = BytesRange::new(0, 1024);
        assert_eq!(h.to_header(), "bytes=0-1023");

        let h = BytesRange::new(1024, 1024);
        assert_eq!(h.to_header(), "bytes=1024-2047");
    }

    #[test]
    fn test_bytes_range_from_range_bounds() {
        assert_eq!(BytesRange::new(0, 11), BytesRange::from(..=10));
        assert_eq!(BytesRange::new(0, 10), BytesRange::from(..10));
        assert_eq!(BytesRange::new(10, 10), BytesRange::from(10..20));
        assert_eq!(BytesRange::new(10, 11), BytesRange::from(10..=20));
    }

    #[test]
    fn test_bytes_range_from_str() -> Result<()> {
        let cases = vec![
            ("range", "bytes=123-124", BytesRange::new(123, 2)),
            ("one byte", "bytes=0-0", BytesRange::new(0, 1)),
            ("lower case header", "bytes=0-0", BytesRange::new(0, 1)),
        ];

        for (name, input, expected) in cases {
            let actual = input.parse()?;
            assert_eq!(expected, actual, "{name}")
        }

        Ok(())
    }
}
