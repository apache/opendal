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
pub struct BytesRange(
    /// Offset of the range.
    u64,
    /// Size of the range.
    Option<u64>,
);

impl BytesRange {
    /// Create a new `BytesRange`
    ///
    /// It better to use `BytesRange::from_range(1024..2048)` to construct.
    ///
    /// # Note
    ///
    /// The behavior for `None` and `Some` of `size` is different.
    ///
    /// - size=None => `bytes=<offset>-`, read from `<offset>` until the end
    /// - size=Some(1024) => `bytes=<offset>-<offset + 1024>`, read 1024 bytes starting from the `<offset>`
    pub fn new(offset: u64, size: Option<u64>) -> Self {
        BytesRange(offset, size)
    }

    /// Get offset of BytesRange.
    pub fn offset(&self) -> u64 {
        self.0
    }

    /// Get size of BytesRange.
    pub fn size(&self) -> Option<u64> {
        self.1
    }

    /// Advance the range by `n` bytes.
    ///
    /// Returns an error if `n` is larger than the size of the range.
    pub fn advance(&mut self, n: u64) -> Result<()> {
        if let Some(size) = self.1 {
            if n > size {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("cannot advance BytesRange by {n} bytes, only {size} bytes left"),
                ));
            }
        }
        self.0 += n;
        self.1 = self.1.map(|size| size - n);
        Ok(())
    }

    /// Check if this range is full of this content.
    ///
    /// If this range is full, we don't need to specify it in http request.
    pub fn is_full(&self) -> bool {
        self.0 == 0 && self.1.is_none()
    }

    /// Convert bytes range into Range header.
    pub fn to_header(&self) -> String {
        format!("bytes={self}")
    }

    /// Convert bytes range into rust range.
    pub fn to_range(&self) -> impl RangeBounds<u64> {
        (
            Bound::Included(self.0),
            match self.1 {
                Some(size) => Bound::Excluded(self.0 + size),
                None => Bound::Unbounded,
            },
        )
    }

    /// Convert bytes range into rust range with usize.
    pub fn to_range_as_usize(self) -> impl RangeBounds<usize> {
        (
            Bound::Included(self.0 as usize),
            match self.1 {
                Some(size) => Bound::Excluded((self.0 + size) as usize),
                None => Bound::Unbounded,
            },
        )
    }
}

impl Display for BytesRange {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.1 {
            None => write!(f, "{}-", self.0),
            // A zero-size range can't be represented as a valid HTTP Range.
            Some(0) => Err(std::fmt::Error),
            Some(size) => write!(f, "{}-{}", self.0, self.0 + size - 1),
        }
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
            Ok(BytesRange::new(
                v[0].parse().map_err(parse_int_error)?,
                None,
            ))
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
            BytesRange::from_range(start..=end)
        }
    }
}

impl BytesRange {
    /// Create a `BytesRange` from any `RangeBounds<u64>`.
    ///
    /// Returns an error if the end bound is before the start bound.
    pub fn from_range(range: impl RangeBounds<u64>) -> Result<Self> {
        let offset = match range.start_bound().cloned() {
            Bound::Included(n) => n,
            Bound::Excluded(n) => n + 1,
            Bound::Unbounded => 0,
        };
        let size = match range.end_bound().cloned() {
            Bound::Included(n) => {
                if n + 1 < offset {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        format!("invalid range: inclusive end ({n}) is before start ({offset})"),
                    ));
                }
                Some(n + 1 - offset)
            }
            Bound::Excluded(n) => {
                if n < offset {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        format!("invalid range: exclusive end ({n}) is before start ({offset})"),
                    ));
                }
                Some(n - offset)
            }
            Bound::Unbounded => None,
        };

        Ok(BytesRange(offset, size))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_range_display_zero_size() {
        // Zero-size at offset 0
        let range = BytesRange::new(0, Some(0));
        assert!(std::fmt::write(&mut String::new(), format_args!("{}", range)).is_err());

        // Zero-size at nonzero offset
        let range = BytesRange::new(5, Some(0));
        assert!(std::fmt::write(&mut String::new(), format_args!("{}", range)).is_err());
    }

    #[test]
    fn test_bytes_range_to_string() {
        let h = BytesRange::new(0, Some(1024));
        assert_eq!(h.to_string(), "0-1023");

        let h = BytesRange::new(1024, None);
        assert_eq!(h.to_string(), "1024-");

        let h = BytesRange::new(1024, Some(1024));
        assert_eq!(h.to_string(), "1024-2047");
    }

    #[test]
    fn test_bytes_range_to_header() {
        let h = BytesRange::new(0, Some(1024));
        assert_eq!(h.to_header(), "bytes=0-1023");

        let h = BytesRange::new(1024, None);
        assert_eq!(h.to_header(), "bytes=1024-");

        let h = BytesRange::new(1024, Some(1024));
        assert_eq!(h.to_header(), "bytes=1024-2047");
    }

    #[test]
    fn test_bytes_range_from_range_bounds() -> Result<()> {
        assert_eq!(BytesRange::new(0, None), BytesRange::from_range(..)?);
        assert_eq!(BytesRange::new(10, None), BytesRange::from_range(10..)?);
        assert_eq!(BytesRange::new(0, Some(11)), BytesRange::from_range(..=10)?);
        assert_eq!(BytesRange::new(0, Some(10)), BytesRange::from_range(..10)?);
        assert_eq!(
            BytesRange::new(10, Some(10)),
            BytesRange::from_range(10..20)?
        );
        assert_eq!(
            BytesRange::new(10, Some(11)),
            BytesRange::from_range(10..=20)?
        );

        Ok(())
    }

    #[test]
    fn test_bytes_range_from_str() -> Result<()> {
        let cases = vec![
            ("range-start", "bytes=123-", BytesRange::new(123, None)),
            ("range", "bytes=123-124", BytesRange::new(123, Some(2))),
            ("one byte", "bytes=0-0", BytesRange::new(0, Some(1))),
            (
                "lower case header",
                "bytes=0-0",
                BytesRange::new(0, Some(1)),
            ),
        ];

        for (name, input, expected) in cases {
            let actual = input.parse()?;

            assert_eq!(expected, actual, "{name}")
        }

        Ok(())
    }

    #[test]
    fn test_bytes_range_from_str_inverted_returns_error() {
        let result = "bytes=100-50".parse::<BytesRange>();
        assert!(result.is_err(), "inverted range header should return Err");
    }

    #[test]
    fn test_bytes_range_advance_beyond_size_returns_error() {
        let mut r = BytesRange::new(0, Some(3));
        let err = r.advance(5).unwrap_err();
        assert!(
            err.to_string()
                .contains("cannot advance BytesRange by 5 bytes, only 3 bytes left")
        );
    }
}
