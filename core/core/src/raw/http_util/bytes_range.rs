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
    /// It better to use `BytesRange::from(1024..2048)` to construct.
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
    /// # Panics
    ///
    /// Panic if advancing the offset would overflow or if input `n` is larger
    /// than the size of the range.
    pub fn advance(&mut self, n: u64) {
        self.0 = self
            .0
            .checked_add(n)
            .expect("BytesRange::advance overflow: offset + n exceeds u64::MAX");
        self.1 = self.1.map(|size| {
            size.checked_sub(n)
                .expect("BytesRange::advance underflow: n exceeds range size")
        });
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
                Some(size) => Bound::Excluded(
                    self.0
                        .checked_add(size)
                        .expect("BytesRange::to_range overflow: offset + size exceeds u64::MAX"),
                ),
                None => Bound::Unbounded,
            },
        )
    }

    /// Convert bytes range into rust range with usize.
    pub fn to_range_as_usize(self) -> impl RangeBounds<usize> {
        let offset: usize = self
            .0
            .try_into()
            .expect("BytesRange::to_range_as_usize: offset exceeds usize::MAX");
        (
            Bound::Included(offset),
            match self.1 {
                Some(size) => {
                    let end: usize = self
                        .0
                        .checked_add(size)
                        .expect(
                            "BytesRange::to_range_as_usize overflow: offset + size exceeds u64::MAX",
                        )
                        .try_into()
                        .expect(
                            "BytesRange::to_range_as_usize: offset + size exceeds usize::MAX",
                        );
                    Bound::Excluded(end)
                }
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
            Some(size) => write!(
                f,
                "{}-{}",
                self.0,
                self.0
                    .checked_add(size - 1)
                    .ok_or(std::fmt::Error)?
            ),
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
            if end < start {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "header range is invalid: end is less than start",
                )
                .with_operation("BytesRange::from_str")
                .with_context("value", value));
            }
            Ok(BytesRange::new(start, Some(end - start + 1)))
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
            Bound::Excluded(n) => n.saturating_add(1),
            Bound::Unbounded => 0,
        };
        let size = match range.end_bound().cloned() {
            Bound::Included(n) => Some(n.saturating_add(1).saturating_sub(offset)),
            Bound::Excluded(n) => Some(n.saturating_sub(offset)),
            Bound::Unbounded => None,
        };

        BytesRange(offset, size)
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
    fn test_bytes_range_from_range_bounds() {
        assert_eq!(BytesRange::new(0, None), BytesRange::from(..));
        assert_eq!(BytesRange::new(10, None), BytesRange::from(10..));
        assert_eq!(BytesRange::new(0, Some(11)), BytesRange::from(..=10));
        assert_eq!(BytesRange::new(0, Some(10)), BytesRange::from(..10));
        assert_eq!(BytesRange::new(10, Some(10)), BytesRange::from(10..20));
        assert_eq!(BytesRange::new(10, Some(11)), BytesRange::from(10..=20));
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
    fn test_bytes_range_from_str_invalid_end_less_than_start() {
        let cases = vec!["bytes=100-50", "bytes=10-9", "bytes=1-0"];

        for input in cases {
            let result: Result<BytesRange> = input.parse();
            assert!(
                result.is_err(),
                "expected error for invalid range {input}, got {result:?}"
            );
        }
    }

    #[allow(clippy::reversed_empty_ranges)]
    #[test]
    fn test_bytes_range_from_range_bounds_underflow() {
        // Invalid ranges where end < start should produce zero-size ranges
        // rather than underflowing.
        assert_eq!(BytesRange::new(100, Some(0)), BytesRange::from(100..50));
        assert_eq!(BytesRange::new(10, Some(0)), BytesRange::from(10..=5));
        assert_eq!(BytesRange::new(5, Some(0)), BytesRange::from(5..0));
        assert_eq!(BytesRange::new(5, Some(0)), BytesRange::from(5..=0));
    }

    #[test]
    fn test_bytes_range_from_range_bounds_u64_max() {
        // Boundary cases near u64::MAX must not overflow.
        assert_eq!(
            BytesRange::new(0, Some(u64::MAX)),
            BytesRange::from(..=u64::MAX)
        );
        assert_eq!(
            BytesRange::new(0, Some(u64::MAX)),
            BytesRange::from(..u64::MAX)
        );
        assert_eq!(
            BytesRange::new(1, Some(u64::MAX.saturating_sub(1))),
            BytesRange::from(1..=u64::MAX)
        );
        // Excluded start at u64::MAX must not overflow.
        assert_eq!(
            BytesRange::new(u64::MAX, None),
            BytesRange::from((u64::MAX)..)
        );
    }

    #[test]
    fn test_bytes_range_display_overflow() {
        // offset=u64::MAX, size=2 would overflow in Display (u64::MAX + 1)
        let range = BytesRange::new(u64::MAX, Some(2));
        assert!(std::fmt::write(&mut String::new(), format_args!("{}", range)).is_err());
    }

    #[test]
    #[should_panic(expected = "BytesRange::to_range overflow")]
    fn test_bytes_range_to_range_overflow() {
        let range = BytesRange::new(u64::MAX, Some(1));
        let _ = range.to_range();
    }

    #[test]
    #[should_panic(expected = "BytesRange::to_range_as_usize")]
    fn test_bytes_range_to_range_as_usize_overflow() {
        let range = BytesRange::new(u64::MAX, Some(1));
        let _ = range.to_range_as_usize();
    }

    #[test]
    #[should_panic(expected = "BytesRange::advance overflow")]
    fn test_bytes_range_advance_offset_overflow() {
        let mut range = BytesRange::new(u64::MAX, None);
        range.advance(1);
    }

    #[test]
    #[should_panic(expected = "BytesRange::advance underflow")]
    fn test_bytes_range_advance_size_underflow() {
        let mut range = BytesRange::new(0, Some(1));
        range.advance(2);
    }
}
