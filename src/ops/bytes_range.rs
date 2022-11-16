// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::ops::Bound;
use std::ops::RangeBounds;
use std::str::FromStr;

use anyhow::anyhow;

/// BytesRange(offset, size) carries a range of content.
///
/// BytesRange implements `ToString` which can be used as `Range` HTTP header directly.
///
/// <unit> should always be `bytes`.
///
/// ```text
/// Range: bytes=<range-start>-
/// Range: bytes=<range-start>-<range-end>
/// Range: bytes=-<suffix-length>
/// ```
///
/// # Notes
///
/// BytesRange support constuct via rust native range syntex like `..`, `1024..`, `..2048`.
/// But it's has different symantic on `RangeTo`: `..<end>`.
/// In rust, `..<end>` means all items that `< end`, but in BytesRange, `..<end>` means the
/// tailing part of content, a.k.a, the last `<end>` bytes of content.
///
/// - `0..1024` will be converted to header `range: bytes=0-1024`
/// - `..1024` will be converted to header `range: bytes=-1024`
#[derive(Default, Debug, Clone, Copy, Eq, PartialEq)]
pub struct BytesRange(
    /// Offset of the range.
    Option<u64>,
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
    /// The behavior for `None` and `Some(0)` is different.
    ///
    /// - offset=None => `bytes=-<size>`, read <size> bytes from end.
    /// - offset=Some(0) => `bytes=0-<size>`, read <size> bytes from start.
    pub fn new(offset: Option<u64>, size: Option<u64>) -> Self {
        BytesRange(offset, size)
    }

    /// Get start position of BytesRange.
    pub fn offset(&self) -> Option<u64> {
        self.0
    }

    /// Get end position of BytesRange.
    pub fn size(&self) -> Option<u64> {
        self.1
    }

    /// If this range is full of this object content.
    ///
    /// If this range is full, we don't need to specify it in http request.
    pub fn is_full(&self) -> bool {
        self.0.unwrap_or_default() == 0 && self.1.is_none()
    }

    /// Convert bytes range into Range header.
    ///
    /// # NOTE
    ///
    /// - `bytes=-1023` means get the suffix of the file.
    /// - `bytes=0-1023` means get the first 1024 bytes, we must set the end to 1023.
    pub fn to_header(&self) -> String {
        format!("bytes={self}")
    }
}

impl Display for BytesRange {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match (self.0, self.1) {
            (Some(offset), None) => write!(f, "{}-", offset),
            (None, Some(size)) => write!(f, "-{}", size - 1),
            (Some(offset), Some(size)) => write!(f, "{}-{}", offset, offset + size - 1),
            (None, None) => write!(f, "0-"),
        }
    }
}

impl FromStr for BytesRange {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let s = s.strip_prefix("bytes=").ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                anyhow!("header range is invalid: {s}"),
            )
        })?;

        if s.contains(',') {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                anyhow!("header range should not contain multiple ranges: {s}"),
            ));
        }

        let v = s.split('-').collect::<Vec<_>>();
        if v.len() != 2 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                anyhow!("header range should not contain multiple ranges: {s}"),
            ));
        }

        let parse_int_error = |e: std::num::ParseIntError| {
            Error::new(
                ErrorKind::InvalidInput,
                anyhow!("header range must contain valid integer: {e}"),
            )
        };

        if v[1].is_empty() {
            // <range-start>-
            Ok(BytesRange::new(
                Some(v[0].parse().map_err(parse_int_error)?),
                None,
            ))
        } else if v[0].is_empty() {
            // -<suffix-length>
            Ok(BytesRange::new(
                None,
                Some(v[1].parse::<u64>().map_err(parse_int_error)? + 1),
            ))
        } else {
            // <range-start>-<range-end>
            let start: u64 = v[0].parse().map_err(parse_int_error)?;
            let end: u64 = v[1].parse().map_err(parse_int_error)?;
            Ok(BytesRange::new(Some(start), Some(end - start + 1)))
        }
    }
}

impl<T> From<T> for BytesRange
where
    T: RangeBounds<u64>,
{
    fn from(range: T) -> Self {
        let offset = match range.start_bound().cloned() {
            Bound::Included(n) => Some(n),
            Bound::Excluded(n) => Some(n + 1),
            Bound::Unbounded => None,
        };
        let size = match range.end_bound().cloned() {
            Bound::Included(n) => Some(n + 1 - offset.unwrap_or_default()),
            Bound::Excluded(n) => Some(n - offset.unwrap_or_default()),
            Bound::Unbounded => None,
        };

        BytesRange(offset, size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_range_to_string() {
        let h = BytesRange::new(None, Some(1024));
        assert_eq!(h.to_string(), "-1023");

        let h = BytesRange::new(Some(0), Some(1024));
        assert_eq!(h.to_string(), "0-1023");

        let h = BytesRange::new(Some(1024), None);
        assert_eq!(h.to_string(), "1024-");

        let h = BytesRange::new(Some(1024), Some(1024));
        assert_eq!(h.to_string(), "1024-2047");
    }

    #[test]
    fn test_bytes_range_to_header() {
        let h = BytesRange::new(None, Some(1024));
        assert_eq!(h.to_header(), "bytes=-1023");

        let h = BytesRange::new(Some(0), Some(1024));
        assert_eq!(h.to_header(), "bytes=0-1023");

        let h = BytesRange::new(Some(1024), None);
        assert_eq!(h.to_header(), "bytes=1024-");

        let h = BytesRange::new(Some(1024), Some(1024));
        assert_eq!(h.to_header(), "bytes=1024-2047");
    }

    #[test]
    fn test_bytes_range_from_range_bounds() {
        assert_eq!(BytesRange::new(None, None), BytesRange::from(..));
        assert_eq!(BytesRange::new(Some(10), None), BytesRange::from(10..));
        assert_eq!(BytesRange::new(None, Some(11)), BytesRange::from(..=10));
        assert_eq!(BytesRange::new(None, Some(10)), BytesRange::from(..10));
        assert_eq!(
            BytesRange::new(Some(10), Some(10)),
            BytesRange::from(10..20)
        );
        assert_eq!(
            BytesRange::new(Some(10), Some(11)),
            BytesRange::from(10..=20)
        );
    }

    #[test]
    fn test_bytes_range_from_str() -> Result<()> {
        let cases = vec![
            (
                "range-start",
                "bytes=123-",
                BytesRange::new(Some(123), None),
            ),
            ("suffix", "bytes=-123", BytesRange::new(None, Some(124))),
            (
                "range",
                "bytes=123-124",
                BytesRange::new(Some(123), Some(2)),
            ),
            ("one byte", "bytes=0-0", BytesRange::new(Some(0), Some(1))),
            (
                "lower case header",
                "bytes=0-0",
                BytesRange::new(Some(0), Some(1)),
            ),
        ];

        for (name, input, expected) in cases {
            let actual = input.parse()?;

            assert_eq!(expected, actual, "{name}")
        }

        Ok(())
    }
}
