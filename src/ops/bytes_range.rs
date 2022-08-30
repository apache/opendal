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

use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::ops::Bound;
use std::ops::Range;
use std::ops::RangeBounds;

use anyhow::anyhow;

/// BytesRange(offset, size) carries a range of content.
///
/// BytesRange implements `ToString` which can be used as `Range` HTTP header directly.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct BytesRange(Option<u64>, Option<u64>);

impl BytesRange {
    /// Create a new `BytesRange`
    ///
    /// It better to use `BytesRange::from(1024..2048)` to construct.
    ///
    /// # Note
    ///
    /// The behavior for `None` and `Some(0)` is different.
    ///
    /// - offset=None => `bytes=-<size>`
    /// - offset=Some(0) => `bytes=0-<size>`
    pub fn new(offset: Option<u64>, size: Option<u64>) -> Self {
        BytesRange(offset, size)
    }

    /// Get offset from BytesRange.
    pub fn offset(&self) -> Option<u64> {
        self.0
    }

    /// Get size from BytesRange.
    pub fn size(&self) -> Option<u64> {
        self.1
    }

    /// Parse header Range into BytesRange
    ///
    /// NOTE: we don't support multi range.
    ///
    /// Range: <unit>=<range-start>-
    /// Range: <unit>=<range-start>-<range-end>
    /// Range: <unit>=-<suffix-length>
    pub fn from_header_range(s: &str) -> Result<Self> {
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

    /// Parse header Content-Range into BytesRange
    ///
    /// Content-Range: <unit> <range-start>-<range-end>/<size>
    /// Content-Range: <unit> <range-start>-<range-end>/*
    /// Content-Range: <unit> */<size>
    pub fn from_header_content_range(s: &str) -> Result<Self> {
        let s = s.strip_prefix("bytes ").ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                anyhow!("header content range is invalid: {s}"),
            )
        })?;

        let parse_int_error = |e: std::num::ParseIntError| {
            Error::new(
                ErrorKind::InvalidInput,
                anyhow!("header range must contain valid integer: {e}"),
            )
        };

        if let Some(size) = s.strip_prefix("*/") {
            return Ok(BytesRange::new(
                Some(0),
                Some(size.parse().map_err(parse_int_error)?),
            ));
        }

        // We don't care about the total size in content-range.
        let s = s
            .get(..s.find('/').expect("content-range must have /"))
            .expect("content-range must has /");
        let v = s.split('-').collect::<Vec<_>>();
        if v.len() != 2 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                anyhow!("header range should not contain multiple ranges: {s}"),
            ));
        }

        let start: u64 = v[0].parse().map_err(parse_int_error)?;
        let end: u64 = v[1].parse().map_err(parse_int_error)?;
        Ok(BytesRange::new(Some(start), Some(end - start + 1)))
    }

    /// Build [`Range<u64>`] with total size.
    pub fn to_range(&self, total_size: u64) -> Range<u64> {
        match (self.0, self.1) {
            (Some(offset), None) => offset..total_size,
            (None, Some(size)) => total_size - size..total_size,
            (Some(offset), Some(size)) => offset..offset + size,
            _ => panic!("invalid range"),
        }
    }
}

impl ToString for BytesRange {
    // # NOTE
    //
    // - `bytes=-1023` means get the suffix of the file.
    // - `bytes=0-1023` means get the first 1024 bytes, we must set the end to 1023.
    fn to_string(&self) -> String {
        match (self.0, self.1) {
            (Some(offset), None) => format!("bytes={}-", offset),
            (None, Some(size)) => format!("bytes=-{}", size - 1),
            (Some(offset), Some(size)) => format!("bytes={}-{}", offset, offset + size - 1),
            _ => panic!("invalid range"),
        }
    }
}

impl<T> From<T> for BytesRange
where T: RangeBounds<u64>
{
    fn from(range: T) -> Self {
        let offset = match range.start_bound().cloned() {
            Bound::Included(n) => Some(n),
            Bound::Excluded(n) => Some(n + 1),
            // Note: RangeBounds `..x` means `0..x`, it's different from `bytes=..x`
            Bound::Unbounded => Some(0),
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
        assert_eq!(h.to_string(), "bytes=-1023");

        let h = BytesRange::new(Some(0), Some(1024));
        assert_eq!(h.to_string(), "bytes=0-1023");

        let h = BytesRange::new(Some(1024), None);
        assert_eq!(h.to_string(), "bytes=1024-");

        let h = BytesRange::new(Some(1024), Some(1024));
        assert_eq!(h.to_string(), "bytes=1024-2047");
    }

    #[test]
    fn test_bytes_range_from_range_bounds() {
        assert_eq!(BytesRange::new(Some(0), None), BytesRange::from(..));
        assert_eq!(BytesRange::new(Some(10), None), BytesRange::from(10..));
        assert_eq!(BytesRange::new(Some(0), Some(11)), BytesRange::from(..=10));
        assert_eq!(BytesRange::new(Some(0), Some(10)), BytesRange::from(..10));
        assert_eq!(
            BytesRange::new(Some(10), Some(10)),
            BytesRange::from(10..20)
        );
    }

    #[test]
    fn test_bytes_range_from_header_range() -> Result<()> {
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
            let actual = BytesRange::from_header_range(input)?;

            assert_eq!(expected, actual, "{name}")
        }

        Ok(())
    }

    #[test]
    fn test_bytes_range_from_header_content_range() -> Result<()> {
        let cases = vec![
            (
                "range start with unknown size",
                "bytes 123-123/*",
                BytesRange::new(Some(123), Some(1)),
            ),
            (
                "range start with known size",
                "bytes 123-123/1",
                BytesRange::new(Some(123), Some(1)),
            ),
            (
                "only have size",
                "bytes */1024",
                BytesRange::new(Some(0), Some(1024)),
            ),
        ];

        for (name, input, expected) in cases {
            let actual = BytesRange::from_header_content_range(input)?;

            assert_eq!(expected, actual, "{name}")
        }

        Ok(())
    }

    #[test]
    fn test_bytes_range_to_range() {
        let cases = vec![
            (
                "offset only",
                BytesRange::new(Some(1024), None),
                2048,
                1024..2048,
            ),
            (
                "size only",
                BytesRange::new(None, Some(1024)),
                2048,
                1024..2048,
            ),
            (
                "offset zero",
                BytesRange::new(Some(0), Some(1024)),
                2048,
                0..1024,
            ),
            (
                "part of data",
                BytesRange::new(Some(1024), Some(1)),
                4096,
                1024..1025,
            ),
        ];

        for (name, input, input_size, expected) in cases {
            let actual = input.to_range(input_size);

            assert_eq!(expected, actual, "{name}")
        }
    }
}
