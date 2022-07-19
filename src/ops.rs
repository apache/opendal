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

//! Operations used by [`Accessor`][crate::Accessor].
//!
//! Users should not use struct or functions here, use [`Operator`][crate::Operator] instead

use std::collections::Bound;
use std::fmt::Display;
use std::fmt::Formatter;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::ops::RangeBounds;

use anyhow::anyhow;
use time::Duration;

use crate::error::other;
use crate::error::ObjectError;
use crate::ObjectMode;

/// Operation is the name for APIs in `Accessor`.
#[derive(Debug, Copy, Clone)]
pub enum Operation {
    /// Operation for [`crate::Accessor::metadata`]
    Metadata,
    /// Operation for [`crate::Accessor::create`]
    Create,
    /// Operation for [`crate::Accessor::read`]
    Read,
    /// Operation for [`crate::Accessor::write`]
    Write,
    /// Operation for [`crate::Accessor::stat`]
    Stat,
    /// Operation for [`crate::Accessor::delete`]
    Delete,
    /// Operation for [`crate::Accessor::list`]
    List,
    /// Operation for [`crate::Accessor::presign`]
    Presign,
}

impl Default for Operation {
    fn default() -> Self {
        Operation::Metadata
    }
}

impl Display for Operation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Operation::Metadata => write!(f, "metadata"),
            Operation::Create => write!(f, "create"),
            Operation::Read => write!(f, "read"),
            Operation::Write => write!(f, "write"),
            Operation::Stat => write!(f, "stat"),
            Operation::Delete => write!(f, "delete"),
            Operation::List => write!(f, "list"),
            Operation::Presign => write!(f, "presign"),
        }
    }
}

/// Args for `create` operation.
///
/// The path must be normalized.
#[derive(Debug, Clone, Default)]
pub struct OpCreate {
    path: String,
    mode: ObjectMode,
}

impl OpCreate {
    /// Create a new `OpCreate`.
    ///
    /// If input path is not match with object mode, an error will be returned.
    pub fn new(path: &str, mode: ObjectMode) -> Result<Self> {
        match mode {
            ObjectMode::FILE => {
                if path.ends_with('/') {
                    return Err(other(ObjectError::new(
                        "create",
                        path,
                        anyhow!("Is a directory"),
                    )));
                }
                Ok(Self {
                    path: path.to_string(),
                    mode,
                })
            }
            ObjectMode::DIR => {
                if !path.ends_with('/') {
                    return Err(other(ObjectError::new(
                        "create",
                        path,
                        anyhow!("Not a directory"),
                    )));
                }

                Ok(Self {
                    path: path.to_string(),
                    mode,
                })
            }
            ObjectMode::Unknown => Err(other(ObjectError::new(
                "create",
                path,
                anyhow!("create unknown object mode is not supported"),
            ))),
        }
    }

    /// Get path from option.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Get object mode from option.
    pub fn mode(&self) -> ObjectMode {
        self.mode
    }
}

/// Args for `read` operation.
///
/// The path must be normalized.
#[derive(Debug, Clone, Default)]
pub struct OpRead {
    path: String,
    offset: Option<u64>,
    size: Option<u64>,
}

impl OpRead {
    /// Create a new `OpRead`.
    ///
    /// If input path is not a file path, an error will be returned.
    pub fn new(path: &str, range: impl RangeBounds<u64>) -> Result<Self> {
        if path.ends_with('/') {
            return Err(other(ObjectError::new(
                "read",
                path,
                anyhow!("Is a directory"),
            )));
        }

        let br = BytesRange::from(range);

        Ok(Self {
            path: path.to_string(),
            offset: br.offset(),
            size: br.size(),
        })
    }

    pub(crate) fn new_with_offset(
        path: &str,
        offset: Option<u64>,
        size: Option<u64>,
    ) -> Result<Self> {
        if path.ends_with('/') {
            return Err(other(ObjectError::new(
                "read",
                path,
                anyhow!("Is a directory"),
            )));
        }

        Ok(Self {
            path: path.to_string(),
            offset,
            size,
        })
    }

    /// Get path from option.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Get offset from option.
    pub fn offset(&self) -> Option<u64> {
        self.offset
    }

    /// Get size from option.
    pub fn size(&self) -> Option<u64> {
        self.size
    }
}

/// Args for `stat` operation.
///
/// The path must be normalized.
#[derive(Debug, Clone, Default)]
pub struct OpStat {
    path: String,
}

impl OpStat {
    /// Create a new `OpStat`.
    pub fn new(path: &str) -> Result<Self> {
        Ok(Self {
            path: path.to_string(),
        })
    }

    /// Get path from option.
    pub fn path(&self) -> &str {
        &self.path
    }
}

/// Args for `write` operation.
///
/// The path must be normalized.
#[derive(Debug, Clone, Default)]
pub struct OpWrite {
    path: String,
    size: u64,
}

impl OpWrite {
    /// Create a new `OpWrite`.
    ///
    /// If input path is not a file path, an error will be returned.
    pub fn new(path: &str, size: u64) -> Result<Self> {
        if path.ends_with('/') {
            return Err(other(ObjectError::new(
                "write",
                path,
                anyhow!("Is a directory"),
            )));
        }

        Ok(Self {
            path: path.to_string(),
            size,
        })
    }

    /// Get path from option.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Get size from option.
    pub fn size(&self) -> u64 {
        self.size
    }
}

/// Args for `delete` operation.
///
/// The path must be normalized.
#[derive(Debug, Clone, Default)]
pub struct OpDelete {
    path: String,
}

impl OpDelete {
    /// Create a new `OpDelete`.
    pub fn new(path: &str) -> Result<Self> {
        Ok(Self {
            path: path.to_string(),
        })
    }

    /// Get path from option.
    pub fn path(&self) -> &str {
        &self.path
    }
}

/// Args for `list` operation.
///
/// The path must be normalized.
#[derive(Debug, Clone, Default)]
pub struct OpList {
    path: String,
}

impl OpList {
    /// Create a new `OpList`.
    ///
    /// If input path is not a dir path, an error will be returned.
    pub fn new(path: &str) -> Result<Self> {
        if !path.ends_with('/') {
            return Err(other(ObjectError::new(
                "list",
                path,
                anyhow!("Not a directory"),
            )));
        }

        Ok(Self {
            path: path.to_string(),
        })
    }

    /// Get path from option.
    pub fn path(&self) -> &str {
        &self.path
    }
}

/// Args for `presign` operation.
///
/// The path must be normalized.
#[derive(Debug, Clone, Default)]
pub struct OpPresign {
    path: String,
    op: Operation,
    expire: Duration,
}

impl OpPresign {
    /// Create a new `OpPresign`.
    pub fn new(path: &str, op: Operation, expire: Duration) -> Result<Self> {
        Ok(Self {
            path: path.to_string(),
            op,
            expire,
        })
    }

    /// Get path from op.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Get operation from op.
    pub fn operation(&self) -> Operation {
        self.op
    }

    /// Get expire from op.
    pub fn expire(&self) -> Duration {
        self.expire
    }
}

/// PresignedRequest is a presigned request return by `presign`.
///
/// # TODO
///
/// Add signed headers
#[derive(Debug, Clone)]
pub struct PresignedRequest {
    method: http::Method,
    uri: http::Uri,
    headers: http::HeaderMap,
}

impl PresignedRequest {
    /// Create a new PresignedRequest
    pub fn new(method: http::Method, uri: http::Uri, headers: http::HeaderMap) -> Self {
        Self {
            method,
            uri,
            headers,
        }
    }

    /// Return request's method.
    pub fn method(&self) -> &http::Method {
        &self.method
    }

    /// Return request's uri.
    pub fn uri(&self) -> &http::Uri {
        &self.uri
    }

    /// Return request's header.
    pub fn header(&self) -> &http::HeaderMap {
        &self.headers
    }
}

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
        let s = s.to_lowercase();
        let s = s.strip_prefix("range: bytes=").ok_or_else(|| {
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
        let s = s.to_lowercase();
        let s = s.strip_prefix("content-range: bytes ").ok_or_else(|| {
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
where
    T: RangeBounds<u64>,
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
                "Range: bytes=123-",
                BytesRange::new(Some(123), None),
            ),
            (
                "suffix",
                "Range: bytes=-123",
                BytesRange::new(None, Some(124)),
            ),
            (
                "range",
                "Range: bytes=123-124",
                BytesRange::new(Some(123), Some(2)),
            ),
            (
                "one byte",
                "Range: bytes=0-0",
                BytesRange::new(Some(0), Some(1)),
            ),
            (
                "lower case header",
                "range: bytes=0-0",
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
                "Content-Range: bytes 123-123/*",
                BytesRange::new(Some(123), Some(1)),
            ),
            (
                "range start with known size",
                "Content-Range: bytes 123-123/1",
                BytesRange::new(Some(123), Some(1)),
            ),
            (
                "only have size",
                "Content-Range: bytes */1024",
                BytesRange::new(Some(0), Some(1024)),
            ),
        ];

        for (name, input, expected) in cases {
            let actual = BytesRange::from_header_content_range(input)?;

            assert_eq!(expected, actual, "{name}")
        }

        Ok(())
    }
}
