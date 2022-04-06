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
use std::io::Result;
use std::ops::RangeBounds;

use anyhow::anyhow;

use crate::error::other;
use crate::error::ObjectError;
use crate::ObjectMode;

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
    pub fn path(&self) -> &str {
        &self.path
    }
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
    pub fn path(&self) -> &str {
        &self.path
    }
    pub fn offset(&self) -> Option<u64> {
        self.offset
    }
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
    pub fn path(&self) -> &str {
        &self.path
    }
    pub fn size(&self) -> u64 {
        self.size
    }
}

/// Args for `delete` operation.
///
/// The path must be normalized.
#[derive(Debug, Clone, Default)]
pub struct OpDelete {
    pub path: String,
}

impl OpDelete {
    /// Create a new `OpDelete`.
    pub fn new(path: &str) -> Result<Self> {
        Ok(Self {
            path: path.to_string(),
        })
    }
    pub fn path(&self) -> &str {
        &self.path
    }
}

/// Args for `list` operation.
///
/// The path must be normalized.
#[derive(Debug, Clone, Default)]
pub struct OpList {
    pub path: String,
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
    pub fn path(&self) -> &str {
        &self.path
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
    pub fn new(offset: Option<u64>, size: Option<u64>) -> Self {
        BytesRange(offset, size)
    }

    pub fn offset(&self) -> Option<u64> {
        self.0
    }

    pub fn size(&self) -> Option<u64> {
        self.1
    }
}

impl ToString for BytesRange {
    // # NOTE
    //
    // - `bytes=-1023` means get the suffix of the file, we must set the start to 0.
    // - `bytes=0-1023` means get the first 1024 bytes, we must set the end to 1023.
    fn to_string(&self) -> String {
        match (self.0, self.1) {
            (Some(offset), None) => format!("bytes={}-", offset),
            (None, Some(size)) => format!("bytes=0-{}", size - 1),
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
        assert_eq!(h.to_string(), "bytes=0-1023");

        let h = BytesRange::new(Some(1024), None);
        assert_eq!(h.to_string(), "bytes=1024-");

        let h = BytesRange::new(Some(1024), Some(1024));
        assert_eq!(h.to_string(), "bytes=1024-2047");
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
    }
}
