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

//! Operations used by [`Accessor`][crate::Accessor]

use std::collections::Bound;
use std::ops::RangeBounds;

#[derive(Debug, Clone, Default)]
pub struct OpRead {
    pub path: String,
    pub offset: Option<u64>,
    pub size: Option<u64>,
}

impl OpRead {
    pub fn new(path: &str, range: impl RangeBounds<u64>) -> Self {
        let br = BytesRange::from(range);

        Self {
            path: path.to_string(),
            offset: br.offset(),
            size: br.size(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct OpStat {
    pub path: String,
}

impl OpStat {
    pub fn new(path: &str) -> Self {
        Self {
            path: path.to_string(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct OpWrite {
    pub path: String,
    pub size: u64,
}

impl OpWrite {
    pub fn new(path: &str, size: u64) -> Self {
        Self {
            path: path.to_string(),
            size,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct OpDelete {
    pub path: String,
}

impl OpDelete {
    pub fn new(path: &str) -> Self {
        Self {
            path: path.to_string(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct OpList {
    pub path: String,
}

impl OpList {
    pub fn new(path: &str) -> Self {
        Self {
            path: path.to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct BytesRange(Option<u64>, Option<u64>);

impl BytesRange {
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
