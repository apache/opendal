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

use super::backend::*;
use opendal_core::raw::*;
use opendal_core::*;

/// Reader returned by this backend.
pub struct RedisReader {
    backend: RedisBackend,
    path: String,
}

impl RedisReader {
    pub(super) fn new(backend: RedisBackend, path: &str, _: OpRead) -> Self {
        Self {
            backend,
            path: path.to_string(),
        }
    }
}

impl oio::StreamRead for RedisReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let p = build_absolute_path(&backend.root, path);

        let (buffer, metadata) = if range.is_full() {
            // Full read - use GET
            match backend.core.get(&p).await? {
                Some(bs) => {
                    let metadata = MetadataBuilder::file(bs.len() as u64).build();
                    (bs, Some(metadata))
                }
                None => return Err(Error::new(ErrorKind::NotFound, "key not found in redis")),
            }
        } else if let Some((start, end)) = redis_getrange_bounds(range) {
            // Range read - fetch range, length, and existence in one pipeline
            let (content_length, bs) = match backend.core.get_range(&p, start, end).await? {
                Some(v) => v,
                None => return Err(Error::new(ErrorKind::NotFound, "key not found in redis")),
            };
            let content_range = range.to_content_range(content_length)?;
            if bs.len() != content_range.len() {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "redis key changed concurrently during range read",
                )
                .set_temporary());
            }
            let metadata = MetadataBuilder::file(content_length as u64).build();
            (bs, Some(metadata))
        } else {
            let content_length = match backend.core.len(&p).await? {
                Some(v) => v,
                None => return Err(Error::new(ErrorKind::NotFound, "key not found in redis")),
            };
            let _ = range.to_content_range(content_length)?;
            let metadata = MetadataBuilder::file(content_length as u64).build();
            (Buffer::new(), Some(metadata))
        };

        let rp = metadata.map_or_else(RpRead::default, RpRead::new);
        Ok((rp, Box::new(buffer) as Box<dyn oio::ReadStreamDyn>))
    }
}

fn redis_getrange_bounds(range: BytesRange) -> Option<(i64, i64)> {
    match range {
        BytesRange::Range { size: Some(0), .. } | BytesRange::Suffix { size: 0 } => None,
        BytesRange::Range { offset, size: None } => {
            let start = i64::try_from(offset).ok()?;
            Some((start, -1))
        }
        BytesRange::Range {
            offset,
            size: Some(size),
        } => {
            let start = i64::try_from(offset).ok()?;
            let end = offset.saturating_add(size - 1).min(i64::MAX as u64) as i64;
            Some((start, end))
        }
        BytesRange::Suffix { size } => {
            if size <= i64::MAX as u64 {
                Some((-(size as i64), -1))
            } else {
                Some((0, -1))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn getrange_bounds_cover_all_range_forms() {
        assert_eq!(redis_getrange_bounds(BytesRange::new(0, Some(0))), None);
        assert_eq!(redis_getrange_bounds(BytesRange::new(10, Some(0))), None);
        assert_eq!(redis_getrange_bounds(BytesRange::suffix(0)), None);
        assert_eq!(redis_getrange_bounds(BytesRange::new(u64::MAX, None)), None);
        assert_eq!(
            redis_getrange_bounds(BytesRange::new(u64::MAX, Some(5))),
            None
        );

        assert_eq!(
            redis_getrange_bounds(BytesRange::new(5, None)),
            Some((5, -1))
        );
        assert_eq!(
            redis_getrange_bounds(BytesRange::new(0, Some(10))),
            Some((0, 9))
        );
        assert_eq!(
            redis_getrange_bounds(BytesRange::new(5, Some(u64::MAX))),
            Some((5, i64::MAX))
        );
        assert_eq!(
            redis_getrange_bounds(BytesRange::suffix(10)),
            Some((-10, -1))
        );
        assert_eq!(
            redis_getrange_bounds(BytesRange::suffix(i64::MAX as u64)),
            Some((-i64::MAX, -1))
        );
        assert_eq!(
            redis_getrange_bounds(BytesRange::suffix(u64::MAX)),
            Some((0, -1))
        );
    }
}
