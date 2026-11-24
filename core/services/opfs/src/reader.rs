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

use std::ops::Range;

#[cfg(target_arch = "wasm32")]
use super::backend::OpfsBackend;
#[cfg(target_arch = "wasm32")]
use super::core::*;
#[cfg(target_arch = "wasm32")]
use opendal_core::raw::*;
use opendal_core::*;
#[cfg(target_arch = "wasm32")]
use send_wrapper::SendWrapper;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::JsCast;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::JsFuture;
#[cfg(target_arch = "wasm32")]
use web_sys::FileSystemFileHandle;

/// Resolve `range` against `file_size` using `u64` arithmetic so files larger
/// than 4 GiB are handled without 32-bit `usize` truncation on `wasm32`.
pub(crate) fn resolve_read_range(range: BytesRange, file_size: u64) -> Result<Range<u64>> {
    let (offset, size) = match range {
        BytesRange::Range { offset, size } => (offset, size),
        BytesRange::Suffix { size } => {
            return Ok(file_size.saturating_sub(size)..file_size);
        }
    };
    if size == Some(0) {
        return Ok(0..0);
    }
    if offset >= file_size {
        if size.is_none() {
            return Ok(file_size..file_size);
        }
        return Err(Error::new(
            ErrorKind::RangeNotSatisfied,
            "range starts at or beyond the end of the object",
        )
        .with_operation("OpfsReadStream::read")
        .with_context("offset", offset)
        .with_context("content_length", file_size));
    }

    let end = size.map_or(file_size, |size| offset.saturating_add(size).min(file_size));
    Ok(offset..end)
}

#[cfg(target_arch = "wasm32")]
pub struct OpfsReadStream {
    handle: SendWrapper<FileSystemFileHandle>,
    range: BytesRange,
    done: bool,
}

#[cfg(target_arch = "wasm32")]
impl OpfsReadStream {
    pub fn new(handle: FileSystemFileHandle, range: BytesRange) -> Self {
        Self {
            handle: SendWrapper::new(handle),
            range,
            done: false,
        }
    }
}

#[cfg(target_arch = "wasm32")]
impl oio::ReadStream for OpfsReadStream {
    async fn read(&mut self) -> Result<Buffer> {
        if self.done {
            return Ok(Buffer::new());
        }
        self.done = true;

        let file: web_sys::File = JsFuture::from(self.handle.get_file())
            .await
            .and_then(JsCast::dyn_into)
            .map_err(parse_js_error)?;

        let blob: &web_sys::Blob = file.as_ref();
        let file_size = blob.size() as u64;
        let slice = resolve_read_range(self.range, file_size)?;
        if slice.is_empty() {
            return Ok(Buffer::new());
        }

        let blob = if slice.start == 0 && slice.end == file_size {
            blob.clone()
        } else {
            blob.slice_with_f64_and_f64(slice.start as f64, slice.end as f64)
                .map_err(parse_js_error)?
        };

        let array_buffer = JsFuture::from(blob.array_buffer())
            .await
            .map_err(parse_js_error)?;
        let uint8_array = js_sys::Uint8Array::new(&array_buffer);

        Ok(Buffer::from(uint8_array.to_vec()))
    }
}

/// Reader returned by this backend.
#[cfg(target_arch = "wasm32")]
pub struct OpfsReader {
    backend: OpfsBackend,
    path: String,
}

#[cfg(target_arch = "wasm32")]
impl OpfsReader {
    pub(super) fn new(backend: OpfsBackend, path: &str, _: OpRead) -> Self {
        Self {
            backend,
            path: path.to_string(),
        }
    }
}

#[cfg(target_arch = "wasm32")]
impl oio::StreamRead for OpfsReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();

        let p = build_absolute_path(&backend.core.root, path);
        let handle = get_file_handle(&p, false).await?;
        let rp = RpRead::default();
        let stream = OpfsReadStream::new(handle, range);

        Ok((rp, Box::new(stream) as Box<dyn oio::ReadStreamDyn>))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_read_range_matches_bytes_range_to_content_range() {
        let cases = [
            (BytesRange::new(0, None), 10),
            (BytesRange::new(0, None), 0),
            (BytesRange::new(2, Some(4)), 10),
            (BytesRange::new(2, None), 10),
            (BytesRange::new(8, Some(4)), 10),
            (BytesRange::new(8, Some(u64::MAX)), 10),
            (BytesRange::new(10, None), 10),
            (BytesRange::new(20, None), 10),
            (BytesRange::new(u64::MAX, None), 10),
            (BytesRange::new(u64::MAX, Some(0)), 10),
            (BytesRange::new(0, Some(0)), 0),
            (BytesRange::new(0, Some(1)), 0),
            (BytesRange::new(10, Some(1)), 10),
            (BytesRange::new(100, Some(5)), 10),
            (BytesRange::new(u64::MAX, Some(1)), 10),
            (BytesRange::suffix(4), 10),
            (BytesRange::suffix(10), 10),
            (BytesRange::suffix(20), 10),
            (BytesRange::suffix(0), 10),
            (BytesRange::suffix(4), 0),
        ];

        for (range, file_size) in cases {
            let actual = resolve_read_range(range, file_size);
            let expected = range
                .to_content_range(file_size as usize)
                .map(|r| r.start as u64..r.end as u64);
            match (actual, expected) {
                (Ok(a), Ok(e)) => assert_eq!(a, e, "range={range:?}, file_size={file_size}"),
                (Err(a), Err(e)) => {
                    assert_eq!(a.kind(), e.kind(), "range={range:?}, file_size={file_size}")
                }
                (a, e) => {
                    panic!("mismatch for range={range:?}, file_size={file_size}: {a:?} vs {e:?}")
                }
            }
        }
    }

    #[test]
    fn resolve_read_range_rejects_out_of_bounds_and_handles_large_u64() {
        let err = resolve_read_range(BytesRange::new(100, Some(5)), 10).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::RangeNotSatisfied);

        let err = resolve_read_range(BytesRange::new(0, Some(1)), 0).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::RangeNotSatisfied);

        // Files larger than 4 GiB (`u32::MAX`) must not truncate on wasm32.
        let five_gib = 5 * 1024 * 1024 * 1024_u64;
        assert_eq!(
            resolve_read_range(BytesRange::new(five_gib - 10, Some(4)), five_gib).unwrap(),
            (five_gib - 10)..(five_gib - 6)
        );
        assert_eq!(
            resolve_read_range(BytesRange::suffix(16), five_gib).unwrap(),
            (five_gib - 16)..five_gib
        );
        let err = resolve_read_range(BytesRange::new(five_gib, Some(1)), five_gib).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::RangeNotSatisfied);
    }
}
