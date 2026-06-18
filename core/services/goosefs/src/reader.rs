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
use super::core::GoosefsCore;
use super::core::parse_error;
use goosefs_sdk::io::GoosefsFileReader as SdkReader;
use opendal_core::raw::*;
use opendal_core::*;
use std::sync::Arc;

pub struct GoosefsReadStream {
    core: Arc<GoosefsCore>,
    path: String,
    range: BytesRange,
    content_length: Option<u64>,
    /// Lazily-opened SDK reader. `None` until the first `read()` call.
    inner: Option<SdkReader>,
    /// Terminal flag: once the underlying stream has returned `None`,
    /// subsequent `read()` calls must keep returning an empty `Buffer`
    /// without re-entering the SDK (which would try to open a fresh
    /// stream and re-read the file).
    done: bool,
}

impl GoosefsReadStream {
    pub fn new(
        core: Arc<GoosefsCore>,
        path: String,
        range: BytesRange,
        content_length: Option<u64>,
    ) -> Self {
        GoosefsReadStream {
            core,
            path,
            range,
            content_length,
            inner: None,
            done: false,
        }
    }

    /// Open the underlying SDK reader, picking between the full-file
    /// and ranged variants based on `self.range`.
    async fn open(&self) -> Result<SdkReader> {
        let offset = self.range.offset();
        let size = self.range.size();

        // Three cases:
        //   1. No offset and no size      → full-file stream.
        //   2. Offset+size both set       → ranged stream.
        //   3. Offset set, size unbounded → resolve tail length via
        //      get_status and fall through to the ranged opener, so we
        //      still get per-block streaming (rather than buffering the
        //      whole tail in one call).
        match (offset, size) {
            (0, None) => self.core.open_reader(&self.path).await,
            (off, Some(len)) => self.core.open_range_reader(&self.path, off, len).await,
            (off, None) => {
                let content_length = self.content_length.ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "content length must be known for offset reads",
                    )
                })?;
                let len = content_length.saturating_sub(off);
                if len == 0 {
                    // Empty tail — short-circuit with a zero-length
                    // ranged open so the very next `read_next_block`
                    // call returns `None` and we EOF cleanly.
                    self.core.open_range_reader(&self.path, off, 0).await
                } else {
                    self.core.open_range_reader(&self.path, off, len).await
                }
            }
        }
    }
}

impl oio::ReadStream for GoosefsReadStream {
    async fn read(&mut self) -> Result<Buffer> {
        if self.done {
            return Ok(Buffer::new());
        }

        // Lazy open on first call.
        if self.inner.is_none() {
            self.inner = Some(self.open().await?);
        }
        let reader = self.inner.as_mut().expect("inner was just set");

        match reader.read_next_block().await.map_err(parse_error)? {
            Some(block) => Ok(Buffer::from(block)),
            None => {
                self.done = true;
                Ok(Buffer::new())
            }
        }
    }
}

/// Reader returned by this backend.
pub struct GoosefsReader {
    backend: GoosefsBackend,
    path: String,
}

impl GoosefsReader {
    pub(super) fn new(backend: GoosefsBackend, path: &str, _: OpRead) -> Self {
        Self {
            backend,
            path: path.to_string(),
        }
    }
}

impl oio::StreamRead for GoosefsReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();

        let content_length = if range.offset() != 0 && range.size().is_none() {
            let file_info = backend.core.get_status(path).await?;
            Some(
                backend
                    .core
                    .file_info_to_metadata(&file_info)
                    .content_length(),
            )
        } else {
            None
        };
        let rp = RpRead::default();
        let stream = GoosefsReadStream::new(
            backend.core.clone(),
            path.to_string(),
            range,
            content_length,
        );

        Ok((rp, Box::new(stream) as Box<dyn oio::ReadStreamDyn>))
    }
}
