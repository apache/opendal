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

use std::sync::Arc;

use goosefs_sdk::io::GoosefsFileReader as SdkReader;

use super::core::GoosefsCore;
use super::error::parse_error;
use opendal_core::raw::*;
use opendal_core::*;

/// `GoosefsReader` implements [`oio::ReadStream`] on top of the goosefs-sdk
/// high-level streaming reader (`GoosefsFileReader`).
///
/// # Streaming semantics
///
/// Unlike the first-cut implementation, which called
/// `GoosefsCore::read_file()` to fetch the entire requested range into
/// one `bytes::Bytes` before returning anything to the caller, this
/// reader pulls the file **one block at a time** via
/// [`SdkReader::read_next_block`] and hands each block to OpenDAL as a
/// separate `Buffer`:
///
///   - Peak memory is bounded by a single GooseFS block (64 MiB by
///     default), not by the full object size.
///   - Large reads start surfacing data to the caller as soon as the
///     first block lands, rather than stalling until the whole range
///     is materialised.
///   - When `read_next_block` returns `None` the reader returns an
///     empty `Buffer`, which is OpenDAL's `oio::ReadStream` EOF signal.
///
/// # Range handling
///
/// The underlying SDK exposes two entry points:
///
///   - [`SdkReader::open_with_context`] for a full-file stream.
///   - [`SdkReader::open_range_with_context`] for a bounded
///     `[offset, offset+length)` stream.
///
/// We pick between them based on the incoming [`OpRead`] range. When
/// only `offset` is specified (unbounded tail read), we resolve the
/// actual tail length via `get_status` and fall through to the
/// range-based opener, so the SDK can still use its efficient
/// per-block streaming code path.
///
/// # Authentication retry
///
/// The authentication-reset / retry logic lives inside
/// [`GoosefsCore::open_reader`] and [`GoosefsCore::open_range_reader`],
/// so the reader stays focused on streaming. Once the stream is open
/// we do not attempt to rebuild the context mid-read — a mid-stream
/// auth failure almost always means a transport / worker problem
/// rather than a stale SASL credential, and transparent mid-stream
/// recovery would require replaying `bytes_read` bytes, which is
/// more complexity than the failure mode warrants.
pub struct GoosefsReader {
    core: Arc<GoosefsCore>,
    path: String,
    args: OpRead,
    content_length: Option<u64>,
    /// Lazily-opened SDK reader. `None` until the first `read()` call.
    inner: Option<SdkReader>,
    /// Terminal flag: once the underlying stream has returned `None`,
    /// subsequent `read()` calls must keep returning an empty `Buffer`
    /// without re-entering the SDK (which would try to open a fresh
    /// stream and re-read the file).
    done: bool,
}

impl GoosefsReader {
    pub fn new(
        core: Arc<GoosefsCore>,
        path: String,
        args: OpRead,
        content_length: Option<u64>,
    ) -> Self {
        GoosefsReader {
            core,
            path,
            args,
            content_length,
            inner: None,
            done: false,
        }
    }

    /// Open the underlying SDK reader, picking between the full-file
    /// and ranged variants based on `self.args`.
    async fn open(&self) -> Result<SdkReader> {
        let range = self.args.range();
        let offset = range.offset();
        let size = range.size();

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

impl oio::ReadStream for GoosefsReader {
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
