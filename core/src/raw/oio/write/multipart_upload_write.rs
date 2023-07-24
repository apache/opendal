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

use async_trait::async_trait;
use bytes::Bytes;

use crate::{raw::*, *};

const DEFAULT_WRITE_MIN_SIZE: usize = 8 * 1024 * 1024;

/// MultipartUploadWrite is used to implement [`Write`] based on multipart
/// uploads. By implementing MultipartUploadWrite, services don't need to
/// care about the details of buffering and uploading parts.
///
/// The layout after adopting [`MultipartUploadWrite`]:
///
/// - Services impl `MultipartUploadWrite`
/// - `MultipartUploadWriter` impl `Write`
/// - Expose `MultipartUploadWriter` as `Accessor::Writer`
#[async_trait]
pub trait MultipartUploadWrite: Send + Sync + Unpin {
    /// write_once write all data at once.
    ///
    /// MultipartUploadWriter will call this API when the size of data is
    /// already known.
    async fn write_once(&self, size: u64, body: AsyncBody) -> Result<()>;

    /// initiate_part will call start a multipart upload and return the upload id.
    ///
    /// MultipartUploadWriter will call this when:
    ///
    /// - the total size of data is unknown.
    /// - the total size of data is known, but the size of current write
    /// is less then the total size.
    async fn initiate_part(&self) -> Result<String>;

    /// write_part will write a part of the data and returns the result
    /// [`MultipartUploadPart`].
    ///
    /// MultipartUploadWriter will call this API and stores the result in
    /// order.
    ///
    /// - part_number is the index of the part, starting from 0.
    async fn write_part(
        &self,
        upload_id: &str,
        part_number: usize,
        size: u64,
        body: AsyncBody,
    ) -> Result<MultipartUploadPart>;

    /// complete_part will complete the multipart upload to build the final
    /// file.
    async fn complete_part(&self, upload_id: &str, parts: &[MultipartUploadPart]) -> Result<()>;

    /// abort_part will cancel the multipart upload and purge all data.
    async fn abort_part(&self, upload_id: &str) -> Result<()>;
}

/// The result of [`MultipartUploadWrite::write_part`].
///
/// services implement should convert MultipartUploadPart to their own represents.
///
/// - `part_number` is the index of the part, starting from 0.
/// - `etag` is the `ETag` of the part.
pub struct MultipartUploadPart {
    /// The number of the part, starting from 0.
    pub part_number: usize,
    /// The etag of the part.
    pub etag: String,
}

/// MultipartUploadWriter will implements [`Write`] based on multipart
/// uploads.
///
/// ## TODO
///
/// - Add threshold for `write_once` to avoid unnecessary multipart uploads.
/// - Allow users to switch to un-buffered mode if users write 16MiB every time.
pub struct MultipartUploadWriter<W: MultipartUploadWrite> {
    inner: W,
    total_size: Option<u64>,

    upload_id: Option<String>,
    parts: Vec<MultipartUploadPart>,
    buffer: oio::VectorCursor,
    buffer_size: usize,
}

impl<W: MultipartUploadWrite> MultipartUploadWriter<W> {
    /// Create a new MultipartUploadWriter.
    pub fn new(inner: W, total_size: Option<u64>) -> Self {
        Self {
            inner,
            total_size,

            upload_id: None,
            parts: Vec::new(),
            buffer: oio::VectorCursor::new(),
            buffer_size: DEFAULT_WRITE_MIN_SIZE,
        }
    }

    /// Configure the write_min_size.
    ///
    /// write_min_size is used to control the size of internal buffer.
    ///
    /// MultipartUploadWriter will flush the buffer to upload a part when
    /// the size of buffer is larger than write_min_size.
    ///
    /// This value is default to 8 MiB (as recommended by AWS).
    pub fn with_write_min_size(mut self, v: usize) -> Self {
        self.buffer_size = v;
        self
    }
}

#[async_trait]
impl<W> oio::Write for MultipartUploadWriter<W>
where
    W: MultipartUploadWrite,
{
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        let upload_id = match &self.upload_id {
            Some(upload_id) => upload_id,
            None => {
                if self.total_size.unwrap_or_default() == bs.len() as u64 {
                    return self
                        .inner
                        .write_once(bs.len() as u64, AsyncBody::Bytes(bs))
                        .await;
                }

                let upload_id = self.inner.initiate_part().await?;
                self.upload_id = Some(upload_id);
                self.upload_id.as_deref().unwrap()
            }
        };

        // Ignore empty bytes
        if bs.is_empty() {
            return Ok(());
        }

        self.buffer.push(bs);
        // Return directly if the buffer is not full
        if self.buffer.len() <= self.buffer_size {
            return Ok(());
        }

        let bs = self.buffer.peak_at_least(self.buffer_size);
        let size = bs.len();

        match self
            .inner
            .write_part(
                upload_id,
                self.parts.len(),
                size as u64,
                AsyncBody::Bytes(bs),
            )
            .await
        {
            Ok(part) => {
                self.buffer.take(size);
                self.parts.push(part);
                Ok(())
            }
            Err(e) => {
                // If the upload fails, we should pop the given bs to make sure
                // write is re-enter safe.
                self.buffer.pop();
                Err(e)
            }
        }
    }

    async fn sink(&mut self, size: u64, s: oio::Streamer) -> Result<()> {
        if !self.buffer.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Writer::sink should not be used mixed with existing buffer",
            ));
        }

        let upload_id = match &self.upload_id {
            Some(upload_id) => upload_id,
            None => {
                if self.total_size.unwrap_or_default() == size {
                    return self.inner.write_once(size, AsyncBody::Stream(s)).await;
                }

                let upload_id = self.inner.initiate_part().await?;
                self.upload_id = Some(upload_id);
                self.upload_id.as_deref().unwrap()
            }
        };

        let part = self
            .inner
            .write_part(upload_id, self.parts.len(), size, AsyncBody::Stream(s))
            .await?;
        self.parts.push(part);

        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        let upload_id = if let Some(upload_id) = &self.upload_id {
            upload_id
        } else {
            return Ok(());
        };

        // Make sure internal buffer has been flushed.
        if !self.buffer.is_empty() {
            let bs = self.buffer.peak_exact(self.buffer.len());

            match self
                .inner
                .write_part(
                    upload_id,
                    self.parts.len(),
                    bs.len() as u64,
                    AsyncBody::Bytes(bs),
                )
                .await
            {
                Ok(part) => {
                    self.buffer.clear();
                    self.parts.push(part);
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        self.inner.complete_part(upload_id, &self.parts).await
    }

    async fn abort(&mut self) -> Result<()> {
        let upload_id = if let Some(upload_id) = &self.upload_id {
            upload_id
        } else {
            return Ok(());
        };

        self.inner.abort_part(upload_id).await
    }
}
