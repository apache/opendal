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

use crate::raw::*;
use crate::*;

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

    upload_id: Option<String>,
    parts: Vec<MultipartUploadPart>,
}

impl<W: MultipartUploadWrite> MultipartUploadWriter<W> {
    /// Create a new MultipartUploadWriter.
    pub fn new(inner: W) -> Self {
        Self {
            inner,

            upload_id: None,
            parts: Vec::new(),
        }
    }

    /// Get the upload id. Initiate a new multipart upload if the upload id is empty.
    pub async fn upload_id(&mut self) -> Result<String> {
        match &self.upload_id {
            Some(upload_id) => Ok(upload_id.to_string()),
            None => {
                let upload_id = self.inner.initiate_part().await?;
                self.upload_id = Some(upload_id.clone());
                Ok(upload_id)
            }
        }
    }
}

#[async_trait]
impl<W> oio::Write for MultipartUploadWriter<W>
where
    W: MultipartUploadWrite,
{
    async fn write(&mut self, bs: Bytes) -> Result<u64> {
        let upload_id = self.upload_id().await?;

        let size = bs.len();

        self.inner
            .write_part(
                &upload_id,
                self.parts.len(),
                size as u64,
                AsyncBody::Bytes(bs),
            )
            .await
            .map(|v| self.parts.push(v))?;

        Ok(size as u64)
    }

    async fn pipe(&mut self, size: u64, s: oio::Streamer) -> Result<u64> {
        let upload_id = self.upload_id().await?;

        self.inner
            .write_part(&upload_id, self.parts.len(), size, AsyncBody::Stream(s))
            .await
            .map(|v| self.parts.push(v))?;

        Ok(size)
    }

    async fn close(&mut self) -> Result<()> {
        let upload_id = if let Some(upload_id) = &self.upload_id {
            upload_id
        } else {
            return Ok(());
        };

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
