// Copyright 2022 Datafuse Labs
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

use async_trait::async_trait;
use bytes::Bytes;
use bytes::BytesMut;
use http::StatusCode;

use super::backend::CompleteMultipartUploadRequestPart;
use super::backend::S3Backend;
use super::error::parse_error;
use crate::ops::OpWrite;
use crate::raw::*;
use crate::*;

/// the size of parts limit for append
const PART_LIMIT: usize = 4 * 1024 * 1024;
/// size of buffer
/// double capacity should reduce some copy of vector
const BUF_SIZE: usize = 2 * PART_LIMIT;

pub struct S3Writer {
    backend: S3Backend,
    // write buffer for append
    buffer: BytesMut,

    op: OpWrite,
    path: String,

    upload_id: Option<String>,
    parts: Vec<CompleteMultipartUploadRequestPart>,
}

impl S3Writer {
    pub fn new(backend: S3Backend, op: OpWrite, path: String, upload_id: Option<String>) -> Self {
        S3Writer {
            backend,
            buffer: BytesMut::with_capacity(BUF_SIZE),
            op,
            path,
            upload_id,
            parts: vec![],
        }
    }
}

#[async_trait]
impl oio::Write for S3Writer {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        debug_assert!(
            self.upload_id.is_none(),
            "Writer initiated with upload id, but users trying to call write, must be buggy"
        );

        let mut req = self.backend.s3_put_object_request(
            &self.path,
            Some(bs.len()),
            self.op.content_type(),
            self.op.content_disposition(),
            AsyncBody::Bytes(bs),
        )?;

        self.backend
            .signer
            .sign(&mut req)
            .map_err(new_request_sign_error)?;

        let resp = self.backend.client.send_async(req).await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => {
                resp.into_body().consume().await?;
                Ok(())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn append(&mut self, bs: Bytes) -> Result<()> {
        if self.upload_id.is_some() {
            panic!(
                "Writer initiated with upload id, but users trying to call write, must be buggy"
            );
        }

        // A new buffer is introduced
        //
        // for the convenience of flushing
        // the size of our buffer should always be kept as {0} ∪ [PART_LIMIT, BUF_SIZE) bytes!
        let bs: Bytes = if bs.len() < PART_LIMIT {
            if bs.len() + self.buffer.len() < BUF_SIZE {
                // will not exceed limit of buffer, extend buffer directly
                self.buffer.extend_from_slice(&bs);
                return Ok(());
            }
            // truncate the first bytes of buffer
            let trunk_size = bs.len() + self.buffer.len() - PART_LIMIT;
            let trunk = self.buffer.split_to(trunk_size);

            self.buffer.extend_from_slice(&bs);
            debug_assert!(self.buffer.len() >= PART_LIMIT);

            trunk.freeze()
        } else if bs.len() < BUF_SIZE {
            // larger than PART_LIMIT, replace into buffer

            // flush all data in buffer
            self.flush().await?;
            // take the last part of data (larger than PART_LIMIT) in our buffer
            self.buffer.extend_from_slice(&bs);
            return Ok(());
        } else {
            // flush all data in the buffer
            // and then write the data in bs directly (to minimize copy)
            // finally, keep the last part of data (larger than PART_LIMIT) in our buffer

            self.flush().await?;
            // size of directly write trunk
            let trunk_size = (bs.len() / (PART_LIMIT) - 1) * (PART_LIMIT);

            // direct write trunk
            let mut bs = bs;
            let buf = bs.split_to(trunk_size);
            self.buffer.extend_from_slice(&bs);

            buf
        };

        // AWS S3 requires part number must between [1..=10000]
        let part_number = self.parts.len() + 1;

        let upload_id = self.upload_id.as_ref().unwrap();

        let mut req = self.backend.s3_upload_part_request(
            &self.path,
            upload_id,
            part_number,
            Some(bs.len() as u64),
            AsyncBody::Bytes(bs),
        )?;

        self.backend
            .signer
            .sign(&mut req)
            .map_err(new_request_sign_error)?;

        let resp = self.backend.client.send_async(req).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let etag = parse_etag(resp.headers())?
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::Unexpected,
                            "ETag not present in returning response",
                        )
                    })?
                    .to_string();

                resp.into_body().consume().await?;

                self.parts
                    .push(CompleteMultipartUploadRequestPart { part_number, etag });

                Ok(())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn close(&mut self) -> Result<()> {
        if !self.buffer.is_empty() {
            self.flush().await?;
        }

        let upload_id = if let Some(upload_id) = &self.upload_id {
            upload_id
        } else {
            return Ok(());
        };

        let resp = self
            .backend
            .s3_complete_multipart_upload(&self.path, upload_id, &self.parts)
            .await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                resp.into_body().consume().await?;

                Ok(())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn flush(&mut self) -> Result<()> {
        let upload_id = match self.upload_id.as_ref() {
            Some(upload_id) => upload_id,
            // call on normal write? tolerate this.
            None => return Ok(()),
        };

        let bs = std::mem::take(&mut self.buffer);
        self.buffer = BytesMut::with_capacity(BUF_SIZE);
        let bs = bs.freeze();

        // AWS S3 requires part number must between [1..=10000]
        let part_number = self.parts.len() + 1;

        let mut req = self.backend.s3_upload_part_request(
            &self.path,
            upload_id,
            part_number,
            Some(bs.len() as u64),
            AsyncBody::Bytes(bs),
        )?;

        self.backend
            .signer
            .sign(&mut req)
            .map_err(new_request_sign_error)?;

        let resp = self.backend.client.send_async(req).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let etag = parse_etag(resp.headers())?
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::Unexpected,
                            "ETag not present in returning response",
                        )
                    })?
                    .to_string();

                resp.into_body().consume().await?;

                self.parts
                    .push(CompleteMultipartUploadRequestPart { part_number, etag });

                Ok(())
            }
            _ => Err(parse_error(resp).await?),
        }
    }
}
