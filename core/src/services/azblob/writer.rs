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

use async_trait::async_trait;
use http::StatusCode;

use super::core::AzblobCore;
use super::error::parse_error;
use crate::raw::oio::{Stream, Streamer};
use crate::raw::*;
use crate::*;

const X_MS_BLOB_TYPE: &str = "x-ms-blob-type";
const X_MS_BLOB_APPEND_OFFSET: &str = "x-ms-blob-append-offset";

pub type AzblobWriters =
    oio::TwoWaysWriter<oio::OneShotWriter<AzblobWriter>, oio::AppendObjectWriter<AzblobWriter>>;

pub struct AzblobWriter {
    core: Arc<AzblobCore>,

    path: String,
    op: OpWrite,
}

impl AzblobWriter {
    pub fn new(core: Arc<AzblobCore>, path: &str, op: OpWrite) -> Self {
        AzblobWriter {
            core,
            path: path.to_string(),
            op,
        }
    }
}

#[async_trait]
impl oio::OneShotWrite for AzblobWriter {
    async fn write_once(&self, stream: Streamer) -> Result<()> {
        let mut req = self.core.azblob_put_blob_request(
            &self.path,
            Some(stream.size()),
            self.op.content_type(),
            self.op.cache_control(),
            AsyncBody::Stream(stream),
        )?;

        self.core.sign(&mut req).await?;

        let resp = self.core.send(req).await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => {
                resp.into_body().consume().await?;
                Ok(())
            }
            _ => Err(parse_error(resp).await?),
        }
    }
}

#[async_trait]
impl oio::AppendObjectWrite for AzblobWriter {
    async fn offset(&self) -> Result<u64> {
        let resp = self
            .core
            .azblob_get_blob_properties(&self.path, None, None)
            .await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let headers = resp.headers();
                let blob_type = headers.get(X_MS_BLOB_TYPE).and_then(|v| v.to_str().ok());
                if blob_type != Some("AppendBlob") {
                    return Err(Error::new(
                        ErrorKind::ConditionNotMatch,
                        "the blob is not an appendable blob.",
                    ));
                }

                parse_content_length(headers).map(|v| v.unwrap_or_default())
            }
            // Return 0 if the blob is not existing.
            StatusCode::NOT_FOUND => Ok(0),
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn append(&self, offset: u64, stream: Streamer) -> Result<()> {
        // Init appendable blob if we are writing to a file with offset 0.
        if offset == 0 {
            let mut req = self.core.azblob_init_appendable_blob_request(
                &self.path,
                self.op.content_type(),
                self.op.cache_control(),
            )?;

            self.core.sign(&mut req).await?;

            let resp = self.core.client.send(req).await?;
            let status = resp.status();
            match status {
                StatusCode::CREATED => {
                    // do nothing
                }
                _ => {
                    return Err(parse_error(resp).await?);
                }
            }
        }

        let mut req = self.core.azblob_append_blob_request(
            &self.path,
            stream.size(),
            Some(offset),
            AsyncBody::Stream(stream),
        )?;

        self.core.sign(&mut req).await?;

        let resp = self.core.send(req).await?;

        let status = resp.status();
        match status {
            StatusCode::CREATED => Ok(()),
            _ => Err(parse_error(resp).await?),
        }
    }
}
