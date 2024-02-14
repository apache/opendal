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
use uuid::Uuid;

use super::core::AzblobCore;
use super::error::parse_error;
use crate::raw::*;
use crate::*;

const X_MS_BLOB_TYPE: &str = "x-ms-blob-type";

pub type AzblobWriters = TwoWays<oio::BlockWriter<AzblobWriter>, oio::AppendWriter<AzblobWriter>>;

pub struct AzblobWriter {
    core: Arc<AzblobCore>,

    op: OpWrite,
    path: String,
}

impl AzblobWriter {
    pub fn new(core: Arc<AzblobCore>, op: OpWrite, path: String) -> Self {
        AzblobWriter { core, op, path }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl oio::OneShotWrite for AzblobWriter {
    async fn write_once(&self, bs: &dyn oio::WriteBuf) -> Result<()> {
        let bs = oio::ChunkedBytes::from_vec(bs.vectored_bytes(bs.remaining()));
        let mut req = self.core.azblob_put_blob_request(
            &self.path,
            Some(bs.len() as u64),
            &self.op,
            AsyncBody::ChunkedBytes(bs),
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl oio::AppendWrite for AzblobWriter {
    async fn offset(&self) -> Result<u64> {
        let resp = self
            .core
            .azblob_get_blob_properties(&self.path, &OpStat::default())
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

                Ok(parse_content_length(headers)?.unwrap_or_default())
            }
            StatusCode::NOT_FOUND => {
                let mut req = self
                    .core
                    .azblob_init_appendable_blob_request(&self.path, &self.op)?;

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
                Ok(0)
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn append(&self, offset: u64, size: u64, body: AsyncBody) -> Result<()> {
        let mut req = self
            .core
            .azblob_append_blob_request(&self.path, offset, size, body)?;

        self.core.sign(&mut req).await?;

        let resp = self.core.send(req).await?;

        let status = resp.status();
        match status {
            StatusCode::CREATED => {
                resp.into_body().consume().await?;
                Ok(())
            }
            _ => Err(parse_error(resp).await?),
        }
    }
}

#[async_trait]
impl oio::BlockWrite for AzblobWriter {
    async fn write_once(&self, size: u64, body: AsyncBody) -> Result<()> {
        let mut req: http::Request<AsyncBody> =
            self.core
                .azblob_put_blob_request(&self.path, Some(size), &self.op, body)?;
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

    async fn write_block(&self, _block_id: Uuid, size: u64, body: AsyncBody) -> Result<()> {
        let resp = self
            .core
            .azblob_put_block_list(&self.path, Some(size), &self.op, body)
            .await?;

        let status = resp.status();
        match status {
            StatusCode::CREATED | StatusCode::OK => {
                resp.into_body().consume().await?;
                Ok(())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn complete_block(&self, block_ids: Vec<Uuid>) -> Result<()> {
        let resp = self
            .core
            .azblob_complete_put_block_list(&self.path, block_ids)
            .await?;

        let status = resp.status();
        match status {
            StatusCode::CREATED | StatusCode::OK => {
                resp.into_body().consume().await?;
                Ok(())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn abort_block(&self, block_ids: Vec<Uuid>) -> Result<()> {
        // Azure blob storage will gc uncommitted blocks after 7 days.
        for block_id in block_ids {
            let resp = self.core.azblob_delete_blob(&block_id.to_string()).await?;
            match resp.status() {
                StatusCode::OK => {
                    resp.into_body().consume().await?;
                }
                _ => return Err(parse_error(resp).await?),
            }
        }
        Ok(())
    }
}
