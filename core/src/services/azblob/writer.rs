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

use http::StatusCode;
use uuid::Uuid;

use super::core::constants::X_MS_VERSION_ID;
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

    // skip extracting `content-md5` here, as it pertains to the content of the request rather than
    // the content of the block itself for the `append` and `complete put block list` operations.
    fn parse_metadata(headers: &http::HeaderMap) -> Result<Metadata> {
        let mut metadata = Metadata::default();

        if let Some(last_modified) = parse_last_modified(headers)? {
            metadata.set_last_modified(last_modified);
        }
        let etag = parse_etag(headers)?;
        if let Some(etag) = etag {
            metadata.set_etag(etag);
        }
        let version_id = parse_header_to_str(headers, X_MS_VERSION_ID)?;
        if let Some(version_id) = version_id {
            metadata.set_version(version_id);
        }

        Ok(metadata)
    }
}

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
                let resp = self
                    .core
                    .azblob_init_appendable_blob(&self.path, &self.op)
                    .await?;

                let status = resp.status();
                match status {
                    StatusCode::CREATED => {
                        // do nothing
                    }
                    _ => {
                        return Err(parse_error(resp));
                    }
                }
                Ok(0)
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn append(&self, offset: u64, size: u64, body: Buffer) -> Result<Metadata> {
        let resp = self
            .core
            .azblob_append_blob(&self.path, offset, size, body)
            .await?;

        let meta = AzblobWriter::parse_metadata(resp.headers())?;
        let status = resp.status();
        match status {
            StatusCode::CREATED => Ok(meta),
            _ => Err(parse_error(resp)),
        }
    }
}

impl oio::BlockWrite for AzblobWriter {
    async fn write_once(&self, size: u64, body: Buffer) -> Result<Metadata> {
        let resp = self
            .core
            .azblob_put_blob(&self.path, Some(size), &self.op, body)
            .await?;

        let status = resp.status();

        let mut meta = AzblobWriter::parse_metadata(resp.headers())?;
        let md5 = parse_content_md5(resp.headers())?;
        if let Some(md5) = md5 {
            meta.set_content_md5(md5);
        }
        match status {
            StatusCode::CREATED | StatusCode::OK => Ok(meta),
            _ => Err(parse_error(resp)),
        }
    }

    async fn write_block(&self, block_id: Uuid, size: u64, body: Buffer) -> Result<()> {
        let resp = self
            .core
            .azblob_put_block(&self.path, block_id, Some(size), &self.op, body)
            .await?;

        let status = resp.status();
        match status {
            StatusCode::CREATED | StatusCode::OK => Ok(()),
            _ => Err(parse_error(resp)),
        }
    }

    async fn complete_block(&self, block_ids: Vec<Uuid>) -> Result<Metadata> {
        let resp = self
            .core
            .azblob_complete_put_block_list(&self.path, block_ids, &self.op)
            .await?;

        let meta = AzblobWriter::parse_metadata(resp.headers())?;
        let status = resp.status();
        match status {
            StatusCode::CREATED | StatusCode::OK => Ok(meta),
            _ => Err(parse_error(resp)),
        }
    }

    async fn abort_block(&self, _block_ids: Vec<Uuid>) -> Result<()> {
        // refer to https://learn.microsoft.com/en-us/rest/api/storageservices/put-block-list?tabs=microsoft-entra-id
        // Any uncommitted blocks are garbage collected if there are no successful calls to Put Block or Put Block List on the blob within a week.
        // If Put Blob is called on the blob, any uncommitted blocks are garbage collected.
        Ok(())
    }
}
