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

use super::core::AzfileCore;
use super::core::parse_error;
use opendal_core::raw::*;
use opendal_core::*;

// Azure Files limits each Put Range update to 4 MiB.
// https://learn.microsoft.com/en-us/rest/api/storageservices/put-range
const AZFILE_MAX_WRITE_SIZE: usize = 4 * 1024 * 1024;

pub type AzfileWriters = TwoWays<oio::OneShotWriter<AzfileWriter>, oio::AppendWriter<AzfileWriter>>;

pub struct AzfileWriter {
    core: Arc<AzfileCore>,
    ctx: OperationContext,
    op: OpWrite,
    path: String,
}

impl AzfileWriter {
    pub fn new(core: Arc<AzfileCore>, ctx: OperationContext, op: OpWrite, path: String) -> Self {
        AzfileWriter {
            core,
            ctx,
            op,
            path,
        }
    }

    async fn ensure_parent_dir_exists(&self) -> Result<()> {
        self.core
            .ensure_parent_dir_exists(&self.ctx, &self.path)
            .await
    }

    async fn write_ranges(&self, offset: u64, bs: Buffer) -> Result<Metadata> {
        let size = bs.len();
        let mut position = 0;
        let mut metadata;

        loop {
            let chunk_size = (size - position).min(AZFILE_MAX_WRITE_SIZE);
            let end = position + chunk_size;
            let body = bs.slice(position..end);

            let resp = self
                .core
                .azfile_update(
                    &self.ctx,
                    &self.path,
                    chunk_size as u64,
                    offset + position as u64,
                    body,
                )
                .await?;
            let status = resp.status();
            match status {
                StatusCode::OK | StatusCode::CREATED => {
                    metadata = AzfileWriter::parse_metadata(resp.headers())?;
                }
                _ => {
                    return Err(parse_error(resp).with_operation("Backend::azfile_update"));
                }
            }

            position = end;
            if position == size {
                break;
            }
        }

        metadata.set_content_length(offset + size as u64);
        Ok(metadata)
    }

    fn parse_metadata(headers: &http::HeaderMap) -> Result<Metadata> {
        let mut metadata = Metadata::default();

        if let Some(last_modified) = parse_last_modified(headers)? {
            metadata.set_last_modified(last_modified);
        }
        let etag = parse_etag(headers)?;
        if let Some(etag) = etag {
            metadata.set_etag(etag);
        }

        Ok(metadata)
    }
}

impl oio::OneShotWrite for AzfileWriter {
    async fn write_once(&self, bs: Buffer) -> Result<Metadata> {
        self.ensure_parent_dir_exists().await?;

        let size = bs.len();
        let resp = self
            .core
            .azfile_create_file(&self.ctx, &self.path, size, &self.op)
            .await?;

        let status = resp.status();
        match status {
            StatusCode::OK | StatusCode::CREATED => {}
            _ => {
                return Err(parse_error(resp).with_operation("Backend::azfile_create_file"));
            }
        }

        self.write_ranges(0, bs).await
    }
}

impl oio::AppendWrite for AzfileWriter {
    async fn offset(&self) -> Result<u64> {
        self.ensure_parent_dir_exists().await?;

        let resp = self
            .core
            .azfile_get_file_properties(&self.ctx, &self.path)
            .await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(parse_content_length(resp.headers())?.unwrap_or_default()),
            _ => Err(parse_error(resp)),
        }
    }

    async fn append(&self, offset: u64, _size: u64, body: Buffer) -> Result<Metadata> {
        self.write_ranges(offset, body).await
    }
}
