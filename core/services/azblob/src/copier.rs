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

use super::core::AzblobCore;
use super::core::constants::AZBLOB_COPY_MAX_BLOCK_SIZE;
use super::core::constants::AZBLOB_COPY_MIN_BLOCK_SIZE;
use super::core::constants::X_MS_VERSION_ID;
use super::error::parse_error;
use super::writer::AzblobWriter;
use opendal_core::raw::oio::BlockCopy;
use opendal_core::raw::*;
use opendal_core::*;

pub type AzblobCopiers = TwoWays<oio::OneShotCopier, oio::BlockCopier<AzblobCopier>>;

pub fn new_azblob_copier(
    core: Arc<AzblobCore>,
    from: &str,
    to: &str,
    args: OpCopy,
    opts: OpCopier,
) -> Result<AzblobCopiers> {
    let info = core.info.clone();
    let copier = AzblobCopier {
        core,
        from: from.to_string(),
        to: to.to_string(),
        args,
    };

    let Some(chunk) = opts.chunk() else {
        return Ok(TwoWays::One(oio::OneShotCopier::new(async move {
            copier.copy_once().await
        })));
    };

    let block_size = chunk.clamp(AZBLOB_COPY_MIN_BLOCK_SIZE, AZBLOB_COPY_MAX_BLOCK_SIZE) as u64;

    Ok(TwoWays::Two(oio::BlockCopier::new(
        info,
        copier,
        opts.source_content_length_hint(),
        block_size.saturating_sub(1),
        block_size,
        opts.concurrent(),
    )))
}

pub struct AzblobCopier {
    core: Arc<AzblobCore>,
    from: String,
    to: String,
    args: OpCopy,
}

impl oio::BlockCopy for AzblobCopier {
    async fn source_metadata(&self) -> Result<Metadata> {
        let mut args = OpStat::default();
        if let Some(version) = self.args.source_version() {
            args = args.with_version(version);
        }

        let resp = self
            .core
            .azblob_get_blob_properties(&self.from, &args)
            .await?;

        match resp.status() {
            StatusCode::OK => {
                let headers = resp.headers();
                let mut meta = parse_into_metadata(&self.from, headers)?;
                if let Some(version_id) = parse_header_to_str(headers, X_MS_VERSION_ID)? {
                    meta.set_version(version_id);
                }
                Ok(meta)
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn copy_once(&self) -> Result<Metadata> {
        let resp = self
            .core
            .azblob_copy_blob(&self.from, &self.to, self.args.clone())
            .await?;

        match resp.status() {
            StatusCode::ACCEPTED => AzblobWriter::parse_metadata(resp.headers()),
            _ => Err(parse_error(resp)),
        }
    }

    async fn copy_block(&self, block_id: Uuid, range: BytesRange) -> Result<()> {
        let resp = self
            .core
            .azblob_put_block_from_url(
                &self.from,
                &self.to,
                self.args.source_version(),
                block_id,
                range,
            )
            .await?;

        match resp.status() {
            StatusCode::CREATED | StatusCode::OK => Ok(()),
            _ => Err(parse_error(resp)),
        }
    }

    async fn complete_block(&self, block_ids: Vec<Uuid>) -> Result<Metadata> {
        let resp = self
            .core
            .azblob_complete_copy_block_list(&self.to, block_ids, &self.args)
            .await?;

        let meta = AzblobWriter::parse_metadata(resp.headers())?;
        match resp.status() {
            StatusCode::CREATED | StatusCode::OK => Ok(meta),
            _ => Err(parse_error(resp)),
        }
    }

    async fn abort_block(&self, _block_ids: Vec<Uuid>) -> Result<()> {
        Ok(())
    }
}
