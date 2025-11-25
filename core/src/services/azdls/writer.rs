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

use super::core::AzdlsCore;
use super::core::FILE;
use super::core::X_MS_VERSION_ID;
use super::error::parse_error;
use crate::raw::*;
use crate::*;

/// Writer type for azdls: non-append uses PositionWriter, append uses AppendWriter.
pub type AzdlsWriters = TwoWays<oio::PositionWriter<AzdlsWriter>, oio::AppendWriter<AzdlsWriter>>;

#[derive(Clone)]
pub struct AzdlsWriter {
    core: Arc<AzdlsCore>,
    op: OpWrite,
    path: String,
}

impl AzdlsWriter {
    pub fn new(core: Arc<AzdlsCore>, op: OpWrite, path: String) -> Self {
        Self { core, op, path }
    }

    pub async fn create(core: Arc<AzdlsCore>, op: OpWrite, path: String) -> Result<Self> {
        let writer = Self::new(core, op, path);
        writer.create_if_needed().await?;
        Ok(writer)
    }

    async fn create_if_needed(&self) -> Result<()> {
        let resp = self.core.azdls_create(&self.path, FILE, &self.op).await?;
        match resp.status() {
            StatusCode::CREATED | StatusCode::OK => Ok(()),
            StatusCode::CONFLICT if self.op.if_not_exists() => {
                Err(parse_error(resp).with_operation("Backend::azdls_create_request"))
            }
            StatusCode::CONFLICT => Ok(()),
            _ => Err(parse_error(resp).with_operation("Backend::azdls_create_request")),
        }
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
        let version_id = parse_header_to_str(headers, X_MS_VERSION_ID)?;
        if let Some(version_id) = version_id {
            metadata.set_version(version_id);
        }

        Ok(metadata)
    }
}

impl oio::PositionWrite for AzdlsWriter {
    async fn write_all_at(&self, offset: u64, buf: Buffer) -> Result<()> {
        let size = buf.len() as u64;
        let resp = self
            .core
            .azdls_append(&self.path, Some(size), offset, false, false, buf)
            .await?;

        match resp.status() {
            StatusCode::OK | StatusCode::ACCEPTED => Ok(()),
            _ => Err(parse_error(resp).with_operation("Backend::azdls_append_request")),
        }
    }

    async fn close(&self, size: u64) -> Result<Metadata> {
        // Flush accumulated appends once.
        let resp = self.core.azdls_flush(&self.path, size, true).await?;

        let mut meta = AzdlsWriter::parse_metadata(resp.headers())?;
        meta.set_content_length(size);

        match resp.status() {
            StatusCode::OK | StatusCode::ACCEPTED => Ok(meta),
            _ => Err(parse_error(resp).with_operation("Backend::azdls_flush_request")),
        }
    }

    async fn abort(&self) -> Result<()> {
        Ok(())
    }
}

impl oio::AppendWrite for AzdlsWriter {
    async fn offset(&self) -> Result<u64> {
        let resp = self.core.azdls_get_properties(&self.path).await?;

        let status = resp.status();
        let headers = resp.headers();

        match status {
            StatusCode::OK => Ok(parse_content_length(headers)?.unwrap_or_default()),
            StatusCode::NOT_FOUND => Ok(0),
            _ => Err(parse_error(resp)),
        }
    }

    async fn append(&self, offset: u64, size: u64, body: Buffer) -> Result<Metadata> {
        if offset == 0 {
            // Only create when starting a new file; avoid 404 when appending to a non-existent path.
            self.create_if_needed().await?;
        }

        // append + flush in a single request to minimize roundtrips for append mode.
        let resp = self
            .core
            .azdls_append(&self.path, Some(size), offset, true, false, body)
            .await?;

        let mut meta = AzdlsWriter::parse_metadata(resp.headers())?;
        let md5 = parse_content_md5(resp.headers())?;
        if let Some(md5) = md5 {
            meta.set_content_md5(md5);
        }
        meta.set_content_length(offset + size);

        match resp.status() {
            StatusCode::OK | StatusCode::ACCEPTED => Ok(meta),
            _ => Err(parse_error(resp).with_operation("Backend::azdls_append_request")),
        }
    }
}
