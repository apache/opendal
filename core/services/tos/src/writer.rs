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

use constants::X_TOS_OBJECT_SIZE;
use constants::X_TOS_VERSION_ID;
use http::StatusCode;

use crate::core::*;
use crate::error::parse_error;
use crate::utils::tos_parse_etag;
use opendal_core::raw::*;
use opendal_core::*;

pub struct TosWriter {
    core: Arc<TosCore>,

    op: OpWrite,
    path: String,
}

impl TosWriter {
    pub fn new(core: Arc<TosCore>, path: &str, op: OpWrite) -> Self {
        TosWriter {
            core,
            path: path.to_string(),
            op,
        }
    }

    fn parse_header_into_meta(path: &str, headers: &http::HeaderMap) -> Result<Metadata> {
        let mut meta = Metadata::new(EntryMode::from_path(path));
        if let Some(etag) = tos_parse_etag(headers)? {
            meta.set_etag(etag);
        }
        if let Some(version) = parse_header_to_str(headers, X_TOS_VERSION_ID)? {
            meta.set_version(version);
        }
        if let Some(value) =
            parse_header_to_str(headers, X_TOS_OBJECT_SIZE)?.and_then(|size| size.parse().ok())
        {
            meta.set_content_length(value);
        }
        Ok(meta)
    }
}

impl oio::MultipartWrite for TosWriter {
    async fn write_once(&self, size: u64, body: Buffer) -> Result<Metadata> {
        let req = self
            .core
            .tos_put_object_request(&self.path, Some(size), &self.op, body)?;

        let resp = self.core.send(req).await?;

        let status = resp.status();

        let meta = TosWriter::parse_header_into_meta(&self.path, resp.headers())?;

        match status {
            StatusCode::CREATED | StatusCode::OK => Ok(meta),
            _ => Err(parse_error(resp)),
        }
    }

    async fn initiate_part(&self) -> Result<String> {
        // TODO: Implement multipart upload in follow-up PR
        Err(Error::new(
            ErrorKind::Unexpected,
            "Multipart upload not yet implemented for TOS",
        ))
    }

    async fn write_part(
        &self,
        _upload_id: &str,
        _part_number: usize,
        _size: u64,
        _body: Buffer,
    ) -> Result<oio::MultipartPart> {
        // TODO: Implement multipart upload in follow-up PR
        Err(Error::new(
            ErrorKind::Unexpected,
            "Multipart upload not yet implemented for TOS",
        ))
    }

    async fn complete_part(
        &self,
        _upload_id: &str,
        _parts: &[oio::MultipartPart],
    ) -> Result<Metadata> {
        // TODO: Implement multipart upload in follow-up PR
        Err(Error::new(
            ErrorKind::Unexpected,
            "Multipart upload not yet implemented for TOS",
        ))
    }

    async fn abort_part(&self, _upload_id: &str) -> Result<()> {
        // TODO: Implement multipart upload in follow-up PR
        Err(Error::new(
            ErrorKind::Unexpected,
            "Multipart upload not yet implemented for TOS",
        ))
    }
}
