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

use super::core::*;
use super::error::parse_error;
use crate::raw::*;
use crate::services::core::AzblobCore;
use crate::services::writer::AzblobWriter;
use crate::*;
use http::header::{ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE};
use http::Request;
use std::str::FromStr;
use std::sync::Arc;

pub type GhacWriter = TwoWays<GhacWriterV1, GhacWriterV2>;

impl GhacWriter {
    /// TODO: maybe we can move the signed url logic to azblob service instead.
    pub fn new(core: Arc<GhacCore>, write_path: String, url: String) -> Result<Self> {
        match core.service_version {
            GhacVersion::V1 => Ok(TwoWays::One(GhacWriterV1 {
                core,
                path: write_path,
                url,
                size: 0,
            })),
            GhacVersion::V2 => {
                let uri = http::Uri::from_str(&url)
                    .map_err(new_http_uri_invalid_error)?
                    .into_parts();
                let (Some(scheme), Some(authority), Some(pq)) =
                    (uri.scheme, uri.authority, uri.path_and_query)
                else {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        "ghac returns invalid signed url",
                    )
                    .with_context("url", &url));
                };
                let endpoint = format!("{scheme}://{authority}");
                let Some((container, path)) = pq.path().trim_matches('/').split_once("/") else {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        "ghac returns invalid signed url that bucket or path is missing",
                    )
                    .with_context("url", &url));
                };
                let Some(query) = pq.query() else {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        "ghac returns invalid signed url that sas is missing",
                    )
                    .with_context("url", &url));
                };
                let azure_core = Arc::new(AzblobCore {
                    info: {
                        let am = AccessorInfo::default();
                        am.set_scheme(Scheme::Azblob)
                            .set_root("/")
                            .set_name(container)
                            .set_native_capability(Capability {
                                stat: true,
                                stat_with_if_match: true,
                                stat_with_if_none_match: true,
                                stat_has_cache_control: true,
                                stat_has_content_length: true,
                                stat_has_content_type: true,
                                stat_has_content_encoding: true,
                                stat_has_content_range: true,
                                stat_has_etag: true,
                                stat_has_content_md5: true,
                                stat_has_last_modified: true,
                                stat_has_content_disposition: true,

                                read: true,

                                read_with_if_match: true,
                                read_with_if_none_match: true,
                                read_with_override_content_disposition: true,
                                read_with_if_modified_since: true,
                                read_with_if_unmodified_since: true,

                                write: true,
                                write_can_append: true,
                                write_can_empty: true,
                                write_can_multi: true,
                                write_with_cache_control: true,
                                write_with_content_type: true,
                                write_with_if_not_exists: true,
                                write_with_if_none_match: true,
                                write_with_user_metadata: true,

                                copy: true,

                                list: true,
                                list_with_recursive: true,
                                list_has_etag: true,
                                list_has_content_length: true,
                                list_has_content_md5: true,
                                list_has_content_type: true,
                                list_has_last_modified: true,

                                shared: true,

                                ..Default::default()
                            });

                        am.into()
                    },
                    container: container.to_string(),
                    root: "/".to_string(),
                    endpoint,
                    encryption_key: None,
                    encryption_key_sha256: None,
                    encryption_algorithm: None,
                    loader: {
                        let config = reqsign::AzureStorageConfig {
                            sas_token: Some(query.to_string()),
                            ..Default::default()
                        };
                        reqsign::AzureStorageLoader::new(config)
                    },
                    signer: { reqsign::AzureStorageSigner::new() },
                });
                let w = AzblobWriter::new(azure_core, OpWrite::default(), path.to_string());
                let writer = oio::BlockWriter::new(core.info.clone(), w, 4);
                Ok(TwoWays::Two(GhacWriterV2 {
                    core,
                    writer,
                    path: write_path,
                    url,
                    size: 0,
                }))
            }
        }
    }
}

pub struct GhacWriterV1 {
    core: Arc<GhacCore>,

    path: String,
    url: String,
    size: u64,
}

impl oio::Write for GhacWriterV1 {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let size = bs.len() as u64;
        let offset = self.size;

        let mut req = Request::patch(&self.url);
        req = req.header(AUTHORIZATION, format!("Bearer {}", self.core.catch_token));
        req = req.header(ACCEPT, CACHE_HEADER_ACCEPT);
        req = req.header(CONTENT_LENGTH, size);
        req = req.header(CONTENT_TYPE, "application/octet-stream");
        req = req.header(
            CONTENT_RANGE,
            BytesContentRange::default()
                .with_range(offset, offset + size - 1)
                .to_header(),
        );
        let req = req.body(bs).map_err(new_request_build_error)?;

        let resp = self.core.info.http_client().send(req).await?;
        if !resp.status().is_success() {
            return Err(parse_error(resp).map(|err| err.with_operation("Backend::ghac_upload")));
        }
        self.size += size;
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.core
            .ghac_finalize_upload(&self.path, &self.url, self.size)
            .await?;
        Ok(Metadata::default().with_content_length(self.size))
    }
}

pub struct GhacWriterV2 {
    core: Arc<GhacCore>,
    writer: oio::BlockWriter<AzblobWriter>,

    path: String,
    url: String,
    size: u64,
}

impl oio::Write for GhacWriterV2 {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let size = bs.len() as u64;

        self.writer.write(bs).await?;
        self.size += size;
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.writer.close().await?;
        let _ = self
            .core
            .ghac_finalize_upload(&self.path, &self.url, self.size)
            .await;
        Ok(Metadata::default().with_content_length(self.size))
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }
}
