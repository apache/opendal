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

use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use bytes::Buf;
use http::StatusCode;

use super::CLOUDFLARE_KV_SCHEME;
use super::config::CloudflareKvConfig;
use super::core::CloudflareKvCore;
use super::deleter::CloudflareKvDeleter;
use super::error::parse_error;
use super::lister::CloudflareKvLister;
use super::model::*;
use super::writer::CloudflareWriter;
use crate::raw::*;
use crate::*;

#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct CloudflareKvBuilder {
    pub(super) config: CloudflareKvConfig,

    /// The HTTP client used to communicate with CloudFlare.
    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
    pub(super) http_client: Option<HttpClient>,
}

impl Debug for CloudflareKvBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CloudflareKvBuilder")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl CloudflareKvBuilder {
    /// Set the token used to authenticate with CloudFlare.
    pub fn api_token(mut self, api_token: &str) -> Self {
        if !api_token.is_empty() {
            self.config.api_token = Some(api_token.to_string())
        }
        self
    }

    /// Set the account ID used to authenticate with CloudFlare.
    pub fn account_id(mut self, account_id: &str) -> Self {
        if !account_id.is_empty() {
            self.config.account_id = Some(account_id.to_string())
        }
        self
    }

    /// Set the namespace ID.
    pub fn namespace_id(mut self, namespace_id: &str) -> Self {
        if !namespace_id.is_empty() {
            self.config.namespace_id = Some(namespace_id.to_string())
        }
        self
    }

    /// Set the default ttl for cloudflare_kv services.
    ///
    /// If set, we will specify `EX` for write operations.
    pub fn default_ttl(mut self, ttl: Duration) -> Self {
        self.config.default_ttl = Some(ttl);
        self
    }

    /// Set the root within this backend.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }
}

impl Builder for CloudflareKvBuilder {
    type Config = CloudflareKvConfig;

    fn build(self) -> Result<impl Access> {
        let api_token = match &self.config.api_token {
            Some(api_token) => format_authorization_by_bearer(api_token)?,
            None => {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "api_token is required",
                ));
            }
        };

        let Some(account_id) = self.config.account_id.clone() else {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "account_id is required",
            ));
        };

        let Some(namespace_id) = self.config.namespace_id.clone() else {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "namespace_id is required",
            ));
        };

        // Validate default TTL is at least 60 seconds if specified
        if let Some(ttl) = self.config.default_ttl {
            if ttl < Duration::from_secs(60) {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "Default TTL must be at least 60 seconds",
                ));
            }
        }

        let root = normalize_root(
            self.config
                .root
                .clone()
                .unwrap_or_else(|| "/".to_string())
                .as_str(),
        );

        Ok(CloudflareKvBackend {
            core: Arc::new(CloudflareKvCore {
                api_token,
                account_id,
                namespace_id,
                expiration_ttl: self.config.default_ttl,
                info: {
                    let am = AccessorInfo::default();
                    am.set_scheme(CLOUDFLARE_KV_SCHEME)
                        .set_root(&root)
                        .set_native_capability(Capability {
                            create_dir: true,

                            stat: true,
                            stat_with_if_match: true,
                            stat_with_if_none_match: true,
                            stat_with_if_modified_since: true,
                            stat_with_if_unmodified_since: true,

                            read: true,
                            read_with_if_match: true,
                            read_with_if_none_match: true,
                            read_with_if_modified_since: true,
                            read_with_if_unmodified_since: true,

                            write: true,
                            write_can_empty: true,
                            write_total_max_size: Some(25 * 1024 * 1024),

                            list: true,
                            list_with_limit: true,
                            list_with_recursive: true,

                            delete: true,
                            delete_max_size: Some(10000),

                            shared: false,

                            ..Default::default()
                        });

                    // allow deprecated api here for compatibility
                    #[allow(deprecated)]
                    if let Some(client) = self.http_client {
                        am.update_http_client(|_| client);
                    }

                    am.into()
                },
            }),
        })
    }
}

#[derive(Debug, Clone)]
pub struct CloudflareKvBackend {
    core: Arc<CloudflareKvCore>,
}

impl Access for CloudflareKvBackend {
    type Reader = Buffer;
    type Writer = oio::OneShotWriter<CloudflareWriter>;
    type Lister = oio::PageLister<CloudflareKvLister>;
    type Deleter = oio::BatchDeleter<CloudflareKvDeleter>;

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn create_dir(&self, path: &str, _args: OpCreateDir) -> Result<RpCreateDir> {
        let path = build_abs_path(&self.core.info.root(), path);

        if path == build_abs_path(&self.core.info.root(), "") {
            return Ok(RpCreateDir::default());
        }

        // Split path into segments and create directories for each level
        let segments: Vec<&str> = path
            .trim_start_matches('/')
            .trim_end_matches('/')
            .split('/')
            .collect();

        // Create each directory level
        let mut current_path = String::from("/");
        for segment in segments {
            // Build the current directory path
            if !current_path.ends_with('/') {
                current_path.push('/');
            }
            current_path.push_str(segment);
            current_path.push('/');

            // Create metadata for current directory
            let cf_kv_metadata = CfKvMetadata {
                etag: build_tmp_path_of(&current_path),
                last_modified: Timestamp::now().to_string(),
                content_length: 0,
                is_dir: true,
            };

            // Set the directory entry
            self.core
                .set(&current_path, Buffer::new(), cf_kv_metadata)
                .await?;
        }

        Ok(RpCreateDir::default())
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let path = build_abs_path(&self.core.info.root(), path);
        let new_path = path.trim_end_matches('/');

        let resp = self.core.metadata(new_path).await?;

        // Handle non-OK response
        if resp.status() != StatusCode::OK {
            // Special handling for potential directory paths
            if path.ends_with('/') && resp.status() == StatusCode::NOT_FOUND {
                // Try listing the path to check if it's a directory
                let list_resp = self.core.list(&path, None, None).await?;

                if list_resp.status() == StatusCode::OK {
                    let list_body = list_resp.into_body();
                    let list_result: CfKvListResponse = serde_json::from_reader(list_body.reader())
                        .map_err(new_json_deserialize_error)?;

                    // If listing returns results, treat as directory
                    if let Some(entries) = list_result.result {
                        if !entries.is_empty() {
                            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
                        }
                    }

                    // Empty or no results means not found
                    return Err(Error::new(
                        ErrorKind::NotFound,
                        "key not found in CloudFlare KV",
                    ));
                }
            }

            // For all other error cases, parse the error response
            return Err(parse_error(resp));
        }

        let resp_body = resp.into_body();
        let cf_response: CfKvStatResponse =
            serde_json::from_reader(resp_body.reader()).map_err(new_json_deserialize_error)?;

        if !cf_response.success {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "cloudflare_kv stat this key failed for reason we don't know",
            ));
        }

        let metadata = match cf_response.result {
            Some(metadata) => {
                if path.ends_with('/') && !metadata.is_dir {
                    return Err(Error::new(
                        ErrorKind::NotFound,
                        "key not found in CloudFlare KV",
                    ));
                } else {
                    metadata
                }
            }
            None => {
                return Err(Error::new(
                    ErrorKind::NotFound,
                    "key not found in CloudFlare KV",
                ));
            }
        };

        // Check if_match condition
        if let Some(if_match) = &args.if_match() {
            if if_match != &metadata.etag {
                return Err(Error::new(ErrorKind::ConditionNotMatch, "etag mismatch"));
            }
        }

        // Check if_none_match condition
        if let Some(if_none_match) = &args.if_none_match() {
            if if_none_match == &metadata.etag {
                return Err(Error::new(
                    ErrorKind::ConditionNotMatch,
                    "etag match when expected none match",
                ));
            }
        }

        // Parse since time once for both time-based conditions
        let last_modified = metadata
            .last_modified
            .parse::<Timestamp>()
            .map_err(|_| Error::new(ErrorKind::Unsupported, "invalid since format"))?;

        // Check modified_since condition
        if let Some(modified_since) = &args.if_modified_since() {
            if !last_modified.gt(modified_since) {
                return Err(Error::new(
                    ErrorKind::ConditionNotMatch,
                    "not modified since specified time",
                ));
            }
        }

        // Check unmodified_since condition
        if let Some(unmodified_since) = &args.if_unmodified_since() {
            if !last_modified.le(unmodified_since) {
                return Err(Error::new(
                    ErrorKind::ConditionNotMatch,
                    "modified since specified time",
                ));
            }
        }

        let meta = Metadata::new(if metadata.is_dir {
            EntryMode::DIR
        } else {
            EntryMode::FILE
        })
        .with_etag(metadata.etag)
        .with_content_length(metadata.content_length as u64)
        .with_last_modified(metadata.last_modified.parse::<Timestamp>()?);

        Ok(RpStat::new(meta))
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let path = build_abs_path(&self.core.info.root(), path);
        let resp = self.core.get(&path).await?;

        let status = resp.status();

        if status != StatusCode::OK {
            return Err(parse_error(resp));
        }

        let resp_body = resp.into_body();

        if args.if_match().is_some()
            || args.if_none_match().is_some()
            || args.if_modified_since().is_some()
            || args.if_unmodified_since().is_some()
        {
            let meta_resp = self.core.metadata(&path).await?;

            if meta_resp.status() != StatusCode::OK {
                return Err(parse_error(meta_resp));
            }

            let cf_response: CfKvStatResponse =
                serde_json::from_reader(meta_resp.into_body().reader())
                    .map_err(new_json_deserialize_error)?;

            if !cf_response.success && cf_response.result.is_some() {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "cloudflare_kv read this key failed for reason we don't know",
                ));
            }

            let metadata = cf_response.result.unwrap();

            // Check if_match condition
            if let Some(if_match) = &args.if_match() {
                if if_match != &metadata.etag {
                    return Err(Error::new(ErrorKind::ConditionNotMatch, "etag mismatch"));
                }
            }

            // Check if_none_match condition
            if let Some(if_none_match) = &args.if_none_match() {
                if if_none_match == &metadata.etag {
                    return Err(Error::new(
                        ErrorKind::ConditionNotMatch,
                        "etag match when expected none match",
                    ));
                }
            }

            // Parse since time once for both time-based conditions
            let last_modified = metadata
                .last_modified
                .parse::<Timestamp>()
                .map_err(|_| Error::new(ErrorKind::Unsupported, "invalid since format"))?;

            // Check modified_since condition
            if let Some(modified_since) = &args.if_modified_since() {
                if !last_modified.gt(modified_since) {
                    return Err(Error::new(
                        ErrorKind::ConditionNotMatch,
                        "not modified since specified time",
                    ));
                }
            }

            // Check unmodified_since condition
            if let Some(unmodified_since) = &args.if_unmodified_since() {
                if !last_modified.le(unmodified_since) {
                    return Err(Error::new(
                        ErrorKind::ConditionNotMatch,
                        "modified since specified time",
                    ));
                }
            }
        }

        let range = args.range();
        let buffer = if range.is_full() {
            resp_body
        } else {
            let start = range.offset() as usize;
            let end = match range.size() {
                Some(size) => (range.offset() + size) as usize,
                None => resp_body.len(),
            };
            resp_body.slice(start..end.min(resp_body.len()))
        };
        Ok((RpRead::new(), buffer))
    }

    async fn write(&self, path: &str, _: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let path = build_abs_path(&self.core.info.root(), path);
        let writer = CloudflareWriter::new(self.core.clone(), path);

        let w = oio::OneShotWriter::new(writer);

        Ok((RpWrite::default(), w))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::BatchDeleter::new(
                CloudflareKvDeleter::new(self.core.clone()),
                self.core.info.full_capability().delete_max_size,
            ),
        ))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let path = build_abs_path(&self.core.info.root(), path);

        let limit = match args.limit() {
            Some(limit) => {
                // The list limit of cloudflare_kv is limited to 10..1000.
                if !(10..=1000).contains(&limit) {
                    1000
                } else {
                    limit
                }
            }
            None => 1000,
        };

        let l = CloudflareKvLister::new(self.core.clone(), &path, args.recursive(), Some(limit));

        Ok((RpList::default(), oio::PageLister::new(l)))
    }
}
