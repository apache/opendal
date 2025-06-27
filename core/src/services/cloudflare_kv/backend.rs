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

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::time::Duration;

use bytes::Buf;
use http::StatusCode;
use serde::Deserialize;
use serde::Serialize;

use crate::raw::*;
use crate::services::cloudflare_kv::core::CloudflareKvCore;
use crate::services::cloudflare_kv::delete::CloudflareKvDeleter;
use crate::services::cloudflare_kv::error::parse_error;
use crate::services::cloudflare_kv::lister::CloudflareKvLister;
use crate::services::cloudflare_kv::writer::CloudflareWriter;
use crate::services::CloudflareKvConfig;
use crate::ErrorKind;
use crate::*;

impl Configurator for CloudflareKvConfig {
    type Builder = CloudflareKvBuilder;
    fn into_builder(self) -> Self::Builder {
        CloudflareKvBuilder {
            config: self,
            http_client: None,
        }
    }
}

#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct CloudflareKvBuilder {
    config: CloudflareKvConfig,

    /// The HTTP client used to communicate with CloudFlare.
    http_client: Option<HttpClient>,
}

impl Debug for CloudflareKvBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CloudFlareKvBuilder")
            .field("config", &self.config)
            .finish()
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
    const SCHEME: Scheme = Scheme::CloudflareKv;
    type Config = CloudflareKvConfig;

    fn build(self) -> Result<impl Access> {
        let api_token = match &self.config.api_token {
            Some(api_token) => format_authorization_by_bearer(api_token)?,
            None => {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "api_token is required",
                ))
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

        let root = normalize_root(&self.config.root.unwrap_or_default());

        Ok(CloudflareKvAccessor {
            core: Arc::new(CloudflareKvCore {
                api_token,
                account_id,
                namespace_id,
                expiration_ttl: self.config.default_ttl,
                info: {
                    let am = AccessorInfo::default();
                    am.set_scheme(Scheme::CloudflareKv)
                        .set_root(&root)
                        .set_native_capability(Capability {
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
                            write_total_max_size: Some(25 * 1024 * 1024),

                            list: true,
                            list_with_limit: true,
                            list_with_recursive: true,

                            delete: true,
                            delete_max_size: Some(10000),

                            shared: true,

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
pub struct CloudflareKvAccessor {
    core: std::sync::Arc<CloudflareKvCore>,
}

impl Access for CloudflareKvAccessor {
    type Reader = Buffer;
    type Writer = oio::OneShotWriter<CloudflareWriter>;
    type Lister = oio::PageLister<CloudflareKvLister>;
    type Deleter = oio::BatchDeleter<CloudflareKvDeleter>;

    fn info(&self) -> std::sync::Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let resp = self.core.metadata(path).await?;

        if resp.status() != StatusCode::OK {
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
            Some(metadata) => metadata,
            None => {
                return Err(Error::new(
                    ErrorKind::NotFound,
                    "key not found in CloudFlare KV",
                ))
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
        let last_modified = chrono::DateTime::parse_from_rfc3339(&metadata.last_modified)
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

        let meta = Metadata::new(EntryMode::FILE)
            .with_etag(metadata.etag)
            .with_content_length(metadata.content_length as u64)
            .with_last_modified(parse_datetime_from_rfc3339(&metadata.last_modified)?);

        Ok(RpStat::new(meta))
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.core.batch_get(&[path.to_string()]).await?;

        let status = resp.status();

        if status != StatusCode::OK {
            return Err(parse_error(resp));
        }

        let resp_body = resp.into_body();
        let cf_response: CfKvGetResponse =
            serde_json::from_reader(resp_body.reader()).map_err(new_json_deserialize_error)?;

        if !cf_response.success {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "cloudflare_kv read this key failed for reason we don't know",
            ));
        }

        // Extract data from response and handle not found cases
        let data = cf_response
            .result
            .and_then(|result| result.values)
            .and_then(|values| values.into_iter().next())
            .and_then(|(_, data)| data)
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "key not found in CloudFlare KV"))?;

        // Check if_match condition
        if let Some(if_match) = &args.if_match() {
            if if_match != &data.metadata.etag {
                return Err(Error::new(ErrorKind::ConditionNotMatch, "etag mismatch"));
            }
        }

        // Check if_none_match condition
        if let Some(if_none_match) = &args.if_none_match() {
            if if_none_match == &data.metadata.etag {
                return Err(Error::new(
                    ErrorKind::ConditionNotMatch,
                    "etag match when expected none match",
                ));
            }
        }

        // Parse since time once for both time-based conditions
        let last_modified = chrono::DateTime::parse_from_rfc3339(&data.metadata.last_modified)
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

        Ok((RpRead::new(), Buffer::from(data.value.clone())))
    }

    async fn write(&self, path: &str, _: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let writer = CloudflareWriter::new(self.core.clone(), path.to_string());

        let w = oio::OneShotWriter::new(writer);

        Ok((RpWrite::default(), w))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::BatchDeleter::new(CloudflareKvDeleter::new(self.core.clone())),
        ))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        if let Some(limit) = args.limit() {
            if !(10..=1000).contains(&limit) {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "limit must be between 10 and 1000, default 1000.",
                ));
            }
        }
        let l = CloudflareKvLister::new(self.core.clone(), path, args.limit());

        Ok((RpList::default(), oio::PageLister::new(l)))
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct CfKvResponse {
    pub(super) errors: Vec<CfKvError>,
}

#[derive(Debug, Deserialize)]
pub(super) struct CfKvGetResponse {
    success: bool,
    result: Option<CfKvGetResult>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CfKvMetadata {
    pub etag: String,
    pub last_modified: String,
    pub content_length: usize,
}

#[derive(Debug, Deserialize)]
struct CfKvGetResultData {
    value: String,
    metadata: CfKvMetadata,
    // expiration: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct CfKvGetResult {
    values: Option<HashMap<String, Option<CfKvGetResultData>>>,
}

#[derive(Debug, Deserialize)]
pub(super) struct CfKvError {
    pub(super) code: i32,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CfKvSetData {
    pub key: String,
    pub value: String,
    pub metadata: CfKvMetadata,
}

#[derive(Debug, Deserialize)]
pub struct CfKvDeleteResult {
    pub successful_key_count: usize,
    pub unsuccessful_keys: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct CfKvDeleteResponse {
    pub success: bool,
    pub result: Option<CfKvDeleteResult>,
}

#[derive(Debug, Deserialize)]
pub struct CfKvListKey {
    pub name: String,
    pub metadata: CfKvMetadata,
    // expiration: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct CfKvListResultInfo {
    // pub count: Option<usize>,
    pub cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CfKvListResponse {
    pub success: bool,
    pub result_info: Option<CfKvListResultInfo>,
    pub result: Option<Vec<CfKvListKey>>,
}

#[derive(Debug, Deserialize)]
pub struct CfKvStatResponse {
    pub success: bool,
    pub result: Option<CfKvMetadata>,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_deserialize_get_json_response() {
        let json_str = r#"{
			"errors": [],
			"messages": [],
			"result": {
                "values": {
                    "key1": {
                        "expiration": 1577836800,
                        "metadata": {
                            "etag": "xxxx",
                            "last_modified": "xx",
                            "content_length": 0
                        },
                        "value": "My-Key"
                    }
                }
            },
			"success": true
		}"#;

        let response: CfKvGetResponse = serde_json::from_slice(json_str.as_bytes()).unwrap();

        assert!(response.success);
        let values = response.result.unwrap().values.unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(
            values.get("key1").as_ref().unwrap().as_ref().unwrap().value,
            "My-Key"
        );
    }

    #[test]
    fn test_deserialize_json_response() {
        let json_str = r#"{
			"errors": [],
			"messages": [],
			"result": {},
			"success": true
		}"#;

        let response: CfKvResponse = serde_json::from_slice(json_str.as_bytes()).unwrap();

        assert_eq!(response.errors.len(), 0);
    }
}
