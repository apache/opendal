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

use bytes::Buf;
use http::StatusCode;
use opendal_core::raw::*;
use opendal_core::*;

use super::CLOUDFLARE_KV_SCHEME;
use super::config::CloudflareKvConfig;
use super::core::parse_error;
use super::core::{CloudflareKvCore, ErrorContext};
use super::deleter::CloudflareKvDeleter;
use super::lister::CloudflareKvLister;
use super::model::*;
use super::reader::*;
use super::writer::CloudflareWriter;

#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct CloudflareKvBuilder {
    pub(super) config: CloudflareKvConfig,
    pub(super) default_ttl: Option<Duration>,
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
        self.default_ttl = Some(ttl);
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

    fn build(self) -> Result<impl Service> {
        let default_ttl = match self.default_ttl {
            Some(ttl) => Some(ttl),
            None => self
                .config
                .default_ttl
                .map(signed_duration_to_duration)
                .transpose()?,
        };
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
        if let Some(ttl) = default_ttl
            && ttl < Duration::from_secs(60)
        {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "Default TTL must be at least 60 seconds",
            ));
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
                expiration_ttl: default_ttl,
                info: ServiceInfo::new(CLOUDFLARE_KV_SCHEME, &root, ""),
                capability: Capability {
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

                    ..Default::default()
                },
            }),
        })
    }
}

#[derive(Debug, Clone)]
pub struct CloudflareKvBackend {
    pub(crate) core: Arc<CloudflareKvCore>,
}

impl Service for CloudflareKvBackend {
    type Reader = oio::StreamReader<CloudflareKvReader>;
    type Writer = oio::OneShotWriter<CloudflareWriter>;
    type Lister = oio::PageLister<CloudflareKvLister>;
    type Deleter = oio::BatchDeleter<CloudflareKvDeleter>;
    type Copier = ();
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        self.core.info.clone()
    }

    fn capability(&self) -> Capability {
        self.core.capability
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        _args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        let path = build_absolute_path(&self.core.info.root(), path);

        if path == build_absolute_path(&self.core.info.root(), "") {
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
            let resp = self
                .core
                .set(ctx, &current_path, Buffer::new(), cf_kv_metadata)
                .await?;
            if resp.status() != StatusCode::OK {
                return Err(parse_error(
                    ErrorContext::new(ServiceOperation("WriteValue")),
                    resp,
                ));
            }
        }

        Ok(RpCreateDir::default())
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        let path = build_absolute_path(&self.core.info.root(), path);
        let new_path = path.trim_end_matches('/');

        let resp = self.core.metadata(ctx, new_path).await?;

        // Handle non-OK response
        if resp.status() != StatusCode::OK {
            // Special handling for potential directory paths
            if path.ends_with('/') && resp.status() == StatusCode::NOT_FOUND {
                // Try listing the path to check if it's a directory
                let list_resp = self.core.list(ctx, &path, None, None).await?;

                if list_resp.status() != StatusCode::OK {
                    return Err(parse_error(
                        ErrorContext::new(ServiceOperation("ListKeys")),
                        list_resp,
                    ));
                }

                let list_body = list_resp.clone().into_body();
                let list_result: CfKvListResponse = serde_json::from_reader(list_body.reader())
                    .map_err(new_json_deserialize_error)?;

                if !list_result.success {
                    return Err(parse_error(
                        ErrorContext::new(ServiceOperation("ListKeys")),
                        list_resp,
                    ));
                }

                // If listing returns results, treat as directory
                if let Some(entries) = list_result.result
                    && !entries.is_empty()
                {
                    return Ok(RpStat::new(MetadataBuilder::dir().build()));
                }

                // Empty or no results means not found
                return Err(Error::new(
                    ErrorKind::NotFound,
                    "key not found in CloudFlare KV",
                ));
            }

            // For all other error cases, parse the error response
            return Err(parse_error(
                ErrorContext::new(ServiceOperation("GetMetadata")),
                resp,
            ));
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
        if let Some(if_match) = &args.if_match()
            && if_match != &metadata.etag
        {
            return Err(Error::new(ErrorKind::ConditionNotMatch, "etag mismatch"));
        }

        // Check if_none_match condition
        if let Some(if_none_match) = &args.if_none_match()
            && if_none_match == &metadata.etag
        {
            return Err(Error::new(
                ErrorKind::ConditionNotMatch,
                "etag match when expected none match",
            ));
        }

        // Parse since time once for both time-based conditions
        let last_modified = metadata
            .last_modified
            .parse::<Timestamp>()
            .map_err(|_| Error::new(ErrorKind::Unsupported, "invalid since format"))?;

        // Check modified_since condition
        if let Some(modified_since) = &args.if_modified_since()
            && !last_modified.gt(modified_since)
        {
            return Err(Error::new(
                ErrorKind::ConditionNotMatch,
                "not modified since specified time",
            ));
        }

        // Check unmodified_since condition
        if let Some(unmodified_since) = &args.if_unmodified_since()
            && !last_modified.le(unmodified_since)
        {
            return Err(Error::new(
                ErrorKind::ConditionNotMatch,
                "modified since specified time",
            ));
        }

        let mut meta = if metadata.is_dir {
            MetadataBuilder::dir()
        } else {
            MetadataBuilder::file(metadata.content_length as u64)
        };
        meta.etag(metadata.etag)
            .last_modified(metadata.last_modified.parse::<Timestamp>()?);

        Ok(RpStat::new(meta.build()))
    }
    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let output: oio::StreamReader<CloudflareKvReader> = {
            Ok(oio::StreamReader::new(CloudflareKvReader::new(
                self.clone(),
                ctx.clone(),
                path,
                args,
            )))
        }?;

        Ok(output)
    }

    fn write(&self, ctx: &OperationContext, path: &str, _: OpWrite) -> Result<Self::Writer> {
        let output: oio::OneShotWriter<CloudflareWriter> = {
            let path = build_absolute_path(&self.core.info.root(), path);
            let writer = CloudflareWriter::new(self.core.clone(), ctx.clone(), path);

            let w = oio::OneShotWriter::new(writer);

            Ok(w)
        }?;

        Ok(output)
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        let output: oio::BatchDeleter<CloudflareKvDeleter> = {
            Ok(oio::BatchDeleter::new(
                CloudflareKvDeleter::new(self.core.clone(), ctx.clone()),
                self.core.capability.delete_max_size,
            ))
        }?;

        Ok(output)
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        let output: oio::PageLister<CloudflareKvLister> = {
            let path = build_absolute_path(&self.core.info.root(), path);

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

            let l = CloudflareKvLister::new(
                self.core.clone(),
                ctx.clone(),
                &path,
                args.recursive(),
                Some(limit),
            );

            Ok(oio::PageLister::new(l))
        }?;

        Ok(output)
    }

    fn copy(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpCopy,
    ) -> Result<Self::Copier> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn rename(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpRename,
    ) -> Result<RpRename> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn presign(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpPresign,
    ) -> Result<RpPresign> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::time::Duration;

    use futures::stream;
    use http::Response;

    use super::*;

    struct SequenceTransport {
        responses: Mutex<VecDeque<(StatusCode, String)>>,
    }

    impl SequenceTransport {
        fn new(responses: Vec<(StatusCode, &str)>) -> Self {
            Self {
                responses: Mutex::new(
                    responses
                        .into_iter()
                        .map(|(status, body)| (status, body.to_string()))
                        .collect(),
                ),
            }
        }
    }

    impl HttpTransport for SequenceTransport {
        async fn fetch(&self, _req: http::Request<Buffer>) -> Result<Response<HttpBody>> {
            let (status, body_str) = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected extra HTTP request");
            let body = Buffer::from(body_str);
            let len = body.len() as u64;
            Response::builder()
                .status(status)
                .body(HttpBody::new(stream::iter([Ok(body)]), Some(len)))
                .map_err(new_request_build_error)
        }
    }

    fn test_backend() -> CloudflareKvBackend {
        CloudflareKvBackend {
            core: Arc::new(CloudflareKvCore {
                api_token: "Bearer test-token".to_string(),
                account_id: "acc123".to_string(),
                namespace_id: "ns456".to_string(),
                expiration_ttl: None,
                info: ServiceInfo::new(CLOUDFLARE_KV_SCHEME, "/", ""),
                capability: Capability::default(),
            }),
        }
    }

    #[test]
    fn backend_debug_redacts_api_token() {
        let sentinel = "cfkv-backend-secret-token";
        let builder = CloudflareKvBuilder::default()
            .api_token(sentinel)
            .account_id("acc123")
            .namespace_id("ns456");
        assert!(!format!("{builder:?}").contains(sentinel));

        let backend = builder.build().unwrap();
        assert!(!format!("{backend:?}").contains(sentinel));
    }

    #[tokio::test]
    async fn create_dir_propagates_http_errors() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let backend = test_backend();

            let ctx_500 = OperationContext::default().with_http_transport(HttpTransporter::new(
                SequenceTransport::new(vec![(StatusCode::INTERNAL_SERVER_ERROR, "server error")]),
            ));
            let err_500 = backend
                .create_dir(&ctx_500, "a/b/", OpCreateDir::default())
                .await
                .unwrap_err();
            assert_eq!(err_500.kind(), ErrorKind::Unexpected);
            assert!(err_500.is_temporary());

            let ctx_403 = OperationContext::default().with_http_transport(HttpTransporter::new(
                SequenceTransport::new(vec![(StatusCode::FORBIDDEN, "forbidden")]),
            ));
            let err_403 = backend
                .create_dir(&ctx_403, "a/", OpCreateDir::default())
                .await
                .unwrap_err();
            assert_eq!(err_403.kind(), ErrorKind::PermissionDenied);
            assert!(err_403.is_temporary());
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn stat_dir_propagates_fallback_list_errors() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let backend = test_backend();

            let ctx_500 = OperationContext::default().with_http_transport(HttpTransporter::new(
                SequenceTransport::new(vec![
                    (StatusCode::NOT_FOUND, "not found"),
                    (StatusCode::INTERNAL_SERVER_ERROR, "list failed"),
                ]),
            ));
            let err_500 = backend
                .stat(&ctx_500, "dir/", OpStat::default())
                .await
                .unwrap_err();
            assert_eq!(err_500.kind(), ErrorKind::Unexpected);
            assert!(err_500.is_temporary());

            let ctx_429 = OperationContext::default().with_http_transport(HttpTransporter::new(
                SequenceTransport::new(vec![
                    (StatusCode::NOT_FOUND, "not found"),
                    (StatusCode::TOO_MANY_REQUESTS, "rate limited"),
                ]),
            ));
            let err_429 = backend
                .stat(&ctx_429, "dir/", OpStat::default())
                .await
                .unwrap_err();
            assert_eq!(err_429.kind(), ErrorKind::Unexpected);
            assert!(!err_429.is_temporary());

            let ctx_false = OperationContext::default().with_http_transport(HttpTransporter::new(
                SequenceTransport::new(vec![
                    (StatusCode::NOT_FOUND, "not found"),
                    (
                        StatusCode::OK,
                        r#"{"success":false,"result":null,"errors":[{"code":10000}]}"#,
                    ),
                ]),
            ));
            let err_false = backend
                .stat(&ctx_false, "dir/", OpStat::default())
                .await
                .unwrap_err();
            assert_eq!(err_false.kind(), ErrorKind::PermissionDenied);
        })
        .await
        .unwrap();
    }
}
