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

use bytes::Buf;
use http::StatusCode;
use log::debug;
use serde::Deserialize;

use super::DBFS_SCHEME;
use super::config::DbfsConfig;
use super::core::parse_error;
use super::core::{DbfsCore, ErrorContext};
use super::deleter::DbfsDeleter;
use super::lister::DbfsLister;
use super::writer::DbfsWriter;
use opendal_core::raw::*;
use opendal_core::*;

/// [Dbfs](https://docs.databricks.com/api/azure/workspace/dbfs)'s REST API support.
#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct DbfsBuilder {
    pub(super) config: DbfsConfig,
}

impl DbfsBuilder {
    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// Set endpoint of this backend.
    ///
    /// Endpoint must be full uri, e.g.
    ///
    /// - Azure: `https://adb-1234567890123456.78.azuredatabricks.net`
    /// - Aws: `https://dbc-123a5678-90bc.cloud.databricks.com`
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        let endpoint = endpoint.trim_end_matches('/');
        self.config.endpoint = if endpoint.is_empty() {
            None
        } else {
            Some(endpoint.to_string())
        };
        self
    }

    /// Set the token of this backend.
    pub fn token(mut self, token: &str) -> Self {
        if !token.is_empty() {
            self.config.token = Some(token.to_string());
        }
        self
    }
}

impl Builder for DbfsBuilder {
    type Config = DbfsConfig;

    /// Build a DbfsBackend.
    fn build(self) -> Result<impl Service> {
        debug!("backend build started: {:?}", self);

        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root {root}");

        let endpoint = match &self.config.endpoint {
            Some(endpoint) => {
                if endpoint.trim_end_matches('/').is_empty() {
                    return Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                        .with_operation("Builder::build")
                        .with_context("service", DBFS_SCHEME));
                }
                endpoint.to_string()
            }
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                    .with_operation("Builder::build")
                    .with_context("service", DBFS_SCHEME));
            }
        };
        debug!("backend use endpoint: {}", endpoint);

        let token = match self.config.token {
            Some(token) if !token.is_empty() => token,
            _ => {
                return Err(
                    Error::new(ErrorKind::ConfigInvalid, "missing token for Dbfs")
                        .with_operation("Builder::build")
                        .with_context("service", DBFS_SCHEME),
                );
            }
        };

        let capability = Capability {
            stat: true,

            write: true,
            create_dir: true,
            delete: true,
            rename: true,

            list: true,

            shared: true,

            ..Default::default()
        };

        Ok(DbfsBackend {
            core: Arc::new(DbfsCore {
                root,
                endpoint,
                token,
            }),
            capability,
        })
    }
}

/// Backend for DBFS service
#[derive(Debug, Clone)]
pub struct DbfsBackend {
    core: Arc<DbfsCore>,
    capability: Capability,
}

impl Service for DbfsBackend {
    type Reader = ();
    type Writer = oio::OneShotWriter<DbfsWriter>;
    type Lister = oio::PageLister<DbfsLister>;
    type Deleter = oio::OneShotDeleter<DbfsDeleter>;
    type Copier = ();
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        ServiceInfo::new(DBFS_SCHEME, &self.core.root, "")
    }

    fn capability(&self) -> Capability {
        self.capability
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        _: OpCreateDir,
    ) -> Result<RpCreateDir> {
        let resp = self.core.dbfs_create_dir(ctx, path).await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => Ok(RpCreateDir::default()),
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("Mkdirs")),
                resp,
            )),
        }
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, _: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(MetadataBuilder::dir().build()));
        }

        let resp = self.core.dbfs_get_status(ctx, path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let mut meta = MetadataBuilder::unknown();
                let bs = resp.into_body();
                let decoded_response: DbfsStatus =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;
                meta.last_modified(Timestamp::from_millisecond(
                    decoded_response.modification_time,
                )?);
                match decoded_response.is_dir {
                    true => meta.set_dir(),
                    false => meta.set_file(decoded_response.file_size as u64),
                };
                Ok(RpStat::new(meta.build()))
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("GetStatus")),
                resp,
            )),
        }
    }

    fn read(&self, _ctx: &OperationContext, _path: &str, _args: OpRead) -> Result<Self::Reader> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let output: oio::OneShotWriter<DbfsWriter> = {
            Ok(oio::OneShotWriter::new(DbfsWriter::new(
                self.core.clone(),
                ctx.clone(),
                args,
                path.to_string(),
            )))
        }?;

        Ok(output)
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        let output: oio::OneShotDeleter<DbfsDeleter> = {
            Ok(oio::OneShotDeleter::new(DbfsDeleter::new(
                self.core.clone(),
                ctx.clone(),
            )))
        }?;

        Ok(output)
    }

    fn list(&self, ctx: &OperationContext, path: &str, _args: OpList) -> Result<Self::Lister> {
        let output: oio::PageLister<DbfsLister> = {
            let l = DbfsLister::new(self.core.clone(), ctx.clone(), path.to_string());

            Ok(oio::PageLister::new(l))
        }?;

        Ok(output)
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        _args: OpRename,
    ) -> Result<RpRename> {
        self.core.dbfs_ensure_parent_path(ctx, to).await?;

        let resp = self.core.dbfs_rename(ctx, from, to).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(RpRename::default()),
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("Move")),
                resp,
            )),
        }
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

#[derive(Deserialize)]
struct DbfsStatus {
    // Not used fields.
    // path: String,
    is_dir: bool,
    file_size: i64,
    modification_time: i64,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use http::Method;
    use serde_json::Value;

    use super::*;
    use crate::core::test_support::RecordingTransport;

    const TEST_TIMEOUT: Duration = Duration::from_secs(10);

    #[test]
    fn build_rejects_empty_endpoint_and_token() {
        let invalid_configs = [
            DbfsConfig {
                endpoint: None,
                token: Some("tok".to_string()),
                root: None,
            },
            DbfsConfig {
                endpoint: Some(String::new()),
                token: Some("tok".to_string()),
                root: None,
            },
            DbfsConfig {
                endpoint: Some("///".to_string()),
                token: Some("tok".to_string()),
                root: None,
            },
            DbfsConfig {
                endpoint: Some("https://example.databricks.com".to_string()),
                token: None,
                root: None,
            },
            DbfsConfig {
                endpoint: Some("https://example.databricks.com".to_string()),
                token: Some(String::new()),
                root: None,
            },
        ];

        for cfg in invalid_configs {
            let err = cfg.into_builder().build().expect_err("config must fail");
            assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
        }

        let err = DbfsBuilder::default()
            .endpoint("///")
            .token("tok")
            .build()
            .expect_err("slash-only endpoint must fail");
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    }

    #[tokio::test]
    async fn rename_ensures_parent_dir_not_destination_file() {
        tokio::time::timeout(TEST_TIMEOUT, async {
            let transport = RecordingTransport::new([
                (
                    StatusCode::NOT_FOUND,
                    r#"{"error_code":"RESOURCE_DOES_NOT_EXIST","message":"not found"}"#,
                ),
                (StatusCode::OK, "{}"),
                (StatusCode::OK, "{}"),
            ]);
            let ctx = transport.context();

            let backend = DbfsBuilder::default()
                .endpoint("https://example.databricks.com")
                .token("tok")
                .root("/root")
                .build()
                .unwrap();

            backend
                .rename(&ctx, "a.txt", "dir/b.txt", OpRename::default())
                .await
                .unwrap();

            let reqs = transport.requests();
            assert_eq!(reqs.len(), 3);

            assert_eq!(reqs[0].method, Method::GET);
            assert_eq!(
                reqs[0].uri,
                "https://example.databricks.com/api/2.0/dbfs/get-status?path=/root/dir"
            );

            assert_eq!(reqs[1].method, Method::POST);
            assert_eq!(
                reqs[1].uri,
                "https://example.databricks.com/api/2.0/dbfs/mkdirs"
            );
            let mkdir_body: Value = serde_json::from_slice(&reqs[1].body).unwrap();
            assert_eq!(mkdir_body["path"], "/root/dir");

            assert_eq!(reqs[2].method, Method::POST);
            assert_eq!(
                reqs[2].uri,
                "https://example.databricks.com/api/2.0/dbfs/move"
            );
            let move_body: Value = serde_json::from_slice(&reqs[2].body).unwrap();
            assert_eq!(move_body["source_path"], "/root/a.txt");
            assert_eq!(move_body["destination_path"], "/root/dir/b.txt");
        })
        .await
        .expect("test timed out");
    }

    #[tokio::test]
    async fn rename_skips_mkdirs_when_parent_already_exists() {
        tokio::time::timeout(TEST_TIMEOUT, async {
            let transport = RecordingTransport::new([
                (
                    StatusCode::OK,
                    r#"{"is_dir":true,"file_size":0,"modification_time":1700000000000}"#,
                ),
                (StatusCode::OK, "{}"),
            ]);
            let ctx = transport.context();

            let backend = DbfsBuilder::default()
                .endpoint("https://example.databricks.com")
                .token("tok")
                .root("/root")
                .build()
                .unwrap();

            backend
                .rename(&ctx, "a.txt", "dir/b.txt", OpRename::default())
                .await
                .unwrap();

            let reqs = transport.requests();
            assert_eq!(reqs.len(), 2);
            assert_eq!(reqs[0].method, Method::GET);
            assert_eq!(
                reqs[0].uri,
                "https://example.databricks.com/api/2.0/dbfs/get-status?path=/root/dir"
            );
            assert_eq!(reqs[1].method, Method::POST);
            assert_eq!(
                reqs[1].uri,
                "https://example.databricks.com/api/2.0/dbfs/move"
            );
        })
        .await
        .expect("test timed out");
    }

    #[tokio::test]
    async fn rename_skips_parent_check_when_root_and_parent_are_slash() {
        tokio::time::timeout(TEST_TIMEOUT, async {
            let transport = RecordingTransport::new([(StatusCode::OK, "{}")]);
            let ctx = transport.context();

            let backend = DbfsBuilder::default()
                .endpoint("https://example.databricks.com")
                .token("tok")
                .root("/")
                .build()
                .unwrap();

            backend
                .rename(&ctx, "a.txt", "b.txt", OpRename::default())
                .await
                .unwrap();

            let reqs = transport.requests();
            assert_eq!(reqs.len(), 1);
            assert_eq!(reqs[0].method, Method::POST);
            assert_eq!(
                reqs[0].uri,
                "https://example.databricks.com/api/2.0/dbfs/move"
            );
            let move_body: Value = serde_json::from_slice(&reqs[0].body).unwrap();
            assert_eq!(move_body["source_path"], "/a.txt");
            assert_eq!(move_body["destination_path"], "/b.txt");
        })
        .await
        .expect("test timed out");
    }

    #[tokio::test]
    async fn rename_ensures_configured_root_when_destination_is_at_root() {
        tokio::time::timeout(TEST_TIMEOUT, async {
            let transport = RecordingTransport::new([
                (
                    StatusCode::NOT_FOUND,
                    r#"{"error_code":"RESOURCE_DOES_NOT_EXIST","message":"not found"}"#,
                ),
                (StatusCode::CREATED, "{}"),
                (StatusCode::OK, "{}"),
            ]);
            let ctx = transport.context();

            let backend = DbfsBuilder::default()
                .endpoint("https://example.databricks.com")
                .token("tok")
                .root("/root")
                .build()
                .unwrap();

            backend
                .rename(&ctx, "a.txt", "b.txt", OpRename::default())
                .await
                .unwrap();

            let reqs = transport.requests();
            assert_eq!(reqs.len(), 3);
            assert_eq!(
                reqs[0].uri,
                "https://example.databricks.com/api/2.0/dbfs/get-status?path=/root"
            );
            let mkdir_body: Value = serde_json::from_slice(&reqs[1].body).unwrap();
            assert_eq!(mkdir_body["path"], "/root");
            let move_body: Value = serde_json::from_slice(&reqs[2].body).unwrap();
            assert_eq!(move_body["source_path"], "/root/a.txt");
            assert_eq!(move_body["destination_path"], "/root/b.txt");
        })
        .await
        .expect("test timed out");
    }

    #[tokio::test]
    async fn rename_propagates_mkdirs_failure_without_calling_move() {
        tokio::time::timeout(TEST_TIMEOUT, async {
            let transport = RecordingTransport::new([
                (
                    StatusCode::NOT_FOUND,
                    r#"{"error_code":"RESOURCE_DOES_NOT_EXIST","message":"not found"}"#,
                ),
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    r#"{"error_code":"INTERNAL_ERROR","message":"mkdirs failed"}"#,
                ),
            ]);
            let ctx = transport.context();

            let backend = DbfsBuilder::default()
                .endpoint("https://example.databricks.com")
                .token("tok")
                .root("/root")
                .build()
                .unwrap();

            let err = backend
                .rename(&ctx, "a.txt", "dir/b.txt", OpRename::default())
                .await
                .unwrap_err();

            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(err.is_temporary());

            let reqs = transport.requests();
            assert_eq!(reqs.len(), 2);
            assert_eq!(
                reqs[1].uri,
                "https://example.databricks.com/api/2.0/dbfs/mkdirs"
            );
        })
        .await
        .expect("test timed out");
    }

    #[tokio::test]
    async fn stat_missing_trailing_slash_returns_not_found() {
        tokio::time::timeout(TEST_TIMEOUT, async {
            let transport = RecordingTransport::new([(
                StatusCode::NOT_FOUND,
                r#"{"error_code":"RESOURCE_DOES_NOT_EXIST","message":"missing"}"#,
            )]);
            let ctx = transport.context();

            let backend = DbfsBuilder::default()
                .endpoint("https://example.databricks.com")
                .token("tok")
                .build()
                .unwrap();

            // Root stat still succeeds without network calls.
            let root_rp = backend.stat(&ctx, "/", OpStat::default()).await.unwrap();
            assert_eq!(root_rp.into_metadata().mode(), EntryMode::DIR);
            assert!(transport.requests().is_empty());

            // Non-existent directory must return NotFound rather than fabricating a DIR.
            let err = backend
                .stat(&ctx, "missing/", OpStat::default())
                .await
                .unwrap_err();
            assert_eq!(err.kind(), ErrorKind::NotFound);
            assert_eq!(transport.requests().len(), 1);
        })
        .await
        .expect("test timed out");
    }
}
