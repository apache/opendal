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

use asyncband::once::OnceCell;
use tikv_client::Config;
use tikv_client::RawClient;

use super::TIKV_SCHEME;
use opendal_core::raw::ServiceOperation;
use opendal_core::*;

#[derive(Clone, Copy, Debug)]
struct ErrorContext {
    service_operation: ServiceOperation,
}

impl ErrorContext {
    const fn new(service_operation: ServiceOperation) -> Self {
        Self { service_operation }
    }
}

/// TikvCore holds the configuration and client for interacting with TiKV.
#[derive(Clone)]
pub struct TikvCore {
    pub client: OnceCell<RawClient>,
    pub endpoints: Vec<String>,
    pub insecure: bool,
    pub ca_path: Option<String>,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
}

impl Debug for TikvCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TikvCore")
            .field("endpoints", &self.endpoints)
            .field("insecure", &self.insecure)
            .finish()
    }
}

impl TikvCore {
    async fn get_connection(&self) -> Result<&RawClient> {
        self.client
            .get_or_try_init(|| async {
                if self.insecure {
                    return RawClient::new(self.endpoints.clone()).await.map_err(|err| {
                        parse_error(ErrorContext::new(ServiceOperation("Connect")), err)
                    });
                }

                if let Some(ca_path) = self.ca_path.as_ref()
                    && let Some(key_path) = self.key_path.as_ref()
                    && let Some(cert_path) = self.cert_path.as_ref()
                {
                    let config = Config::default().with_security(ca_path, cert_path, key_path);
                    return RawClient::new_with_config(self.endpoints.clone(), config)
                        .await
                        .map_err(|err| {
                            parse_error(ErrorContext::new(ServiceOperation("Connect")), err)
                        });
                }

                Err(
                    Error::new(ErrorKind::ConfigInvalid, "invalid configuration")
                        .with_context("service", TIKV_SCHEME)
                        .with_context("endpoints", format!("{:?}", self.endpoints)),
                )
            })
            .await
    }

    pub async fn get(&self, path: &str) -> Result<Option<Buffer>> {
        let result = self
            .get_connection()
            .await?
            .get(path.to_owned())
            .await
            .map_err(|err| parse_error(ErrorContext::new(ServiceOperation("Get")), err))?;
        Ok(result.map(Buffer::from))
    }

    pub async fn set(&self, path: &str, value: Buffer) -> Result<()> {
        self.get_connection()
            .await?
            .put(path.to_owned(), value.to_vec())
            .await
            .map_err(|err| parse_error(ErrorContext::new(ServiceOperation("Put")), err))
    }

    pub async fn delete(&self, path: &str) -> Result<()> {
        self.get_connection()
            .await?
            .delete(path.to_owned())
            .await
            .map_err(|err| parse_error(ErrorContext::new(ServiceOperation("Delete")), err))
    }
}

// Standard gRPC status codes (https://grpc.io/docs/guides/status-codes/).
const GRPC_CANCELLED: i32 = 1;
const GRPC_UNKNOWN: i32 = 2;
const GRPC_DEADLINE_EXCEEDED: i32 = 4;
const GRPC_NOT_FOUND: i32 = 5;
const GRPC_ALREADY_EXISTS: i32 = 6;
const GRPC_PERMISSION_DENIED: i32 = 7;
const GRPC_RESOURCE_EXHAUSTED: i32 = 8;
const GRPC_FAILED_PRECONDITION: i32 = 9;
const GRPC_ABORTED: i32 = 10;
const GRPC_OUT_OF_RANGE: i32 = 11;
const GRPC_UNIMPLEMENTED: i32 = 12;
const GRPC_INTERNAL: i32 = 13;
const GRPC_UNAVAILABLE: i32 = 14;
const GRPC_UNAUTHENTICATED: i32 = 16;

fn classify_grpc_status_code(code: i32) -> (ErrorKind, &'static str, bool) {
    match code {
        GRPC_NOT_FOUND => (ErrorKind::NotFound, "not found in tikv", false),
        GRPC_ALREADY_EXISTS => (ErrorKind::AlreadyExists, "already exists in tikv", false),
        GRPC_PERMISSION_DENIED | GRPC_UNAUTHENTICATED => (
            ErrorKind::PermissionDenied,
            "permission denied by tikv",
            false,
        ),
        GRPC_FAILED_PRECONDITION => (
            ErrorKind::ConditionNotMatch,
            "precondition failed in tikv",
            false,
        ),
        GRPC_OUT_OF_RANGE => (
            ErrorKind::RangeNotSatisfied,
            "range out of bounds in tikv",
            false,
        ),
        GRPC_UNIMPLEMENTED => (
            ErrorKind::Unsupported,
            "operation is not supported by tikv",
            false,
        ),
        GRPC_RESOURCE_EXHAUSTED => (ErrorKind::RateLimited, "tikv resource exhausted", true),
        GRPC_CANCELLED
        | GRPC_UNKNOWN
        | GRPC_DEADLINE_EXCEEDED
        | GRPC_ABORTED
        | GRPC_INTERNAL
        | GRPC_UNAVAILABLE => (
            ErrorKind::Unexpected,
            "transient grpc error from tikv",
            true,
        ),
        _ => (ErrorKind::Unexpected, "grpc api error from tikv", false),
    }
}

fn classify_internal_error(message: &str) -> (ErrorKind, &'static str, bool) {
    // tikv-client wraps TLS PEM open/read failures and PD endpoint/cluster
    // configuration mismatches in `Error::InternalError`.
    if message.contains("failed to open ")
        || message.contains("failed to load ")
        || message.contains("duplicated PD endpoint")
        || message.contains("PD response cluster_id mismatch")
    {
        return (
            ErrorKind::ConfigInvalid,
            "invalid tikv configuration",
            false,
        );
    }

    // tikv-client aggregates unreachable or leaderless PD connection failures
    // in `Error::InternalError` during cluster discovery and connection.
    if message.contains("PD cluster failed to respond")
        || message.contains("failed to connect to ")
        || message.contains("no leader found in GetMembersResponse")
        || message.contains("unexpected no PD leader")
        || message.contains("failed to get PD members")
        || message.contains("TimestampRequest channel is closed")
    {
        return (
            ErrorKind::Unexpected,
            "transient connection error from tikv",
            true,
        );
    }

    (ErrorKind::Unexpected, "internal error from tikv", false)
}

fn parse_error(ctx: ErrorContext, err: tikv_client::Error) -> Error {
    let mut grpc_code = None;
    let (kind, message, temporary) = match &err {
        tikv_client::Error::Url(_)
        | tikv_client::Error::ColumnFamilyError(_)
        | tikv_client::Error::KeyspaceNotFound(_) => (
            ErrorKind::ConfigInvalid,
            "invalid tikv configuration",
            false,
        ),
        tikv_client::Error::Unimplemented | tikv_client::Error::UnsupportedMode => (
            ErrorKind::Unsupported,
            "operation is not supported by tikv",
            false,
        ),
        tikv_client::Error::Grpc(_) => (
            ErrorKind::Unexpected,
            "grpc transport error from tikv",
            true,
        ),
        tikv_client::Error::Io(_) => (ErrorKind::Unexpected, "io error from tikv", true),
        tikv_client::Error::RegionError(_)
        | tikv_client::Error::LeaderNotFound { .. }
        | tikv_client::Error::RegionForKeyNotFound { .. }
        | tikv_client::Error::RegionForRangeNotFound { .. }
        | tikv_client::Error::RegionNotFoundInResponse { .. }
        | tikv_client::Error::EntryNotFoundInRegionCache
        | tikv_client::Error::NoCurrentRegions
        | tikv_client::Error::Channel(_)
        | tikv_client::Error::Canceled(_)
        | tikv_client::Error::JoinError(_) => {
            (ErrorKind::Unexpected, "transient error from tikv", true)
        }
        tikv_client::Error::GrpcAPI(status) => {
            grpc_code = Some(status.code().description());
            classify_grpc_status_code(i32::from(status.code()))
        }
        tikv_client::Error::InternalError { message } => classify_internal_error(message),
        _ => (ErrorKind::Unexpected, "error from tikv", false),
    };

    let mut opendal_err = Error::new(kind, message)
        .with_context("service", TIKV_SCHEME)
        .with_context("service_operation", ctx.service_operation.0)
        .set_source(err);

    if let Some(code) = grpc_code {
        opendal_err = opendal_err.with_context("grpc_code", code);
    }
    if temporary {
        opendal_err = opendal_err.set_temporary();
    }

    opendal_err
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn parse_error_classifies_connect_and_data_errors() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let unused_addr = listener.local_addr().unwrap();
        drop(listener);

        let security_mgr = tikv_client::SecurityManager::default();
        let grpc_err = tokio::time::timeout(
            Duration::from_secs(5),
            security_mgr.connect(&format!("http://{unused_addr}"), |c| c),
        )
        .await
        .expect("connect to closed port should finish quickly")
        .expect_err("connect to closed port should fail");
        assert!(matches!(grpc_err, tikv_client::Error::Grpc(_)));

        let url_err = tokio::time::timeout(
            Duration::from_secs(5),
            security_mgr.connect("http://[invalid", |c| c),
        )
        .await
        .expect("invalid url should fail immediately")
        .expect_err("invalid url should fail");
        assert!(matches!(url_err, tikv_client::Error::Url(_)));

        let tls_load_err = tikv_client::SecurityManager::load(
            "/nonexistent/ca.pem",
            "/nonexistent/cert.pem",
            "/nonexistent/key.pem",
        )
        .err()
        .expect("missing TLS files should fail");

        // Transient gRPC transport error on Connect must be temporary Unexpected, not ConfigInvalid.
        let parsed = parse_error(ErrorContext::new(ServiceOperation("Connect")), grpc_err);
        assert_eq!(parsed.kind(), ErrorKind::Unexpected);
        assert!(parsed.is_temporary());

        // Invalid URI must remain permanent ConfigInvalid.
        let parsed = parse_error(ErrorContext::new(ServiceOperation("Connect")), url_err);
        assert_eq!(parsed.kind(), ErrorKind::ConfigInvalid);
        assert!(!parsed.is_temporary());

        // TLS PEM load failure must remain permanent ConfigInvalid.
        let parsed = parse_error(ErrorContext::new(ServiceOperation("Connect")), tls_load_err);
        assert_eq!(parsed.kind(), ErrorKind::ConfigInvalid);
        assert!(!parsed.is_temporary());

        for op in ["Connect", "Get"] {
            let io_err = parse_error(
                ErrorContext::new(ServiceOperation(op)),
                tikv_client::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "reset",
                )),
            );
            assert_eq!(io_err.kind(), ErrorKind::Unexpected);
            assert!(io_err.is_temporary());

            let region_err = parse_error(
                ErrorContext::new(ServiceOperation(op)),
                tikv_client::Error::RegionError(Box::default()),
            );
            assert_eq!(region_err.kind(), ErrorKind::Unexpected);
            assert!(region_err.is_temporary());

            let pd_unreachable = parse_error(
                ErrorContext::new(ServiceOperation(op)),
                tikv_client::Error::InternalError {
                    message: "[pd/cluster.rs:209]: PD cluster failed to respond".to_string(),
                },
            );
            assert_eq!(pd_unreachable.kind(), ErrorKind::Unexpected);
            assert!(pd_unreachable.is_temporary());

            let dup_endpoint = parse_error(
                ErrorContext::new(ServiceOperation(op)),
                tikv_client::Error::InternalError {
                    message: "[pd/cluster.rs:172]: duplicated PD endpoint 127.0.0.1:2379"
                        .to_string(),
                },
            );
            assert_eq!(dup_endpoint.kind(), ErrorKind::ConfigInvalid);
            assert!(!dup_endpoint.is_temporary());

            let cluster_mismatch = parse_error(
                ErrorContext::new(ServiceOperation(op)),
                tikv_client::Error::InternalError {
                    message: "[pd/cluster.rs:188]: PD response cluster_id mismatch, want 1, got 2"
                        .to_string(),
                },
            );
            assert_eq!(cluster_mismatch.kind(), ErrorKind::ConfigInvalid);
            assert!(!cluster_mismatch.is_temporary());

            let unsupported = parse_error(
                ErrorContext::new(ServiceOperation(op)),
                tikv_client::Error::UnsupportedMode,
            );
            assert_eq!(unsupported.kind(), ErrorKind::Unsupported);
            assert!(!unsupported.is_temporary());

            let kv_err = parse_error(
                ErrorContext::new(ServiceOperation(op)),
                tikv_client::Error::KvError {
                    message: "key too large".to_string(),
                },
            );
            assert_eq!(kv_err.kind(), ErrorKind::Unexpected);
            assert!(!kv_err.is_temporary());
        }
    }

    #[test]
    fn classify_grpc_status_code_maps_transient_and_permanent_codes() {
        for code in [
            GRPC_CANCELLED,
            GRPC_UNKNOWN,
            GRPC_DEADLINE_EXCEEDED,
            GRPC_ABORTED,
            GRPC_INTERNAL,
            GRPC_UNAVAILABLE,
        ] {
            let (kind, _, temporary) = classify_grpc_status_code(code);
            assert_eq!(kind, ErrorKind::Unexpected, "code {code}");
            assert!(temporary, "code {code} should be temporary");
        }

        let (kind, _, temporary) = classify_grpc_status_code(GRPC_RESOURCE_EXHAUSTED);
        assert_eq!(kind, ErrorKind::RateLimited);
        assert!(temporary);

        for code in [GRPC_PERMISSION_DENIED, GRPC_UNAUTHENTICATED] {
            let (kind, _, temporary) = classify_grpc_status_code(code);
            assert_eq!(kind, ErrorKind::PermissionDenied, "code {code}");
            assert!(!temporary, "code {code} should not be temporary");
        }

        assert_eq!(
            classify_grpc_status_code(GRPC_NOT_FOUND).0,
            ErrorKind::NotFound
        );
        assert!(!classify_grpc_status_code(GRPC_NOT_FOUND).2);
        assert_eq!(
            classify_grpc_status_code(GRPC_ALREADY_EXISTS).0,
            ErrorKind::AlreadyExists
        );
        assert_eq!(
            classify_grpc_status_code(GRPC_FAILED_PRECONDITION).0,
            ErrorKind::ConditionNotMatch
        );
        assert_eq!(
            classify_grpc_status_code(GRPC_OUT_OF_RANGE).0,
            ErrorKind::RangeNotSatisfied
        );
        assert_eq!(
            classify_grpc_status_code(GRPC_UNIMPLEMENTED).0,
            ErrorKind::Unsupported
        );
        // InvalidArgument (3) and DataLoss (15) are permanent Unexpected.
        for code in [3, 15] {
            let (kind, _, temporary) = classify_grpc_status_code(code);
            assert_eq!(kind, ErrorKind::Unexpected, "code {code}");
            assert!(!temporary, "code {code} should not be temporary");
        }
    }
}
