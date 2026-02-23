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
use bytes::Bytes;
use http::Request;
use http::Response;
use http::StatusCode;
use http::header;
use http::header::CONTENT_TYPE;
use mea::mutex::Mutex;
use serde::Deserialize;

use opendal_core::raw::*;
use opendal_core::*;

use std::sync::OnceLock;

/// Authentication mode for Swift.
pub enum SwiftAuth {
    /// Static token provided directly.
    Token,
    /// Keystone v3 credentials for automatic token acquisition and refresh.
    Keystone(KeystoneCredentials),
}

/// Keystone v3 credentials.
pub struct KeystoneCredentials {
    pub auth_url: String,
    pub username: String,
    pub password: String,
    pub user_domain_name: String,
    pub project_name: String,
    pub project_domain_name: String,
}

/// Signer that manages token lifecycle for Swift.
///
/// In static-token mode, the token is used as-is with no refresh.
/// In Keystone mode, the token is acquired on first use and refreshed
/// automatically when it approaches expiry (2-minute grace period).
pub struct SwiftSigner {
    pub info: Arc<AccessorInfo>,

    /// Authentication mode.
    pub auth: SwiftAuth,

    /// Cached token.
    pub token: String,
    /// Token expiry. Set to `Timestamp::MAX` for static tokens (never expire).
    pub expires_in: Timestamp,
    /// Storage URL discovered from Keystone catalog. Empty for static-token mode
    /// (endpoint is on SwiftCore directly).
    pub storage_url: String,
}

impl SwiftSigner {
    /// Create a signer for static-token authentication.
    pub fn new_static(info: Arc<AccessorInfo>, token: String) -> Self {
        SwiftSigner {
            info,
            auth: SwiftAuth::Token,
            token,
            expires_in: Timestamp::MAX,
            storage_url: String::new(),
        }
    }

    /// Create a signer for Keystone v3 authentication.
    pub fn new_keystone(info: Arc<AccessorInfo>, creds: KeystoneCredentials) -> Self {
        SwiftSigner {
            info,
            auth: SwiftAuth::Keystone(creds),
            token: String::new(),
            expires_in: Timestamp::MIN, // forces first-call refresh
            storage_url: String::new(),
        }
    }

    /// Insert the `X-Auth-Token` header, refreshing if needed.
    pub async fn sign<T>(&mut self, req: &mut Request<T>) -> Result<()> {
        if !self.token.is_empty() && self.expires_in > Timestamp::now() {
            let value = self
                .token
                .parse()
                .expect("token must be valid header value");
            req.headers_mut().insert("X-Auth-Token", value);
            return Ok(());
        }

        self.acquire_token().await?;

        let value = self
            .token
            .parse()
            .expect("token must be valid header value");
        req.headers_mut().insert("X-Auth-Token", value);
        Ok(())
    }

    /// Acquire a Keystone v3 token.
    async fn acquire_token(&mut self) -> Result<()> {
        let creds = match &self.auth {
            SwiftAuth::Token => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "static token expired or missing — cannot refresh",
                ));
            }
            SwiftAuth::Keystone(creds) => creds,
        };

        let url = format!("{}/auth/tokens", creds.auth_url.trim_end_matches('/'));

        let body = serde_json::json!({
            "auth": {
                "identity": {
                    "methods": ["password"],
                    "password": {
                        "user": {
                            "name": creds.username,
                            "domain": { "name": creds.user_domain_name },
                            "password": creds.password,
                        }
                    }
                },
                "scope": {
                    "project": {
                        "name": creds.project_name,
                        "domain": { "name": creds.project_domain_name },
                    }
                }
            }
        });

        let body_bytes = Bytes::from(serde_json::to_vec(&body).map_err(new_json_serialize_error)?);

        let request = Request::post(&url)
            .header(CONTENT_TYPE, "application/json")
            .header(header::CONTENT_LENGTH, body_bytes.len())
            .body(Buffer::from(body_bytes))
            .map_err(new_request_build_error)?;

        let resp = self.info.http_client().send(request).await?;

        match resp.status() {
            StatusCode::CREATED => {
                // Token is in the response header.
                let token = resp
                    .headers()
                    .get("X-Subject-Token")
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::Unexpected,
                            "Keystone response missing X-Subject-Token header",
                        )
                    })?
                    .to_str()
                    .map_err(|_| {
                        Error::new(
                            ErrorKind::Unexpected,
                            "X-Subject-Token header contains non-ASCII characters",
                        )
                    })?
                    .to_string();

                let body = resp.into_body();
                let data: KeystoneTokenResponse =
                    serde_json::from_reader(body.reader()).map_err(new_json_deserialize_error)?;

                // Parse expiry with 2-minute grace period.
                let expires_at: Timestamp = data.token.expires_at.parse()?;
                self.expires_in = expires_at - Duration::from_secs(120);

                self.token = token;

                // Extract Swift storage URL from the service catalog.
                if let Some(url) = extract_swift_endpoint(&data.token.catalog) {
                    self.storage_url = url;
                }

                Ok(())
            }
            _ => {
                let status = resp.status();
                let body = resp.into_body();
                let body_str = String::from_utf8_lossy(&body.to_bytes()).to_string();
                Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Keystone authentication failed with status {status}: {body_str}"),
                ))
            }
        }
    }
}

/// Extract the public Swift storage URL from the Keystone service catalog.
fn extract_swift_endpoint(catalog: &[KeystoneCatalogEntry]) -> Option<String> {
    catalog
        .iter()
        .find(|entry| entry.service_type == "object-store")
        .and_then(|entry| {
            entry
                .endpoints
                .iter()
                .find(|ep| ep.interface == "public")
                .map(|ep| ep.url.trim_end_matches('/').to_string())
        })
}

pub struct SwiftCore {
    pub info: Arc<AccessorInfo>,
    pub root: String,
    /// Lazily resolved endpoint. Set from config or discovered via Keystone catalog.
    pub endpoint: OnceLock<String>,
    pub container: String,
    pub signer: Arc<Mutex<SwiftSigner>>,
}

impl Debug for SwiftCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SwiftCore")
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .field("container", &self.container)
            .finish_non_exhaustive()
    }
}

impl SwiftCore {
    /// Sign a request with the current token.
    pub async fn sign<T>(&self, req: &mut Request<T>) -> Result<()> {
        let mut signer = self.signer.lock().await;
        signer.sign(req).await
    }

    /// Get the resolved endpoint, or return an error if not yet available.
    fn get_endpoint(&self) -> Result<&str> {
        self.endpoint.get().map(|s| s.as_str()).ok_or_else(|| {
            Error::new(
                ErrorKind::ConfigInvalid,
                "Swift endpoint not yet resolved — call ensure_endpoint() first",
            )
        })
    }

    /// Ensure the endpoint is resolved.
    ///
    /// For static-token mode, this is a no-op (set at build time).
    /// For Keystone mode without explicit endpoint, this triggers a token
    /// acquisition to discover the storage URL from the service catalog.
    pub async fn ensure_endpoint(&self) -> Result<()> {
        if self.endpoint.get().is_some() {
            return Ok(());
        }

        // Need to discover from Keystone.
        let mut signer = self.signer.lock().await;

        // Double-check after acquiring lock.
        if self.endpoint.get().is_some() {
            return Ok(());
        }

        // Trigger token acquisition if needed.
        if signer.token.is_empty() || signer.expires_in <= Timestamp::now() {
            signer.acquire_token().await?;
        }

        if signer.storage_url.is_empty() {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "Keystone service catalog does not contain an object-store endpoint",
            ));
        }

        // This may race but OnceLock handles that safely.
        let _ = self.endpoint.set(signer.storage_url.clone());
        Ok(())
    }

    pub async fn swift_delete(&self, path: &str) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}",
            self.get_endpoint()?,
            &self.container,
            percent_encode_path(&p)
        );

        let mut req = Request::delete(&url)
            .extension(Operation::Delete)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;

        self.info.http_client().send(req).await
    }

    pub async fn swift_list(
        &self,
        path: &str,
        delimiter: &str,
        limit: Option<usize>,
        marker: &str,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        // The delimiter is used to disable recursive listing.
        // Swift returns a 200 status code when there is no such pseudo directory in prefix.
        let mut url =
            QueryPairsWriter::new(&format!("{}/{}/", self.get_endpoint()?, &self.container))
                .push("prefix", &percent_encode_path(&p))
                .push("delimiter", delimiter)
                .push("format", "json");

        if let Some(limit) = limit {
            url = url.push("limit", &limit.to_string());
        }
        if !marker.is_empty() {
            url = url.push("marker", marker);
        }

        let mut req = Request::get(url.finish())
            .extension(Operation::List)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;

        self.info.http_client().send(req).await
    }

    pub async fn swift_create_object(
        &self,
        path: &str,
        length: u64,
        args: &OpWrite,
        body: Buffer,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);
        let url = format!(
            "{}/{}/{}",
            self.get_endpoint()?,
            &self.container,
            percent_encode_path(&p)
        );

        let mut builder = Request::put(&url);

        // Set user metadata headers.
        if let Some(user_metadata) = args.user_metadata() {
            for (k, v) in user_metadata {
                builder = builder.header(format!("X-Object-Meta-{k}"), v);
            }
        }

        builder = builder.header(header::CONTENT_LENGTH, length);

        let mut req = builder
            .extension(Operation::Write)
            .body(body)
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;

        self.info.http_client().send(req).await
    }

    pub async fn swift_read(
        &self,
        path: &str,
        range: BytesRange,
        _arg: &OpRead,
    ) -> Result<Response<HttpBody>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let url = format!(
            "{}/{}/{}",
            self.get_endpoint()?,
            &self.container,
            percent_encode_path(&p)
        );

        let mut builder = Request::get(&url);

        if !range.is_full() {
            builder = builder.header(header::RANGE, range.to_header());
        }

        let mut req = builder
            .extension(Operation::Read)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;

        self.info.http_client().fetch(req).await
    }

    pub async fn swift_copy(&self, src_p: &str, dst_p: &str) -> Result<Response<Buffer>> {
        // NOTE: current implementation is limited to same container and root

        let src_p = format!(
            "/{}/{}",
            self.container,
            build_abs_path(&self.root, src_p).trim_end_matches('/')
        );

        let dst_p = build_abs_path(&self.root, dst_p)
            .trim_end_matches('/')
            .to_string();

        let url = format!(
            "{}/{}/{}",
            self.get_endpoint()?,
            &self.container,
            percent_encode_path(&dst_p)
        );

        // Request method doesn't support for COPY, we use PUT instead.
        // Reference: https://docs.openstack.org/api-ref/object-store/#copy-object
        let mut req = Request::put(&url)
            .header("X-Copy-From", percent_encode_path(&src_p))
            .header("Content-Length", "0")
            .extension(Operation::Copy)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;

        self.info.http_client().send(req).await
    }

    pub async fn swift_get_metadata(&self, path: &str) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}",
            self.get_endpoint()?,
            &self.container,
            percent_encode_path(&p)
        );

        let mut req = Request::head(&url)
            .extension(Operation::Stat)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;

        self.info.http_client().send(req).await
    }
}

// --- Keystone v3 deserialization types ---

#[derive(Debug, Deserialize)]
struct KeystoneTokenResponse {
    token: KeystoneToken,
}

#[derive(Debug, Deserialize)]
struct KeystoneToken {
    expires_at: String,
    #[serde(default)]
    catalog: Vec<KeystoneCatalogEntry>,
}

#[derive(Debug, Deserialize)]
struct KeystoneCatalogEntry {
    #[serde(rename = "type")]
    service_type: String,
    endpoints: Vec<KeystoneEndpoint>,
}

#[derive(Debug, Deserialize)]
struct KeystoneEndpoint {
    interface: String,
    url: String,
}

// --- List response types ---

#[derive(Debug, Eq, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum ListOpResponse {
    Subdir {
        subdir: String,
    },
    FileInfo {
        bytes: u64,
        hash: String,
        name: String,
        last_modified: String,
        content_type: Option<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_list_response_test() -> Result<()> {
        let resp = bytes::Bytes::from(
            r#"
            [
                {
                    "subdir": "animals/"
                },
                {
                    "subdir": "fruit/"
                },
                {
                    "bytes": 147,
                    "hash": "5e6b5b70b0426b1cc1968003e1afa5ad",
                    "name": "test.txt",
                    "content_type": "text/plain",
                    "last_modified": "2023-11-01T03:00:23.147480"
                }
            ]
            "#,
        );

        let mut out = serde_json::from_slice::<Vec<ListOpResponse>>(&resp)
            .map_err(new_json_deserialize_error)?;

        assert_eq!(out.len(), 3);
        assert_eq!(
            out.pop().unwrap(),
            ListOpResponse::FileInfo {
                bytes: 147,
                hash: "5e6b5b70b0426b1cc1968003e1afa5ad".to_string(),
                name: "test.txt".to_string(),
                last_modified: "2023-11-01T03:00:23.147480".to_string(),
                content_type: Some("text/plain".to_string()),
            }
        );

        assert_eq!(
            out.pop().unwrap(),
            ListOpResponse::Subdir {
                subdir: "fruit/".to_string()
            }
        );

        assert_eq!(
            out.pop().unwrap(),
            ListOpResponse::Subdir {
                subdir: "animals/".to_string()
            }
        );

        Ok(())
    }

    #[test]
    fn extract_swift_endpoint_from_catalog() {
        let catalog = vec![
            KeystoneCatalogEntry {
                service_type: "identity".to_string(),
                endpoints: vec![KeystoneEndpoint {
                    interface: "public".to_string(),
                    url: "https://keystone.example.com/v3".to_string(),
                }],
            },
            KeystoneCatalogEntry {
                service_type: "object-store".to_string(),
                endpoints: vec![
                    KeystoneEndpoint {
                        interface: "admin".to_string(),
                        url: "https://admin.swift.example.com".to_string(),
                    },
                    KeystoneEndpoint {
                        interface: "public".to_string(),
                        url: "https://swift.example.com/v1/AUTH_abc".to_string(),
                    },
                ],
            },
        ];

        assert_eq!(
            extract_swift_endpoint(&catalog),
            Some("https://swift.example.com/v1/AUTH_abc".to_string())
        );
    }

    #[test]
    fn extract_swift_endpoint_missing() {
        let catalog = vec![KeystoneCatalogEntry {
            service_type: "identity".to_string(),
            endpoints: vec![KeystoneEndpoint {
                interface: "public".to_string(),
                url: "https://keystone.example.com/v3".to_string(),
            }],
        }];

        assert_eq!(extract_swift_endpoint(&catalog), None);
    }

    #[test]
    fn extract_swift_endpoint_strips_trailing_slash() {
        let catalog = vec![KeystoneCatalogEntry {
            service_type: "object-store".to_string(),
            endpoints: vec![KeystoneEndpoint {
                interface: "public".to_string(),
                url: "https://swift.example.com/v1/AUTH_abc/".to_string(),
            }],
        }];

        assert_eq!(
            extract_swift_endpoint(&catalog),
            Some("https://swift.example.com/v1/AUTH_abc".to_string())
        );
    }

    #[test]
    fn extract_swift_endpoint_prefers_public() {
        let catalog = vec![KeystoneCatalogEntry {
            service_type: "object-store".to_string(),
            endpoints: vec![
                KeystoneEndpoint {
                    interface: "internal".to_string(),
                    url: "https://internal.swift.example.com/v1/AUTH_abc".to_string(),
                },
                KeystoneEndpoint {
                    interface: "public".to_string(),
                    url: "https://public.swift.example.com/v1/AUTH_abc".to_string(),
                },
                KeystoneEndpoint {
                    interface: "admin".to_string(),
                    url: "https://admin.swift.example.com".to_string(),
                },
            ],
        }];

        assert_eq!(
            extract_swift_endpoint(&catalog),
            Some("https://public.swift.example.com/v1/AUTH_abc".to_string())
        );
    }

    #[test]
    fn parse_keystone_token_response() {
        let json = r#"{
            "token": {
                "methods": ["password"],
                "expires_at": "2026-02-22T17:20:42.000000Z",
                "catalog": [
                    {
                        "type": "object-store",
                        "endpoints": [
                            {
                                "interface": "public",
                                "url": "https://swift.example.com/v1/AUTH_abc",
                                "region_id": "RegionOne",
                                "region": "RegionOne"
                            }
                        ],
                        "id": "abc123",
                        "name": "swift"
                    },
                    {
                        "type": "identity",
                        "endpoints": [
                            {
                                "interface": "public",
                                "url": "https://keystone.example.com/v3",
                                "region_id": "RegionOne",
                                "region": "RegionOne"
                            }
                        ],
                        "id": "def456",
                        "name": "keystone"
                    }
                ],
                "user": {"name": "testuser", "id": "uid123"},
                "project": {"name": "testproject", "id": "pid456"}
            }
        }"#;

        let resp: KeystoneTokenResponse =
            serde_json::from_str(json).expect("should parse Keystone response");
        assert_eq!(resp.token.expires_at, "2026-02-22T17:20:42.000000Z");
        assert_eq!(resp.token.catalog.len(), 2);

        let swift_url = extract_swift_endpoint(&resp.token.catalog);
        assert_eq!(
            swift_url,
            Some("https://swift.example.com/v1/AUTH_abc".to_string())
        );
    }

    #[test]
    fn parse_keystone_token_response_minimal() {
        // Some Keystone responses may have extra fields we don't use.
        // Verify we deserialize correctly with only the fields we need.
        let json = r#"{
            "token": {
                "expires_at": "2025-01-01T00:00:00Z"
            }
        }"#;

        let resp: KeystoneTokenResponse =
            serde_json::from_str(json).expect("should parse minimal response");
        assert_eq!(resp.token.expires_at, "2025-01-01T00:00:00Z");
        assert!(resp.token.catalog.is_empty());
    }

    #[test]
    fn parse_keystone_expiry_as_timestamp() {
        // Verify the expires_at string can be parsed as our Timestamp type
        let expires_at = "2026-02-22T17:20:42.000000Z";
        let ts: Timestamp = expires_at.parse().expect("should parse as Timestamp");
        // Verify it's in the future (or at least parseable and comparable)
        assert!(ts > Timestamp::MIN);
    }

    #[test]
    fn static_signer_never_expires() {
        let info: Arc<AccessorInfo> = AccessorInfo::default().into();
        let signer = SwiftSigner::new_static(info, "test-token".to_string());
        assert_eq!(signer.token, "test-token");
        assert_eq!(signer.expires_in, Timestamp::MAX);
        // Static signer's token should always be considered valid
        assert!(signer.expires_in > Timestamp::now());
    }

    #[test]
    fn keystone_signer_starts_expired() {
        let info: Arc<AccessorInfo> = AccessorInfo::default().into();
        let creds = KeystoneCredentials {
            auth_url: "https://keystone.example.com/v3".to_string(),
            username: "user".to_string(),
            password: "pass".to_string(),
            user_domain_name: "Default".to_string(),
            project_name: "project".to_string(),
            project_domain_name: "Default".to_string(),
        };
        let signer = SwiftSigner::new_keystone(info, creds);
        assert!(signer.token.is_empty());
        assert_eq!(signer.expires_in, Timestamp::MIN);
        // Should be considered expired, forcing token acquisition on first use
        assert!(signer.expires_in <= Timestamp::now());
    }
}
