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

use http::Response;
use http::StatusCode;
use log::debug;
use mea::mutex::Mutex;

use super::SWIFT_SCHEME;
use super::SwiftConfig;
use super::core::*;
use super::deleter::SwiftDeleter;
use super::error::parse_error;
use super::lister::SwiftLister;
use super::writer::SwiftWriter;
use opendal_core::raw::*;
use opendal_core::*;

/// [OpenStack Swift](https://docs.openstack.org/api-ref/object-store/#)'s REST API support.
/// For more information about swift-compatible services, refer to [Compatible Services](#compatible-services).
#[doc = include_str!("docs.md")]
#[doc = include_str!("compatible_services.md")]
#[derive(Debug, Default)]
pub struct SwiftBuilder {
    pub(super) config: SwiftConfig,
}

impl SwiftBuilder {
    /// Set the remote address of this backend
    ///
    /// Endpoints should be full uri, e.g.
    ///
    /// - `http://127.0.0.1:8080/v1/AUTH_test`
    /// - `http://192.168.66.88:8080/swift/v1`
    /// - `https://openstack-controller.example.com:8080/v1/ccount`
    ///
    /// If user inputs endpoint without scheme, we will
    /// prepend `https://` to it.
    ///
    /// When using Keystone v3 authentication, the endpoint can be omitted
    /// and will be discovered from the service catalog.
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        self.config.endpoint = if endpoint.is_empty() {
            None
        } else {
            Some(endpoint.trim_end_matches('/').to_string())
        };
        self
    }

    /// Set container of this backend.
    ///
    /// All operations will happen under this container. It is required. e.g. `snapshots`
    pub fn container(mut self, container: &str) -> Self {
        self.config.container = if container.is_empty() {
            None
        } else {
            Some(container.trim_end_matches('/').to_string())
        };
        self
    }

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

    /// Set the token of this backend.
    ///
    /// Default to empty string.
    pub fn token(mut self, token: &str) -> Self {
        if !token.is_empty() {
            self.config.token = Some(token.to_string());
        }
        self
    }

    /// Set the Keystone v3 authentication URL.
    ///
    /// e.g. `https://keystone.example.com/v3`
    pub fn auth_url(mut self, auth_url: &str) -> Self {
        if !auth_url.is_empty() {
            self.config.auth_url = Some(auth_url.to_string());
        }
        self
    }

    /// Set the username for Keystone v3 authentication.
    pub fn username(mut self, username: &str) -> Self {
        if !username.is_empty() {
            self.config.username = Some(username.to_string());
        }
        self
    }

    /// Set the password for Keystone v3 authentication.
    pub fn password(mut self, password: &str) -> Self {
        if !password.is_empty() {
            self.config.password = Some(password.to_string());
        }
        self
    }

    /// Set the user domain name for Keystone v3 authentication.
    ///
    /// Defaults to "Default" if not specified.
    pub fn user_domain_name(mut self, name: &str) -> Self {
        if !name.is_empty() {
            self.config.user_domain_name = Some(name.to_string());
        }
        self
    }

    /// Set the project (tenant) name for Keystone v3 authentication.
    pub fn project_name(mut self, name: &str) -> Self {
        if !name.is_empty() {
            self.config.project_name = Some(name.to_string());
        }
        self
    }

    /// Set the project domain name for Keystone v3 authentication.
    ///
    /// Defaults to "Default" if not specified.
    pub fn project_domain_name(mut self, name: &str) -> Self {
        if !name.is_empty() {
            self.config.project_domain_name = Some(name.to_string());
        }
        self
    }
}

impl Builder for SwiftBuilder {
    type Config = SwiftConfig;

    /// Build a SwiftBackend.
    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root {root}");

        let container = match self.config.container {
            Some(container) => container,
            None => {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "missing container for Swift",
                ));
            }
        };

        // Determine authentication mode and endpoint.
        let has_keystone = self.config.auth_url.is_some();
        let has_token = self.config.token.is_some();

        let info: Arc<AccessorInfo> = {
            let am = AccessorInfo::default();
            am.set_scheme(SWIFT_SCHEME)
                .set_root(&root)
                .set_native_capability(Capability {
                    stat: true,
                    read: true,

                    write: true,
                    write_can_empty: true,
                    write_with_user_metadata: true,

                    delete: true,

                    list: true,
                    list_with_recursive: true,

                    shared: true,

                    ..Default::default()
                });
            am.into()
        };

        if has_keystone {
            // Keystone v3 authentication mode.
            let auth_url = self.config.auth_url.unwrap();
            let username = self.config.username.ok_or_else(|| {
                Error::new(
                    ErrorKind::ConfigInvalid,
                    "username is required for Keystone v3 authentication",
                )
            })?;
            let password = self.config.password.ok_or_else(|| {
                Error::new(
                    ErrorKind::ConfigInvalid,
                    "password is required for Keystone v3 authentication",
                )
            })?;
            let project_name = self.config.project_name.ok_or_else(|| {
                Error::new(
                    ErrorKind::ConfigInvalid,
                    "project_name is required for Keystone v3 authentication",
                )
            })?;
            let user_domain_name = self
                .config
                .user_domain_name
                .unwrap_or_else(|| "Default".to_string());
            let project_domain_name = self
                .config
                .project_domain_name
                .unwrap_or_else(|| "Default".to_string());

            let creds = KeystoneCredentials {
                auth_url,
                username,
                password,
                user_domain_name,
                project_name,
                project_domain_name,
            };

            let endpoint_lock = std::sync::OnceLock::new();

            // If an explicit endpoint was provided, set it now.
            if let Some(ep) = self.config.endpoint {
                let ep = if ep.starts_with("http") {
                    ep
                } else {
                    format!("https://{ep}")
                };
                let _ = endpoint_lock.set(ep);
            }
            // Otherwise it will be discovered from the Keystone catalog.

            let signer = SwiftSigner::new_keystone(info.clone(), creds);

            Ok(SwiftBackend {
                core: Arc::new(SwiftCore {
                    info,
                    root,

                    endpoint: endpoint_lock,
                    container,
                    signer: Arc::new(Mutex::new(signer)),
                }),
            })
        } else if has_token {
            // Static token mode (existing behavior).
            let endpoint = match self.config.endpoint {
                Some(endpoint) => {
                    if endpoint.starts_with("http") {
                        endpoint
                    } else {
                        format!("https://{endpoint}")
                    }
                }
                None => {
                    return Err(Error::new(
                        ErrorKind::ConfigInvalid,
                        "missing endpoint for Swift",
                    ));
                }
            };
            debug!("backend use endpoint: {}", &endpoint);

            let token = self.config.token.unwrap_or_default();
            let signer = SwiftSigner::new_static(info.clone(), token);

            let endpoint_lock = std::sync::OnceLock::new();
            let _ = endpoint_lock.set(endpoint);

            Ok(SwiftBackend {
                core: Arc::new(SwiftCore {
                    info,
                    root,

                    endpoint: endpoint_lock,
                    container,
                    signer: Arc::new(Mutex::new(signer)),
                }),
            })
        } else {
            Err(Error::new(
                ErrorKind::ConfigInvalid,
                "either token or auth_url (with credentials) must be provided for Swift",
            ))
        }
    }
}

/// Backend for Swift service
#[derive(Debug, Clone)]
pub struct SwiftBackend {
    core: Arc<SwiftCore>,
}

impl Access for SwiftBackend {
    type Reader = HttpBody;
    type Writer = oio::OneShotWriter<SwiftWriter>;
    type Lister = oio::PageLister<SwiftLister>;
    type Deleter = oio::OneShotDeleter<SwiftDeleter>;

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        // Ensure endpoint is resolved (Keystone mode may need first auth).
        self.core.ensure_endpoint().await?;

        let resp = self.core.swift_get_metadata(path).await?;

        match resp.status() {
            StatusCode::OK | StatusCode::NO_CONTENT => {
                let headers = resp.headers();
                let mut meta = parse_into_metadata(path, headers)?;
                let user_meta = parse_prefixed_headers(headers, "x-object-meta-");
                if !user_meta.is_empty() {
                    meta = meta.with_user_metadata(user_meta);
                }

                Ok(RpStat::new(meta))
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.core.ensure_endpoint().await?;

        let resp = self.core.swift_read(path, args.range(), &args).await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => Ok((RpRead::new(), resp.into_body())),
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                Err(parse_error(Response::from_parts(part, buf)))
            }
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.core.ensure_endpoint().await?;

        let writer = SwiftWriter::new(self.core.clone(), args.clone(), path.to_string());

        let w = oio::OneShotWriter::new(writer);

        Ok((RpWrite::default(), w))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        self.core.ensure_endpoint().await?;

        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(SwiftDeleter::new(self.core.clone())),
        ))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.core.ensure_endpoint().await?;

        let l = SwiftLister::new(
            self.core.clone(),
            path.to_string(),
            args.recursive(),
            args.limit(),
        );

        Ok((RpList::default(), oio::PageLister::new(l)))
    }

    async fn copy(&self, from: &str, to: &str, _args: OpCopy) -> Result<RpCopy> {
        self.core.ensure_endpoint().await?;

        // cannot copy objects larger than 5 GB.
        // Reference: https://docs.openstack.org/api-ref/object-store/#copy-object
        let resp = self.core.swift_copy(from, to).await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => Ok(RpCopy::default()),
            _ => Err(parse_error(resp)),
        }
    }
}
