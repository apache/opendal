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
use std::fmt::Formatter;
use std::sync::Arc;

use http::Response;
use http::StatusCode;
use log::debug;

use super::core::*;
use super::delete::SwfitDeleter;
use super::error::parse_error;
use super::lister::SwiftLister;
use super::writer::SwiftWriter;
use crate::raw::*;
use crate::services::SwiftConfig;
use crate::*;

impl Configurator for SwiftConfig {
    type Builder = SwiftBuilder;
    fn into_builder(self) -> Self::Builder {
        SwiftBuilder { config: self }
    }
}

/// [OpenStack Swift](https://docs.openstack.org/api-ref/object-store/#)'s REST API support.
/// For more information about swift-compatible services, refer to [Compatible Services](#compatible-services).
#[doc = include_str!("docs.md")]
#[doc = include_str!("compatible_services.md")]
#[derive(Default, Clone)]
pub struct SwiftBuilder {
    config: SwiftConfig,
}

impl Debug for SwiftBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("SwiftBuilder");
        d.field("config", &self.config);
        d.finish_non_exhaustive()
    }
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
}

impl Builder for SwiftBuilder {
    const SCHEME: Scheme = Scheme::Swift;
    type Config = SwiftConfig;

    /// Build a SwiftBackend.
    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root {}", root);

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

        let container = match self.config.container {
            Some(container) => container,
            None => {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "missing container for Swift",
                ));
            }
        };

        let token = self.config.token.unwrap_or_default();

        Ok(SwiftBackend {
            core: Arc::new(SwiftCore {
                info: {
                    let am = AccessorInfo::default();
                    am.set_scheme(Scheme::Swift)
                        .set_root(&root)
                        .set_native_capability(Capability {
                            stat: true,
                            stat_has_cache_control: true,
                            stat_has_content_length: true,
                            stat_has_content_type: true,
                            stat_has_content_encoding: true,
                            stat_has_content_range: true,
                            stat_has_etag: true,
                            stat_has_content_md5: true,
                            stat_has_last_modified: true,
                            stat_has_content_disposition: true,
                            stat_has_user_metadata: true,
                            read: true,

                            write: true,
                            write_can_empty: true,
                            write_with_user_metadata: true,

                            delete: true,

                            list: true,
                            list_with_recursive: true,
                            list_has_content_length: true,
                            list_has_content_md5: true,
                            list_has_content_type: true,
                            list_has_last_modified: true,

                            shared: true,

                            ..Default::default()
                        });
                    am.into()
                },
                root,
                endpoint,
                container,
                token,
            }),
        })
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
    type Deleter = oio::OneShotDeleter<SwfitDeleter>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        let resp = self.core.swift_get_metadata(path).await?;

        match resp.status() {
            StatusCode::OK | StatusCode::NO_CONTENT => {
                let headers = resp.headers();
                let mut meta = parse_into_metadata(path, headers)?;
                let user_meta = parse_prefixed_headers(headers, "x-object-meta-");
                if !user_meta.is_empty() {
                    meta.with_user_metadata(user_meta);
                }

                Ok(RpStat::new(meta))
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
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
        let writer = SwiftWriter::new(self.core.clone(), args.clone(), path.to_string());

        let w = oio::OneShotWriter::new(writer);

        Ok((RpWrite::default(), w))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(SwfitDeleter::new(self.core.clone())),
        ))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = SwiftLister::new(
            self.core.clone(),
            path.to_string(),
            args.recursive(),
            args.limit(),
        );

        Ok((RpList::default(), oio::PageLister::new(l)))
    }

    async fn copy(&self, from: &str, to: &str, _args: OpCopy) -> Result<RpCopy> {
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
