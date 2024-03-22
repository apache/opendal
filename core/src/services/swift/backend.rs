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

use async_trait::async_trait;
use http::StatusCode;
use log::debug;

use super::core::*;
use super::error::parse_error;
use super::lister::SwiftLister;
use super::writer::SwiftWriter;
use crate::raw::*;
use crate::services::swift::reader::SwiftReader;
use crate::*;

/// [OpenStack Swift](https://docs.openstack.org/api-ref/object-store/#)'s REST API support.
/// For more information about swift-compatible services, refer to [Compatible Services](#compatible-services).
#[doc = include_str!("docs.md")]
#[doc = include_str!("compatible_services.md")]
#[derive(Default, Clone)]
pub struct SwiftBuilder {
    endpoint: Option<String>,
    container: Option<String>,
    root: Option<String>,
    token: Option<String>,
}

impl Debug for SwiftBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Builder");

        ds.field("root", &self.root);
        ds.field("endpoint", &self.endpoint);
        ds.field("container", &self.container);

        if self.token.is_some() {
            ds.field("token", &"<redacted>");
        }

        ds.finish()
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
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        self.endpoint = if endpoint.is_empty() {
            None
        } else {
            Some(endpoint.trim_end_matches('/').to_string())
        };
        self
    }

    /// Set container of this backend.
    ///
    /// All operations will happen under this container. It is required. e.g. `snapshots`
    pub fn container(&mut self, container: &str) -> &mut Self {
        self.container = if container.is_empty() {
            None
        } else {
            Some(container.trim_end_matches('/').to_string())
        };
        self
    }

    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(&mut self, root: &str) -> &mut Self {
        if !root.is_empty() {
            self.root = Some(root.to_string())
        }

        self
    }

    /// Set the token of this backend.
    ///
    /// Default to empty string.
    pub fn token(&mut self, token: &str) -> &mut Self {
        if !token.is_empty() {
            self.token = Some(token.to_string());
        }
        self
    }
}

impl Builder for SwiftBuilder {
    const SCHEME: Scheme = Scheme::Swift;
    type Accessor = SwiftBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = SwiftBuilder::default();

        map.get("endpoint").map(|v| builder.endpoint(v));
        map.get("container").map(|v| builder.container(v));
        map.get("token").map(|v| builder.token(v));

        builder
    }

    /// Build a SwiftBackend.
    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.root.take().unwrap_or_default());
        debug!("backend use root {}", root);

        let endpoint = match self.endpoint.take() {
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

        let container = match self.container.take() {
            Some(container) => container,
            None => {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "missing container for Swift",
                ));
            }
        };
        debug!("backend use container: {}", &container);

        let token = match self.token.take() {
            Some(token) => token,
            None => String::new(),
        };

        let client = HttpClient::new()?;

        debug!("backend build finished: {:?}", &self);
        Ok(SwiftBackend {
            core: Arc::new(SwiftCore {
                root,
                endpoint,
                container,
                token,
                client,
            }),
        })
    }
}

/// Backend for Swift service
#[derive(Debug, Clone)]
pub struct SwiftBackend {
    core: Arc<SwiftCore>,
}

#[async_trait]
impl Accessor for SwiftBackend {
    type Reader = SwiftReader;
    type Writer = oio::OneShotWriter<SwiftWriter>;
    type Lister = oio::PageLister<SwiftLister>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();

    fn info(&self) -> AccessorInfo {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Swift)
            .set_root(&self.core.root)
            .set_native_capability(Capability {
                stat: true,

                read: true,

                write: true,
                write_can_empty: true,
                delete: true,

                list: true,
                list_with_recursive: true,

                ..Default::default()
            });
        am
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        let resp = self.core.swift_get_metadata(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::NO_CONTENT => {
                let meta = parse_into_metadata(path, resp.headers())?;
                Ok(RpStat::new(meta))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        Ok((
            RpRead::default(),
            SwiftReader::new(self.core.clone(), path, args),
        ))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let writer = SwiftWriter::new(self.core.clone(), args.clone(), path.to_string());

        let w = oio::OneShotWriter::new(writer);

        return Ok((RpWrite::default(), w));
    }

    async fn delete(&self, path: &str, _args: OpDelete) -> Result<RpDelete> {
        let resp = self.core.swift_delete(path).await?;

        let status = resp.status();

        match status {
            StatusCode::NO_CONTENT | StatusCode::OK => Ok(RpDelete::default()),
            StatusCode::NOT_FOUND => Ok(RpDelete::default()),
            _ => Err(parse_error(resp).await?),
        }
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
            _ => Err(parse_error(resp).await?),
        }
    }
}
