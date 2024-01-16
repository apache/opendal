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

use async_trait::async_trait;
use http::StatusCode;
use log::debug;
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::raw::*;
use crate::*;
use crate::{Builder, Capability, Error, ErrorKind, Scheme};

use super::client::Client;
use super::core::{parse_error, IcloudSigner, SessionData};

/// Config for icloud services support.
#[derive(Default, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct IcloudConfig {
    /// root of this backend.
    ///
    /// All operations will happen under this root.
    ///
    /// default to `/` if not set.
    pub root: Option<String>,
    /// apple_id of this backend.
    ///
    /// apple_id must be full, mostly like `example@gmail.com`.
    pub apple_id: Option<String>,
    /// password of this backend.
    ///
    /// password must be full.
    pub password: Option<String>,

    /// Session
    ///
    /// token must be valid.
    pub trust_token: Option<String>,
    pub ds_web_auth_token: Option<String>,
    /// enable the china origin
    /// China region `origin` Header needs to be set to "https://www.icloud.com.cn".
    ///
    /// otherwise Apple server will return 302.
    pub is_china_mainland: bool,
}

impl Debug for IcloudConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("IcloudBuilder");
        d.field("root", &self.root);
        d.field("is_china_mainland", &self.is_china_mainland);
        d.finish_non_exhaustive()
    }
}

/// [IcloudDrive](https://www.icloud.com/iclouddrive/) service support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct IcloudBuilder {
    /// icloud config for web session request
    pub config: IcloudConfig,
    /// Specify the http client that used by this service.
    ///
    /// # Notes
    ///
    /// This API is part of OpenDAL's Raw API. `HttpClient` could be changed
    /// during minor updates.
    pub http_client: Option<HttpClient>,
}

impl Debug for IcloudBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("IcloudBuilder");

        d.field("config", &self.config);
        d.finish_non_exhaustive()
    }
}

impl IcloudBuilder {
    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.config.root = Some(root.to_string());
        self
    }

    /// Your Apple id
    ///
    /// It is required. your Apple login email, e.g. `example@gmail.com`
    pub fn apple_id(&mut self, apple_id: &str) -> &mut Self {
        self.config.apple_id = if apple_id.is_empty() {
            None
        } else {
            Some(apple_id.to_string())
        };

        self
    }

    /// Your Apple id password
    ///
    /// It is required. your icloud login password, e.g. `password`
    pub fn password(&mut self, password: &str) -> &mut Self {
        self.config.password = if password.is_empty() {
            None
        } else {
            Some(password.to_string())
        };

        self
    }

    /// Trust token and ds_web_auth_token is used for temporary access to the icloudDrive API.
    ///
    /// Authenticate using session token
    pub fn trust_token(&mut self, trust_token: &str) -> &mut Self {
        self.config.trust_token = if trust_token.is_empty() {
            None
        } else {
            Some(trust_token.to_string())
        };

        self
    }
    /// ds_web_auth_token must be set in Session
    ///
    /// Avoid Two Factor Authentication
    pub fn ds_web_auth_token(&mut self, ds_web_auth_token: &str) -> &mut Self {
        self.config.ds_web_auth_token = if ds_web_auth_token.is_empty() {
            None
        } else {
            Some(ds_web_auth_token.to_string())
        };

        self
    }
    /// Enable the china origin
    /// For China, use "https://www.icloud.com.cn"
    /// For Other region, use "https://www.icloud.com"
    pub fn is_china_mainland(&mut self, is_china_mainland: bool) -> &mut Self {
        self.config.is_china_mainland = is_china_mainland;
        self
    }

    /// Specify the http client that used by this service.
    ///
    /// # Notes
    ///
    /// This API is part of OpenDAL's Raw API. `HttpClient` could be changed
    /// during minor updates.
    pub fn http_client(&mut self, client: HttpClient) -> &mut Self {
        self.http_client = Some(client);
        self
    }
}

impl Builder for IcloudBuilder {
    const SCHEME: Scheme = Scheme::Icloud;
    type Accessor = IcloudBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let config = IcloudConfig::deserialize(ConfigDeserializer::new(map))
            .expect("config deserialize must succeed");

        IcloudBuilder {
            config,
            http_client: None,
        }
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        let root = normalize_root(&self.config.root.take().unwrap_or_default());

        let apple_id = match &self.config.apple_id {
            Some(apple_id) => Ok(apple_id.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "apple_id is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Icloud)),
        }?;

        let password = match &self.config.password {
            Some(password) => Ok(password.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "password is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Icloud)),
        }?;

        let ds_web_auth_token = match &self.config.ds_web_auth_token {
            Some(ds_web_auth_token) => Ok(ds_web_auth_token.clone()),
            None => Err(
                Error::new(ErrorKind::ConfigInvalid, "ds_web_auth_token is empty")
                    .with_operation("Builder::build")
                    .with_context("service", Scheme::Icloud),
            ),
        }?;

        let trust_token = match &self.config.trust_token {
            Some(trust_token) => Ok(trust_token.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "trust_token is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Icloud)),
        }?;

        debug!(
            "Icloud backend is_china_mainland {}",
            &self.config.is_china_mainland
        );

        let client = if let Some(client) = self.http_client.take() {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Icloud)
            })?
        };

        let session_data = SessionData::new();

        Ok(IcloudBackend {
            client: Arc::new(Client {
                core: Arc::new(Mutex::new(IcloudSigner {
                    client,

                    data: session_data,
                    apple_id,
                    password,
                    trust_token: Some(trust_token),
                    ds_web_auth_token: Some(ds_web_auth_token),
                    is_china_mainland: self.config.is_china_mainland,
                })),
                root,
                path_cache: Arc::new(Default::default()),
            }),
        })
    }
}

#[derive(Debug, Clone)]
pub struct IcloudBackend {
    client: Arc<Client>,
}

#[async_trait]
impl Accessor for IcloudBackend {
    type Reader = IncomingAsyncBody;
    type BlockingReader = ();
    type Writer = ();
    type BlockingWriter = ();
    type Lister = ();
    type BlockingLister = ();

    fn info(&self) -> AccessorInfo {
        let mut ma = AccessorInfo::default();
        ma.set_scheme(Scheme::Icloud)
            .set_root(&self.client.root)
            .set_native_capability(Capability {
                stat: true,
                read: true,
                ..Default::default()
            });
        ma
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        // icloud get the filename by id, instead obtain the metadata by filename
        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        let node = self.client.stat(path).await?;

        let mut meta = Metadata::new(match node.type_field.as_str() {
            "FOLDER" => EntryMode::DIR,
            _ => EntryMode::FILE,
        });

        if meta.mode() == EntryMode::DIR || path.ends_with('/') {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        meta = meta.with_content_length(node.size);

        let last_modified = parse_datetime_from_rfc3339(&node.date_modified)?;
        meta = meta.with_last_modified(last_modified);

        Ok(RpStat::new(meta))
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.client.read(path, &args).await?;
        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                let size = parse_content_length(resp.headers())?;
                let range = parse_content_range(resp.headers())?;
                Ok((
                    RpRead::new().with_size(size).with_range(range),
                    resp.into_body(),
                ))
            }
            StatusCode::RANGE_NOT_SATISFIABLE => {
                resp.into_body().consume().await?;
                Ok((RpRead::new().with_size(Some(0)), IncomingAsyncBody::empty()))
            }
            _ => Err(parse_error(resp).await?),
        }
    }
}
