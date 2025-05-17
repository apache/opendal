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
use tokio::sync::Mutex;

use super::core::*;
use crate::raw::*;
use crate::services::IcloudConfig;
use crate::*;

impl Configurator for IcloudConfig {
    type Builder = IcloudBuilder;

    #[allow(deprecated)]
    fn into_builder(self) -> Self::Builder {
        IcloudBuilder {
            config: self,
            http_client: None,
        }
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
    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
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
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// Your Apple id
    ///
    /// It is required. your Apple login email, e.g. `example@gmail.com`
    pub fn apple_id(mut self, apple_id: &str) -> Self {
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
    pub fn password(mut self, password: &str) -> Self {
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
    pub fn trust_token(mut self, trust_token: &str) -> Self {
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
    pub fn ds_web_auth_token(mut self, ds_web_auth_token: &str) -> Self {
        self.config.ds_web_auth_token = if ds_web_auth_token.is_empty() {
            None
        } else {
            Some(ds_web_auth_token.to_string())
        };

        self
    }

    /// Set if your apple id in China mainland.
    ///
    /// If in china mainland, we will connect to `https://www.icloud.com.cn`.
    /// Otherwise, we will connect to `https://www.icloud.com`.
    pub fn is_china_mainland(mut self, is_china_mainland: bool) -> Self {
        self.config.is_china_mainland = is_china_mainland;
        self
    }

    /// Specify the http client that used by this service.
    ///
    /// # Notes
    ///
    /// This API is part of OpenDAL's Raw API. `HttpClient` could be changed
    /// during minor updates.
    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
    #[allow(deprecated)]
    pub fn http_client(mut self, client: HttpClient) -> Self {
        self.http_client = Some(client);
        self
    }
}

impl Builder for IcloudBuilder {
    const SCHEME: Scheme = Scheme::Icloud;
    type Config = IcloudConfig;

    fn build(self) -> Result<impl Access> {
        let root = normalize_root(&self.config.root.unwrap_or_default());

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

        let session_data = SessionData::new();

        let info = AccessorInfo::default();
        info.set_scheme(Scheme::Icloud)
            .set_root(&root)
            .set_native_capability(Capability {
                stat: true,
                stat_has_content_length: true,
                stat_has_last_modified: true,

                read: true,

                shared: true,
                ..Default::default()
            });

        // allow deprecated api here for compatibility
        #[allow(deprecated)]
        if let Some(client) = self.http_client {
            info.update_http_client(|_| client);
        }

        let accessor_info = Arc::new(info);

        let signer = IcloudSigner {
            info: accessor_info.clone(),
            data: session_data,
            apple_id,
            password,
            trust_token: Some(trust_token),
            ds_web_auth_token: Some(ds_web_auth_token),
            is_china_mainland: self.config.is_china_mainland,
            initiated: false,
        };

        let signer = Arc::new(Mutex::new(signer));
        Ok(IcloudBackend {
            core: Arc::new(IcloudCore {
                info: accessor_info,
                signer: signer.clone(),
                root,
                path_cache: PathCacher::new(IcloudPathQuery::new(signer.clone())),
            }),
        })
    }
}

#[derive(Debug, Clone)]
pub struct IcloudBackend {
    core: Arc<IcloudCore>,
}

impl Access for IcloudBackend {
    type Reader = HttpBody;
    type Writer = ();
    type Lister = ();
    type Deleter = ();
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        // icloud get the filename by id, instead obtain the metadata by filename
        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        let node = self.core.stat(path).await?;

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
        let resp = self.core.read(path, args.range(), &args).await?;

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
}
