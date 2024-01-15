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

use crate::raw::*;
use crate::services::icloud::client::Client;
use crate::services::icloud::session::{Session, SessionData};
use crate::*;
use crate::{Builder, Capability, Error, ErrorKind, Scheme};
use async_trait::async_trait;
use log::debug;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use http::StatusCode;
use tokio::sync::Mutex;
use crate::services::icloud::error::parse_error;

#[derive(Default)]
pub struct iCloudBuilder {
    pub root: Option<String>,
    pub apple_id: Option<String>,
    pub password: Option<String>,
    pub trust_token: Option<String>,
    pub ds_web_auth_token: Option<String>,

    pub http_client: Option<HttpClient>,
}

impl Debug for iCloudBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("iCloudBuilder");
        d.field("root", &self.root);
        d.field("apple_id", &self.apple_id);
        d.field("password", &self.password);
        d.field("trust_token", &self.trust_token);
        d.field("ds_web_auth_token",&self.ds_web_auth_token);

        d.finish_non_exhaustive()
    }
}

impl iCloudBuilder {
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.root = Some(root.to_string());
        self
    }

    pub fn apple_id(&mut self, apple_id: &str) -> &mut Self {
        self.apple_id = if apple_id.is_empty() {
            None
        } else {
            Some(apple_id.to_string())
        };

        self
    }

    pub fn password(&mut self, password: &str) -> &mut Self {
        self.password = if password.is_empty() {
            None
        } else {
            Some(password.to_string())
        };

        self
    }

    pub fn trust_token(&mut self, trust_token: &str) -> &mut Self {
        self.trust_token = if trust_token.is_empty() {
            None
        } else {
            Some(trust_token.to_string())
        };

        self
    }

    pub fn ds_web_auth_token(&mut self, ds_web_auth_token: &str) -> &mut Self {
        self.ds_web_auth_token = if ds_web_auth_token.is_empty() {
            None
        } else {
            Some(ds_web_auth_token.to_string())
        };

        self
    }

    pub fn http_client(&mut self, client: HttpClient) -> &mut Self {
        self.http_client = Some(client);
        self
    }
}

impl Builder for iCloudBuilder {
    const SCHEME: Scheme = Scheme::Icloud;
    type Accessor = iCloudBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = Self::default();

        map.get("root").map(|v| builder.root(v));
        map.get("apple_id").map(|v| builder.apple_id(v));
        map.get("password").map(|v| builder.password(v));
        map.get("trust_token").map(|v| builder.trust_token(v));
        map.get("ds_web_auth_token").map(|v|builder.ds_web_auth_token(v));

        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {

        let root = normalize_root(&self.root.take().unwrap_or_default());

        let apple_id = match &self.apple_id {
            Some(apple_id) => Ok(apple_id.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "apple_id is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Icloud)),
        }?;

        let password = match &self.password {
            Some(password) => Ok(password.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "password is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Icloud)),
        }?;

        let ds_web_auth_token=match &self.ds_web_auth_token {
            Some(ds_web_auth_token) => Ok(ds_web_auth_token.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "ds_web_auth_token is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Icloud)),
        }?;

        let client = if let Some(client) = self.http_client.take() {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Icloud)
            })?
        };

        let trust_token = match &self.trust_token {
            Some(trust_token) => Ok(trust_token.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "trust_token is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Icloud)),
        }?;
        debug!("iCloud backend use trust_token {}", &trust_token);

        let session_data = SessionData::new();

        Ok(iCloudBackend {
            client: Arc::new(Client {
                session: Arc::new(Mutex::new(Session {
                    data: session_data,
                    client,
                    apple_id,
                    password,
                    trust_token: Some(trust_token),
                    ds_web_auth_token: Some(ds_web_auth_token),
                })),
                root,
                path_cache: Arc::new(Default::default()),
            }),
        })
    }
}

#[derive(Debug, Clone)]
pub struct iCloudBackend {
    client: Arc<Client>,
}

#[async_trait]
impl Accessor for iCloudBackend {
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
        // iCloud get the filename by id, instead obtain the metadata by filename
        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        let node = self.client.stat(path).await?;



        let mut meta = Metadata::new(match node.type_field.as_str() {
            "FOLDER" => EntryMode::DIR,
            _ => EntryMode::FILE,
        });

        if meta.mode()==EntryMode::DIR || path.ends_with('/') {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        meta = meta.with_content_length(node.size);

        let last_modified= parse_datetime_from_rfc3339(&node.date_modified).map_err(|e| {
            Error::new(ErrorKind::Unexpected, "parse last modified time").set_source(e)
        })?;
        meta=meta.with_last_modified(last_modified);

        Ok(RpStat::new(meta))
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp=self.client.read(path,&args).await?;
        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                let size = parse_content_length(resp.headers())?;
                let range = parse_content_range(resp.headers())?;
                Ok((
                    //0-1023
                    RpRead::new().with_size(size).with_range(range),
                    resp.into_body(),
                ))
            }
            StatusCode::RANGE_NOT_SATISFIABLE =>{
                resp.into_body().consume().await?;
                Ok((RpRead::new().with_size(Some(0)), IncomingAsyncBody::empty()))
            }
            _ => Err(parse_error(resp).await?),
        }

    }
}
