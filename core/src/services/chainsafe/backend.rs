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
use bytes::Buf;
use http::StatusCode;
use log::debug;
use serde::Deserialize;

use super::core::parse_info;
use super::core::ChainsafeCore;
use super::core::ObjectInfoResponse;
use super::error::parse_error;
use super::lister::ChainsafeLister;
use super::reader::ChainsafeReader;
use super::writer::ChainsafeWriter;
use super::writer::ChainsafeWriters;
use crate::raw::*;
use crate::*;

/// Config for backblaze Chainsafe services support.
#[derive(Default, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct ChainsafeConfig {
    /// root of this backend.
    ///
    /// All operations will happen under this root.
    pub root: Option<String>,
    /// api_key of this backend.
    pub api_key: Option<String>,
    /// bucket_id of this backend.
    ///
    /// required.
    pub bucket_id: String,
}

impl Debug for ChainsafeConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("ChainsafeConfig");

        d.field("root", &self.root)
            .field("bucket_id", &self.bucket_id);

        d.finish_non_exhaustive()
    }
}

/// [chainsafe](https://storage.chainsafe.io/) services support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct ChainsafeBuilder {
    config: ChainsafeConfig,

    http_client: Option<HttpClient>,
}

impl Debug for ChainsafeBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("ChainsafeBuilder");

        d.field("config", &self.config);
        d.finish_non_exhaustive()
    }
}

impl ChainsafeBuilder {
    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// api_key of this backend.
    ///
    /// required.
    pub fn api_key(&mut self, api_key: &str) -> &mut Self {
        self.config.api_key = if api_key.is_empty() {
            None
        } else {
            Some(api_key.to_string())
        };

        self
    }

    /// Set bucket_id name of this backend.
    pub fn bucket_id(&mut self, bucket_id: &str) -> &mut Self {
        self.config.bucket_id = bucket_id.to_string();

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

impl Builder for ChainsafeBuilder {
    const SCHEME: Scheme = Scheme::Chainsafe;
    type Accessor = ChainsafeBackend;

    /// Converts a HashMap into an ChainsafeBuilder instance.
    ///
    /// # Arguments
    ///
    /// * `map` - A HashMap containing the configuration values.
    ///
    /// # Returns
    ///
    /// Returns an instance of ChainsafeBuilder.
    fn from_map(map: HashMap<String, String>) -> Self {
        // Deserialize the configuration from the HashMap.
        let config = ChainsafeConfig::deserialize(ConfigDeserializer::new(map))
            .expect("config deserialize must succeed");

        // Create an ChainsafeBuilder instance with the deserialized config.
        ChainsafeBuilder {
            config,
            http_client: None,
        }
    }

    /// Builds the backend and returns the result of ChainsafeBackend.
    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.config.root.clone().unwrap_or_default());
        debug!("backend use root {}", &root);

        // Handle bucket_id.
        if self.config.bucket_id.is_empty() {
            return Err(Error::new(ErrorKind::ConfigInvalid, "bucket_id is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Chainsafe));
        }

        debug!("backend use bucket_id {}", &self.config.bucket_id);

        let api_key = match &self.config.api_key {
            Some(api_key) => Ok(api_key.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "api_key is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Chainsafe)),
        }?;

        let client = if let Some(client) = self.http_client.take() {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Chainsafe)
            })?
        };

        Ok(ChainsafeBackend {
            core: Arc::new(ChainsafeCore {
                root,
                api_key,
                bucket_id: self.config.bucket_id.clone(),
                client,
            }),
        })
    }
}

/// Backend for Chainsafe services.
#[derive(Debug, Clone)]
pub struct ChainsafeBackend {
    core: Arc<ChainsafeCore>,
}

#[async_trait]
impl Accessor for ChainsafeBackend {
    type Reader = ChainsafeReader;
    type Writer = ChainsafeWriters;
    type Lister = oio::PageLister<ChainsafeLister>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();

    fn info(&self) -> AccessorInfo {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Chainsafe)
            .set_root(&self.core.root)
            .set_native_capability(Capability {
                stat: true,

                read: true,

                create_dir: true,
                write: true,
                write_can_empty: true,

                delete: true,

                list: true,

                ..Default::default()
            });

        am
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        let resp = self.core.create_dir(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(RpCreateDir::default()),
            // Allow 409 when creating a existing dir
            StatusCode::CONFLICT => Ok(RpCreateDir::default()),
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        let resp = self.core.object_info(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let bs = resp.into_body();

                let output: ObjectInfoResponse =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;
                Ok(RpStat::new(parse_info(output.content)))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        Ok((
            RpRead::default(),
            ChainsafeReader::new(self.core.clone(), path, args),
        ))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let writer = ChainsafeWriter::new(self.core.clone(), args, path.to_string());

        let w = oio::OneShotWriter::new(writer);

        Ok((RpWrite::default(), w))
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let resp = self.core.delete_object(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(RpDelete::default()),
            // Allow 404 when deleting a non-existing object
            StatusCode::NOT_FOUND => Ok(RpDelete::default()),
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn list(&self, path: &str, _args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = ChainsafeLister::new(self.core.clone(), path);
        Ok((RpList::default(), oio::PageLister::new(l)))
    }
}
