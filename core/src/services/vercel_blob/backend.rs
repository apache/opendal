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

use super::core::parse_blob;
use super::core::Blob;
use super::core::VercelBlobCore;
use super::error::parse_error;
use super::lister::VercelBlobLister;
use super::writer::VercelBlobWriter;
use super::writer::VercelBlobWriters;
use crate::raw::*;
use crate::services::vercel_blob::reader::VercelBlobReader;
use crate::*;

/// Config for backblaze VercelBlob services support.
#[derive(Default, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct VercelBlobConfig {
    /// root of this backend.
    ///
    /// All operations will happen under this root.
    pub root: Option<String>,
    /// vercel blob token.
    pub token: String,
}

impl Debug for VercelBlobConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Config");

        ds.field("root", &self.root);

        ds.finish()
    }
}

/// [VercelBlob](https://vercel.com/docs/storage/vercel-blob) services support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct VercelBlobBuilder {
    config: VercelBlobConfig,

    http_client: Option<HttpClient>,
}

impl Debug for VercelBlobBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("VercelBlobBuilder");

        d.field("config", &self.config);
        d.finish_non_exhaustive()
    }
}

impl VercelBlobBuilder {
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

    /// Vercel Blob token.
    ///
    /// Get from Vercel environment variable `BLOB_READ_WRITE_TOKEN`.
    /// It is required.
    pub fn token(&mut self, token: &str) -> &mut Self {
        self.config.token = token.to_string();

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

impl Builder for VercelBlobBuilder {
    const SCHEME: Scheme = Scheme::VercelBlob;
    type Accessor = VercelBlobBackend;

    /// Converts a HashMap into an VercelBlobBuilder instance.
    ///
    /// # Arguments
    ///
    /// * `map` - A HashMap containing the configuration values.
    ///
    /// # Returns
    ///
    /// Returns an instance of VercelBlobBuilder.
    fn from_map(map: HashMap<String, String>) -> Self {
        // Deserialize the configuration from the HashMap.
        let config = VercelBlobConfig::deserialize(ConfigDeserializer::new(map))
            .expect("config deserialize must succeed");

        // Create an VercelBlobBuilder instance with the deserialized config.
        VercelBlobBuilder {
            config,
            http_client: None,
        }
    }

    /// Builds the backend and returns the result of VercelBlobBackend.
    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.config.root.clone().unwrap_or_default());
        debug!("backend use root {}", &root);

        // Handle token.
        if self.config.token.is_empty() {
            return Err(Error::new(ErrorKind::ConfigInvalid, "token is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::VercelBlob));
        }

        let client = if let Some(client) = self.http_client.take() {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::VercelBlob)
            })?
        };

        Ok(VercelBlobBackend {
            core: Arc::new(VercelBlobCore {
                root,
                token: self.config.token.clone(),
                client,
            }),
        })
    }
}

/// Backend for VercelBlob services.
#[derive(Debug, Clone)]
pub struct VercelBlobBackend {
    core: Arc<VercelBlobCore>,
}

#[async_trait]
impl Accessor for VercelBlobBackend {
    type Reader = VercelBlobReader;
    type Writer = VercelBlobWriters;
    type Lister = oio::PageLister<VercelBlobLister>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();

    fn info(&self) -> AccessorInfo {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::VercelBlob)
            .set_root(&self.core.root)
            .set_native_capability(Capability {
                stat: true,

                read: true,

                write: true,
                write_can_empty: true,
                write_can_multi: true,
                write_multi_min_size: Some(5 * 1024 * 1024),

                delete: true,
                copy: true,

                list: true,
                list_with_limit: true,

                ..Default::default()
            });

        am
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        self.core.head(path).await.map(RpStat::new)
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        Ok((
            RpRead::default(),
            VercelBlobReader::new(self.core.clone(), path, args),
        ))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let concurrent = args.concurrent();
        let writer = VercelBlobWriter::new(self.core.clone(), args, path.to_string());

        let w = oio::MultipartWriter::new(writer, concurrent);

        Ok((RpWrite::default(), w))
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        self.core.delete(path).await.map(|_| RpDelete::default())
    }

    async fn copy(&self, from: &str, to: &str, _args: OpCopy) -> Result<RpCopy> {
        self.core.copy(from, to).await.map(|_| RpCopy::default())
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = VercelBlobLister::new(self.core.clone(), path, args.limit());
        Ok((RpList::default(), oio::PageLister::new(l)))
    }
}
