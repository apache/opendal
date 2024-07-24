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
use serde::{Deserialize, Serialize};

use super::core::*;
use super::error::parse_error;
use super::lister::UpyunLister;
use super::writer::UpyunWriter;
use super::writer::UpyunWriters;
use crate::raw::*;
use crate::*;

/// Config for backblaze upyun services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct UpyunConfig {
    /// root of this backend.
    ///
    /// All operations will happen under this root.
    pub root: Option<String>,
    /// bucket address of this backend.
    pub bucket: String,
    /// username of this backend.
    pub operator: Option<String>,
    /// password of this backend.
    pub password: Option<String>,
}

impl Debug for UpyunConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Config");

        ds.field("root", &self.root);
        ds.field("bucket", &self.bucket);
        ds.field("operator", &self.operator);

        ds.finish()
    }
}

impl Configurator for UpyunConfig {
    fn into_builder(self) -> impl Builder {
        UpyunBuilder {
            config: self,
            http_client: None,
        }
    }
}

/// [upyun](https://www.upyun.com/products/file-storage) services support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct UpyunBuilder {
    config: UpyunConfig,

    http_client: Option<HttpClient>,
}

impl Debug for UpyunBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("UpyunBuilder");

        d.field("config", &self.config);
        d.finish_non_exhaustive()
    }
}

impl UpyunBuilder {
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

    /// bucket of this backend.
    ///
    /// It is required. e.g. `test`
    pub fn bucket(&mut self, bucket: &str) -> &mut Self {
        self.config.bucket = bucket.to_string();

        self
    }

    /// operator of this backend.
    ///
    /// It is required. e.g. `test`
    pub fn operator(&mut self, operator: &str) -> &mut Self {
        self.config.operator = if operator.is_empty() {
            None
        } else {
            Some(operator.to_string())
        };

        self
    }

    /// password of this backend.
    ///
    /// It is required. e.g. `asecret`
    pub fn password(&mut self, password: &str) -> &mut Self {
        self.config.password = if password.is_empty() {
            None
        } else {
            Some(password.to_string())
        };

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

impl Builder for UpyunBuilder {
    const SCHEME: Scheme = Scheme::Upyun;
    type Config = UpyunConfig;

    /// Builds the backend and returns the result of UpyunBackend.
    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.config.root.clone().unwrap_or_default());
        debug!("backend use root {}", &root);

        // Handle bucket.
        if self.config.bucket.is_empty() {
            return Err(Error::new(ErrorKind::ConfigInvalid, "bucket is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Upyun));
        }

        debug!("backend use bucket {}", &self.config.bucket);

        let operator = match &self.config.operator {
            Some(operator) => Ok(operator.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "operator is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Upyun)),
        }?;

        let password = match &self.config.password {
            Some(password) => Ok(password.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "password is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Upyun)),
        }?;

        let client = if let Some(client) = self.http_client {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Upyun)
            })?
        };

        let signer = UpyunSigner {
            operator: operator.clone(),
            password: password.clone(),
        };

        Ok(UpyunBackend {
            core: Arc::new(UpyunCore {
                root,
                operator,
                bucket: self.config.bucket.clone(),
                signer,
                client,
            }),
        })
    }
}

/// Backend for upyun services.
#[derive(Debug, Clone)]
pub struct UpyunBackend {
    core: Arc<UpyunCore>,
}

impl Access for UpyunBackend {
    type Reader = HttpBody;
    type Writer = UpyunWriters;
    type Lister = oio::PageLister<UpyunLister>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();

    fn info(&self) -> Arc<AccessorInfo> {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Upyun)
            .set_root(&self.core.root)
            .set_native_capability(Capability {
                stat: true,

                create_dir: true,

                read: true,

                write: true,
                write_can_empty: true,
                write_can_multi: true,
                write_with_cache_control: true,
                write_with_content_type: true,

                // https://help.upyun.com/knowledge-base/rest_api/#e5b9b6e8a18ce5bc8fe696ade782b9e7bbade4bca0
                write_multi_min_size: Some(1024 * 1024),
                write_multi_max_size: Some(50 * 1024 * 1024),

                delete: true,
                rename: true,
                copy: true,

                list: true,
                list_with_limit: true,

                ..Default::default()
            });

        am.into()
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        let resp = self.core.create_dir(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(RpCreateDir::default()),
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        let resp = self.core.info(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => parse_info(resp.headers()).map(RpStat::new),
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.core.download_file(path, args.range()).await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                Ok((RpRead::default(), resp.into_body()))
            }
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                Err(parse_error(Response::from_parts(part, buf)).await?)
            }
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let concurrent = args.concurrent();
        let executor = args.executor().cloned();
        let writer = UpyunWriter::new(self.core.clone(), args, path.to_string());

        let w = oio::MultipartWriter::new(writer, executor, concurrent);

        Ok((RpWrite::default(), w))
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let resp = self.core.delete(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(RpDelete::default()),
            // Allow 404 when deleting a non-existing object
            StatusCode::NOT_FOUND => Ok(RpDelete::default()),
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = UpyunLister::new(self.core.clone(), path, args.limit());
        Ok((RpList::default(), oio::PageLister::new(l)))
    }

    async fn copy(&self, from: &str, to: &str, _args: OpCopy) -> Result<RpCopy> {
        let resp = self.core.copy(from, to).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(RpCopy::default()),
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn rename(&self, from: &str, to: &str, _args: OpRename) -> Result<RpRename> {
        let resp = self.core.move_object(from, to).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(RpRename::default()),
            _ => Err(parse_error(resp).await?),
        }
    }
}
