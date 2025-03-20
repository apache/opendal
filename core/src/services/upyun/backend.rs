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
use super::delete::UpyunDeleter;
use super::error::parse_error;
use super::lister::UpyunLister;
use super::writer::UpyunWriter;
use super::writer::UpyunWriters;
use crate::raw::*;
use crate::services::UpyunConfig;
use crate::*;

impl Configurator for UpyunConfig {
    type Builder = UpyunBuilder;

    #[allow(deprecated)]
    fn into_builder(self) -> Self::Builder {
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

    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
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
    pub fn root(mut self, root: &str) -> Self {
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
    pub fn bucket(mut self, bucket: &str) -> Self {
        self.config.bucket = bucket.to_string();

        self
    }

    /// operator of this backend.
    ///
    /// It is required. e.g. `test`
    pub fn operator(mut self, operator: &str) -> Self {
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
    pub fn password(mut self, password: &str) -> Self {
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
    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
    #[allow(deprecated)]
    pub fn http_client(mut self, client: HttpClient) -> Self {
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

        let signer = UpyunSigner {
            operator: operator.clone(),
            password: password.clone(),
        };

        Ok(UpyunBackend {
            core: Arc::new(UpyunCore {
                info: {
                    let am = AccessorInfo::default();
                    am.set_scheme(Scheme::Upyun)
                        .set_root(&root)
                        .set_native_capability(Capability {
                            stat: true,
                            stat_has_content_length: true,
                            stat_has_content_type: true,
                            stat_has_content_md5: true,
                            stat_has_cache_control: true,
                            stat_has_content_disposition: true,

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
                            list_has_content_length: true,
                            list_has_content_type: true,
                            list_has_last_modified: true,

                            shared: true,

                            ..Default::default()
                        });

                    // allow deprecated api here for compatibility
                    #[allow(deprecated)]
                    if let Some(client) = self.http_client {
                        am.update_http_client(|_| client);
                    }

                    am.into()
                },
                root,
                operator,
                bucket: self.config.bucket.clone(),
                signer,
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
    type Deleter = oio::OneShotDeleter<UpyunDeleter>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        let resp = self.core.create_dir(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(RpCreateDir::default()),
            _ => Err(parse_error(resp)),
        }
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        let resp = self.core.info(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => parse_info(resp.headers()).map(RpStat::new),
            _ => Err(parse_error(resp)),
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
                Err(parse_error(Response::from_parts(part, buf)))
            }
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let concurrent = args.concurrent();
        let writer = UpyunWriter::new(self.core.clone(), args, path.to_string());

        let w = oio::MultipartWriter::new(self.core.info.clone(), writer, concurrent);

        Ok((RpWrite::default(), w))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(UpyunDeleter::new(self.core.clone())),
        ))
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
            _ => Err(parse_error(resp)),
        }
    }

    async fn rename(&self, from: &str, to: &str, _args: OpRename) -> Result<RpRename> {
        let resp = self.core.move_object(from, to).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(RpRename::default()),
            _ => Err(parse_error(resp)),
        }
    }
}
