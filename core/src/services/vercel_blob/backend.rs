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

use bytes::Buf;
use http::Response;
use http::StatusCode;
use log::debug;

use super::core::parse_blob;
use super::core::Blob;
use super::core::VercelBlobCore;
use super::error::parse_error;
use super::lister::VercelBlobLister;
use super::writer::VercelBlobWriter;
use super::writer::VercelBlobWriters;
use crate::raw::*;
use crate::services::VercelBlobConfig;
use crate::*;

impl Configurator for VercelBlobConfig {
    type Builder = VercelBlobBuilder;

    #[allow(deprecated)]
    fn into_builder(self) -> Self::Builder {
        VercelBlobBuilder {
            config: self,
            http_client: None,
        }
    }
}

/// [VercelBlob](https://vercel.com/docs/storage/vercel-blob) services support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct VercelBlobBuilder {
    config: VercelBlobConfig,

    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
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
    pub fn root(mut self, root: &str) -> Self {
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
    pub fn token(mut self, token: &str) -> Self {
        if !token.is_empty() {
            self.config.token = Some(token.to_string());
        }
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

impl Builder for VercelBlobBuilder {
    const SCHEME: Scheme = Scheme::VercelBlob;
    type Config = VercelBlobConfig;

    /// Builds the backend and returns the result of VercelBlobBackend.
    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.config.root.clone().unwrap_or_default());
        debug!("backend use root {}", &root);

        // Handle token.
        let Some(token) = self.config.token.clone() else {
            return Err(Error::new(ErrorKind::ConfigInvalid, "token is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::VercelBlob));
        };

        Ok(VercelBlobBackend {
            core: Arc::new(VercelBlobCore {
                info: {
                    let am = AccessorInfo::default();
                    am.set_scheme(Scheme::VercelBlob)
                        .set_root(&root)
                        .set_native_capability(Capability {
                            stat: true,
                            stat_has_content_type: true,
                            stat_has_content_length: true,
                            stat_has_last_modified: true,
                            stat_has_content_disposition: true,

                            read: true,

                            write: true,
                            write_can_empty: true,
                            write_can_multi: true,
                            write_multi_min_size: Some(5 * 1024 * 1024),

                            copy: true,

                            list: true,
                            list_with_limit: true,
                            list_has_content_type: true,
                            list_has_content_length: true,
                            list_has_last_modified: true,
                            list_has_content_disposition: true,

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
                token,
            }),
        })
    }
}

/// Backend for VercelBlob services.
#[derive(Debug, Clone)]
pub struct VercelBlobBackend {
    core: Arc<VercelBlobCore>,
}

impl Access for VercelBlobBackend {
    type Reader = HttpBody;
    type Writer = VercelBlobWriters;
    type Lister = oio::PageLister<VercelBlobLister>;
    type Deleter = ();
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        let resp = self.core.head(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let bs = resp.into_body();

                let resp: Blob =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

                parse_blob(&resp).map(RpStat::new)
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.core.download(path, args.range(), &args).await?;

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
        let writer = VercelBlobWriter::new(self.core.clone(), args, path.to_string());

        let w = oio::MultipartWriter::new(self.core.info.clone(), writer, concurrent);

        Ok((RpWrite::default(), w))
    }

    async fn copy(&self, from: &str, to: &str, _args: OpCopy) -> Result<RpCopy> {
        let resp = self.core.copy(from, to).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(RpCopy::default()),
            _ => Err(parse_error(resp)),
        }
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = VercelBlobLister::new(self.core.clone(), path, args.limit());
        Ok((RpList::default(), oio::PageLister::new(l)))
    }
}
