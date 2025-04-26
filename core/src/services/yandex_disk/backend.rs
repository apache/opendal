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
use http::header;
use http::Request;
use http::Response;
use http::StatusCode;
use log::debug;

use super::core::*;
use super::delete::YandexDiskDeleter;
use super::error::parse_error;
use super::lister::YandexDiskLister;
use super::writer::YandexDiskWriter;
use super::writer::YandexDiskWriters;
use crate::raw::*;
use crate::services::YandexDiskConfig;
use crate::*;

impl Configurator for YandexDiskConfig {
    type Builder = YandexDiskBuilder;

    #[allow(deprecated)]
    fn into_builder(self) -> Self::Builder {
        YandexDiskBuilder {
            config: self,
            http_client: None,
        }
    }
}

/// [YandexDisk](https://360.yandex.com/disk/) services support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct YandexDiskBuilder {
    config: YandexDiskConfig,

    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
    http_client: Option<HttpClient>,
}

impl Debug for YandexDiskBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("YandexDiskBuilder");

        d.field("config", &self.config);
        d.finish_non_exhaustive()
    }
}

impl YandexDiskBuilder {
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

    /// yandex disk oauth access_token.
    /// The valid token will looks like `y0_XXXXXXqihqIWAADLWwAAAAD3IXXXXXX0gtVeSPeIKM0oITMGhXXXXXX`.
    /// We can fetch the debug token from <https://yandex.com/dev/disk/poligon>.
    /// To use it in production, please register an app at <https://oauth.yandex.com> instead.
    pub fn access_token(mut self, access_token: &str) -> Self {
        self.config.access_token = access_token.to_string();

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

impl Builder for YandexDiskBuilder {
    const SCHEME: Scheme = Scheme::YandexDisk;
    type Config = YandexDiskConfig;

    /// Builds the backend and returns the result of YandexDiskBackend.
    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.config.root.clone().unwrap_or_default());
        debug!("backend use root {}", &root);

        // Handle oauth access_token.
        if self.config.access_token.is_empty() {
            return Err(
                Error::new(ErrorKind::ConfigInvalid, "access_token is empty")
                    .with_operation("Builder::build")
                    .with_context("service", Scheme::YandexDisk),
            );
        }

        Ok(YandexDiskBackend {
            core: Arc::new(YandexDiskCore {
                info: {
                    let am = AccessorInfo::default();
                    am.set_scheme(Scheme::YandexDisk)
                        .set_root(&root)
                        .set_native_capability(Capability {
                            stat: true,
                            stat_has_last_modified: true,
                            stat_has_content_md5: true,
                            stat_has_content_type: true,
                            stat_has_content_length: true,

                            create_dir: true,

                            read: true,

                            write: true,
                            write_can_empty: true,

                            delete: true,
                            rename: true,
                            copy: true,

                            list: true,
                            list_with_limit: true,
                            list_has_last_modified: true,
                            list_has_content_md5: true,
                            list_has_content_type: true,
                            list_has_content_length: true,

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
                access_token: self.config.access_token.clone(),
            }),
        })
    }
}

/// Backend for YandexDisk services.
#[derive(Debug, Clone)]
pub struct YandexDiskBackend {
    core: Arc<YandexDiskCore>,
}

impl Access for YandexDiskBackend {
    type Reader = HttpBody;
    type Writer = YandexDiskWriters;
    type Lister = oio::PageLister<YandexDiskLister>;
    type Deleter = oio::OneShotDeleter<YandexDiskDeleter>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        self.core.ensure_dir_exists(path).await?;

        Ok(RpCreateDir::default())
    }

    async fn rename(&self, from: &str, to: &str, _args: OpRename) -> Result<RpRename> {
        self.core.ensure_dir_exists(to).await?;

        let resp = self.core.move_object(from, to).await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::CREATED => Ok(RpRename::default()),
            _ => Err(parse_error(resp)),
        }
    }

    async fn copy(&self, from: &str, to: &str, _args: OpCopy) -> Result<RpCopy> {
        self.core.ensure_dir_exists(to).await?;

        let resp = self.core.copy(from, to).await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::CREATED => Ok(RpCopy::default()),
            _ => Err(parse_error(resp)),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        // TODO: move this out of reader.
        let download_url = self.core.get_download_url(path).await?;

        let req = Request::get(download_url)
            .header(header::RANGE, args.range().to_header())
            .body(Buffer::new())
            .map_err(new_request_build_error)?;
        let resp = self.core.info.http_client().fetch(req).await?;

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

    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        let resp = self.core.metainformation(path, None, None).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let bs = resp.into_body();

                let mf: MetainformationResponse =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

                parse_info(mf).map(RpStat::new)
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn write(&self, path: &str, _args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let writer = YandexDiskWriter::new(self.core.clone(), path.to_string());

        let w = oio::OneShotWriter::new(writer);

        Ok((RpWrite::default(), w))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(YandexDiskDeleter::new(self.core.clone())),
        ))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = YandexDiskLister::new(self.core.clone(), path, args.limit());
        Ok((RpList::default(), oio::PageLister::new(l)))
    }
}
