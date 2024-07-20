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

use bytes::Buf;
use http::header;
use http::Request;
use http::Response;
use http::StatusCode;
use log::debug;
use serde::{Deserialize, Serialize};

use crate::raw::*;
use crate::*;

use super::core::*;
use super::error::parse_error;
use super::lister::YandexDiskLister;
use super::writer::YandexDiskWriter;
use super::writer::YandexDiskWriters;

/// Config for backblaze YandexDisk services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct YandexDiskConfig {
    /// root of this backend.
    ///
    /// All operations will happen under this root.
    pub root: Option<String>,
    /// yandex disk oauth access_token.
    pub access_token: String,
}

impl Debug for YandexDiskConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Config");

        ds.field("root", &self.root);

        ds.finish()
    }
}

/// [YandexDisk](https://360.yandex.com/disk/) services support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct YandexDiskBuilder {
    config: YandexDiskConfig,

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
    pub fn root(&mut self, root: &str) -> &mut Self {
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
    pub fn access_token(&mut self, access_token: &str) -> &mut Self {
        self.config.access_token = access_token.to_string();

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

impl Builder for YandexDiskBuilder {
    const SCHEME: Scheme = Scheme::YandexDisk;
    type Accessor = YandexDiskBackend;
    type Config = YandexDiskConfig;

    fn from_config(config: Self::Config) -> Self {
        YandexDiskBuilder {
            config,
            http_client: None,
        }
    }

    /// Builds the backend and returns the result of YandexDiskBackend.
    fn build(&mut self) -> Result<Self::Accessor> {
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

        let client = if let Some(client) = self.http_client.take() {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::YandexDisk)
            })?
        };

        Ok(YandexDiskBackend {
            core: Arc::new(YandexDiskCore {
                root,
                access_token: self.config.access_token.clone(),
                client,
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
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();

    fn info(&self) -> Arc<AccessorInfo> {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::YandexDisk)
            .set_root(&self.core.root)
            .set_native_capability(Capability {
                stat: true,

                create_dir: true,

                read: true,

                write: true,
                write_can_empty: true,

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
        self.core.ensure_dir_exists(path).await?;

        Ok(RpCreateDir::default())
    }

    async fn rename(&self, from: &str, to: &str, _args: OpRename) -> Result<RpRename> {
        self.core.ensure_dir_exists(to).await?;

        let resp = self.core.move_object(from, to).await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::CREATED => Ok(RpRename::default()),
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn copy(&self, from: &str, to: &str, _args: OpCopy) -> Result<RpCopy> {
        self.core.ensure_dir_exists(to).await?;

        let resp = self.core.copy(from, to).await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::CREATED => Ok(RpCopy::default()),
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        // TODO: move this out of reader.
        let download_url = self.core.get_download_url(path).await?;

        let req = Request::get(download_url)
            .header(header::RANGE, args.range().to_header())
            .body(Buffer::new())
            .map_err(new_request_build_error)?;
        let resp = self.core.client.fetch(req).await?;

        let status = resp.status();
        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => Ok((RpRead::new(), resp.into_body())),
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                Err(parse_error(Response::from_parts(part, buf)).await?)
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
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn write(&self, path: &str, _args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let writer = YandexDiskWriter::new(self.core.clone(), path.to_string());

        let w = oio::OneShotWriter::new(writer);

        Ok((RpWrite::default(), w))
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let resp = self.core.delete(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(RpDelete::default()),
            StatusCode::NO_CONTENT => Ok(RpDelete::default()),
            // Yandex Disk deleting a non-empty folder can take an unknown amount of time,
            // So the API responds with the code 202 Accepted (the deletion process has started).
            StatusCode::ACCEPTED => Ok(RpDelete::default()),
            // Allow 404 when deleting a non-existing object
            StatusCode::NOT_FOUND => Ok(RpDelete::default()),
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = YandexDiskLister::new(self.core.clone(), path, args.limit());
        Ok((RpList::default(), oio::PageLister::new(l)))
    }
}
