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
use std::sync::Arc;

use http::Response;
use http::StatusCode;
use http::Uri;
use log::debug;
use reqsign::TencentCosConfig;
use reqsign::TencentCosCredentialLoader;
use reqsign::TencentCosSigner;

use super::core::*;
use super::error::parse_error;
use super::lister::CosLister;
use super::writer::CosWriter;
use super::writer::CosWriters;
use crate::raw::*;
use crate::services::CosConfig;
use crate::*;

impl Configurator for CosConfig {
    type Builder = CosBuilder;
    fn into_builder(self) -> Self::Builder {
        CosBuilder {
            config: self,
            http_client: None,
        }
    }
}

/// Tencent-Cloud COS services support.
#[doc = include_str!("docs.md")]
#[derive(Default, Clone)]
pub struct CosBuilder {
    config: CosConfig,
    http_client: Option<HttpClient>,
}

impl Debug for CosBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CosBuilder")
            .field("config", &self.config)
            .finish()
    }
}

impl CosBuilder {
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

    /// Set endpoint of this backend.
    ///
    /// NOTE: no bucket or account id in endpoint, we will trim them if exists.
    ///
    /// # Examples
    ///
    /// - `https://cos.ap-singapore.myqcloud.com`
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        if !endpoint.is_empty() {
            self.config.endpoint = Some(endpoint.trim_end_matches('/').to_string());
        }

        self
    }

    /// Set secret_id of this backend.
    /// - If it is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn secret_id(mut self, secret_id: &str) -> Self {
        if !secret_id.is_empty() {
            self.config.secret_id = Some(secret_id.to_string());
        }

        self
    }

    /// Set secret_key of this backend.
    /// - If it is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn secret_key(mut self, secret_key: &str) -> Self {
        if !secret_key.is_empty() {
            self.config.secret_key = Some(secret_key.to_string());
        }

        self
    }

    /// Set bucket of this backend.
    /// The param is required.
    pub fn bucket(mut self, bucket: &str) -> Self {
        if !bucket.is_empty() {
            self.config.bucket = Some(bucket.to_string());
        }

        self
    }

    /// Disable config load so that opendal will not load config from
    /// environment.
    ///
    /// For examples:
    ///
    /// - envs like `TENCENTCLOUD_SECRET_ID`
    pub fn disable_config_load(mut self) -> Self {
        self.config.disable_config_load = true;
        self
    }

    /// Specify the http client that used by this service.
    ///
    /// # Notes
    ///
    /// This API is part of OpenDAL's Raw API. `HttpClient` could be changed
    /// during minor updates.
    pub fn http_client(mut self, client: HttpClient) -> Self {
        self.http_client = Some(client);
        self
    }
}

impl Builder for CosBuilder {
    const SCHEME: Scheme = Scheme::Cos;
    type Config = CosConfig;

    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root {}", root);

        let bucket = match &self.config.bucket {
            Some(bucket) => Ok(bucket.to_string()),
            None => Err(
                Error::new(ErrorKind::ConfigInvalid, "The bucket is misconfigured")
                    .with_context("service", Scheme::Cos),
            ),
        }?;
        debug!("backend use bucket {}", &bucket);

        let uri = match &self.config.endpoint {
            Some(endpoint) => endpoint.parse::<Uri>().map_err(|err| {
                Error::new(ErrorKind::ConfigInvalid, "endpoint is invalid")
                    .with_context("service", Scheme::Cos)
                    .with_context("endpoint", endpoint)
                    .set_source(err)
            }),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                .with_context("service", Scheme::Cos)),
        }?;

        let scheme = match uri.scheme_str() {
            Some(scheme) => scheme.to_string(),
            None => "https".to_string(),
        };

        // If endpoint contains bucket name, we should trim them.
        let endpoint = uri.host().unwrap().replace(&format!("//{bucket}."), "//");
        debug!("backend use endpoint {}", &endpoint);

        let client = if let Some(client) = self.http_client {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Cos)
            })?
        };

        let mut cfg = TencentCosConfig::default();
        if !self.config.disable_config_load {
            cfg = cfg.from_env();
        }

        if let Some(v) = self.config.secret_id {
            cfg.secret_id = Some(v);
        }
        if let Some(v) = self.config.secret_key {
            cfg.secret_key = Some(v);
        }

        let cred_loader = TencentCosCredentialLoader::new(GLOBAL_REQWEST_CLIENT.clone(), cfg);

        let signer = TencentCosSigner::new();

        Ok(CosBackend {
            core: Arc::new(CosCore {
                bucket: bucket.clone(),
                root,
                endpoint: format!("{}://{}.{}", &scheme, &bucket, &endpoint),
                signer,
                loader: cred_loader,
                client,
            }),
        })
    }
}

/// Backend for Tencent-Cloud COS services.
#[derive(Debug, Clone)]
pub struct CosBackend {
    core: Arc<CosCore>,
}

impl Access for CosBackend {
    type Reader = HttpBody;
    type Writer = CosWriters;
    type Lister = oio::PageLister<CosLister>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();

    fn info(&self) -> Arc<AccessorInfo> {
        AccessorInfo::default()
            .set_scheme(Scheme::Cos)
            .set_root(&self.core.root)
            .set_name(&self.core.bucket)
            .set_native_capability(Capability {
                stat: true,
                stat_with_if_match: true,
                stat_with_if_none_match: true,

                read: true,

                read_with_if_match: true,
                read_with_if_none_match: true,

                write: true,
                write_can_empty: true,
                write_can_append: true,
                write_can_multi: true,
                write_with_content_type: true,
                write_with_cache_control: true,
                write_with_content_disposition: true,
                // The min multipart size of COS is 1 MiB.
                //
                // ref: <https://www.tencentcloud.com/document/product/436/14112>
                write_multi_min_size: Some(1024 * 1024),
                // The max multipart size of COS is 5 GiB.
                //
                // ref: <https://www.tencentcloud.com/document/product/436/14112>
                write_multi_max_size: if cfg!(target_pointer_width = "64") {
                    Some(5 * 1024 * 1024 * 1024)
                } else {
                    Some(usize::MAX)
                },

                delete: true,
                copy: true,

                list: true,
                list_with_recursive: true,

                presign: true,
                presign_stat: true,
                presign_read: true,
                presign_write: true,

                ..Default::default()
            })
            .into()
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let resp = self.core.cos_head_object(path, &args).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => parse_into_metadata(path, resp.headers()).map(RpStat::new),
            _ => Err(parse_error(resp)),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.core.cos_get_object(path, args.range(), &args).await?;

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
        let writer = CosWriter::new(self.core.clone(), path, args.clone());

        let w = if args.append() {
            CosWriters::Two(oio::AppendWriter::new(writer))
        } else {
            CosWriters::One(oio::MultipartWriter::new(
                writer,
                args.executor().cloned(),
                args.concurrent(),
            ))
        };

        Ok((RpWrite::default(), w))
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let resp = self.core.cos_delete_object(path).await?;

        let status = resp.status();

        match status {
            StatusCode::NO_CONTENT | StatusCode::ACCEPTED | StatusCode::NOT_FOUND => {
                Ok(RpDelete::default())
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = CosLister::new(self.core.clone(), path, args.recursive(), args.limit());
        Ok((RpList::default(), oio::PageLister::new(l)))
    }

    async fn copy(&self, from: &str, to: &str, _args: OpCopy) -> Result<RpCopy> {
        let resp = self.core.cos_copy_object(from, to).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(RpCopy::default()),
            _ => Err(parse_error(resp)),
        }
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        let mut req = match args.operation() {
            PresignOperation::Stat(v) => self.core.cos_head_object_request(path, v)?,
            PresignOperation::Read(v) => {
                self.core
                    .cos_get_object_request(path, BytesRange::default(), v)?
            }
            PresignOperation::Write(v) => {
                self.core
                    .cos_put_object_request(path, None, v, Buffer::new())?
            }
        };
        self.core.sign_query(&mut req, args.expire()).await?;

        // We don't need this request anymore, consume it directly.
        let (parts, _) = req.into_parts();

        Ok(RpPresign::new(PresignedRequest::new(
            parts.method,
            parts.uri,
            parts.headers,
        )))
    }
}
