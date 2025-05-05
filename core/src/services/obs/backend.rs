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

use http::Response;
use http::StatusCode;
use http::Uri;
use log::debug;
use reqsign::HuaweicloudObsConfig;
use reqsign::HuaweicloudObsCredentialLoader;
use reqsign::HuaweicloudObsSigner;

use super::core::{constants, ObsCore};
use super::delete::ObsDeleter;
use super::error::parse_error;
use super::lister::ObsLister;
use super::writer::ObsWriter;
use super::writer::ObsWriters;
use crate::raw::*;
use crate::services::ObsConfig;
use crate::*;

impl Configurator for ObsConfig {
    type Builder = ObsBuilder;

    #[allow(deprecated)]
    fn into_builder(self) -> Self::Builder {
        ObsBuilder {
            config: self,

            http_client: None,
        }
    }
}

/// Huawei-Cloud Object Storage Service (OBS) support
#[doc = include_str!("docs.md")]
#[derive(Default, Clone)]
pub struct ObsBuilder {
    config: ObsConfig,

    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
    http_client: Option<HttpClient>,
}

impl Debug for ObsBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("ObsBuilder");
        d.field("config", &self.config);
        d.finish_non_exhaustive()
    }
}

impl ObsBuilder {
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
    /// Both huaweicloud default domain and user domain endpoints are allowed.
    /// Please DO NOT add the bucket name to the endpoint.
    ///
    /// - `https://obs.cn-north-4.myhuaweicloud.com`
    /// - `obs.cn-north-4.myhuaweicloud.com` (https by default)
    /// - `https://custom.obs.com` (port should not be set)
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        if !endpoint.is_empty() {
            self.config.endpoint = Some(endpoint.trim_end_matches('/').to_string());
        }

        self
    }

    /// Set access_key_id of this backend.
    /// - If it is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn access_key_id(mut self, access_key_id: &str) -> Self {
        if !access_key_id.is_empty() {
            self.config.access_key_id = Some(access_key_id.to_string());
        }

        self
    }

    /// Set secret_access_key of this backend.
    /// - If it is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn secret_access_key(mut self, secret_access_key: &str) -> Self {
        if !secret_access_key.is_empty() {
            self.config.secret_access_key = Some(secret_access_key.to_string());
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

    /// Set bucket versioning status for this backend
    pub fn enable_versioning(mut self, enabled: bool) -> Self {
        self.config.enable_versioning = enabled;

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

impl Builder for ObsBuilder {
    const SCHEME: Scheme = Scheme::Obs;
    type Config = ObsConfig;

    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root {}", root);

        let bucket = match &self.config.bucket {
            Some(bucket) => Ok(bucket.to_string()),
            None => Err(
                Error::new(ErrorKind::ConfigInvalid, "The bucket is misconfigured")
                    .with_context("service", Scheme::Obs),
            ),
        }?;
        debug!("backend use bucket {}", &bucket);

        let uri = match &self.config.endpoint {
            Some(endpoint) => endpoint.parse::<Uri>().map_err(|err| {
                Error::new(ErrorKind::ConfigInvalid, "endpoint is invalid")
                    .with_context("service", Scheme::Obs)
                    .set_source(err)
            }),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                .with_context("service", Scheme::Obs)),
        }?;

        let scheme = match uri.scheme_str() {
            Some(scheme) => scheme.to_string(),
            None => "https".to_string(),
        };

        let (endpoint, is_obs_default) = {
            let host = uri.host().unwrap_or_default().to_string();
            if host.starts_with("obs.")
                && (host.ends_with(".myhuaweicloud.com") || host.ends_with(".huawei.com"))
            {
                (format!("{bucket}.{host}"), true)
            } else {
                (host, false)
            }
        };
        debug!("backend use endpoint {}", &endpoint);

        let mut cfg = HuaweicloudObsConfig::default();
        // Load cfg from env first.
        cfg = cfg.from_env();

        if let Some(v) = self.config.access_key_id {
            cfg.access_key_id = Some(v);
        }

        if let Some(v) = self.config.secret_access_key {
            cfg.secret_access_key = Some(v);
        }

        let loader = HuaweicloudObsCredentialLoader::new(cfg);

        // Set the bucket name in CanonicalizedResource.
        // 1. If the bucket is bound to a user domain name, use the user domain name as the bucket name,
        // for example, `/obs.ccc.com/object`. `obs.ccc.com` is the user domain name bound to the bucket.
        // 2. If you do not access OBS using a user domain name, this field is in the format of `/bucket/object`.
        //
        // Please refer to this doc for more details:
        // https://support.huaweicloud.com/intl/en-us/api-obs/obs_04_0010.html
        let signer = HuaweicloudObsSigner::new({
            if is_obs_default {
                &bucket
            } else {
                &endpoint
            }
        });

        debug!("backend build finished");
        Ok(ObsBackend {
            core: Arc::new(ObsCore {
                info: {
                    let am = AccessorInfo::default();
                    am.set_scheme(Scheme::Obs)
                        .set_root(&root)
                        .set_name(&bucket)
                        .set_native_capability(Capability {
                            stat: true,
                            stat_with_if_match: true,
                            stat_with_if_none_match: true,
                            stat_has_cache_control: true,
                            stat_has_content_length: true,
                            stat_has_content_type: true,
                            stat_has_content_encoding: true,
                            stat_has_content_range: true,
                            stat_has_etag: true,
                            stat_has_content_md5: true,
                            stat_has_last_modified: true,
                            stat_has_content_disposition: true,
                            stat_has_user_metadata: true,

                            read: true,

                            read_with_if_match: true,
                            read_with_if_none_match: true,

                            write: true,
                            write_can_empty: true,
                            write_can_append: true,
                            write_can_multi: true,
                            write_with_content_type: true,
                            write_with_cache_control: true,
                            // The min multipart size of OBS is 5 MiB.
                            //
                            // ref: <https://support.huaweicloud.com/intl/en-us/ugobs-obs/obs_41_0021.html>
                            write_multi_min_size: Some(5 * 1024 * 1024),
                            // The max multipart size of OBS is 5 GiB.
                            //
                            // ref: <https://support.huaweicloud.com/intl/en-us/ugobs-obs/obs_41_0021.html>
                            write_multi_max_size: if cfg!(target_pointer_width = "64") {
                                Some(5 * 1024 * 1024 * 1024)
                            } else {
                                Some(usize::MAX)
                            },
                            write_with_user_metadata: true,

                            delete: true,
                            copy: true,

                            list: true,
                            list_with_recursive: true,
                            list_has_content_length: true,

                            presign: true,
                            presign_stat: true,
                            presign_read: true,
                            presign_write: true,

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
                bucket,
                root,
                endpoint: format!("{}://{}", &scheme, &endpoint),
                signer,
                loader,
            }),
        })
    }
}

/// Backend for Huaweicloud OBS services.
#[derive(Debug, Clone)]
pub struct ObsBackend {
    core: Arc<ObsCore>,
}

impl Access for ObsBackend {
    type Reader = HttpBody;
    type Writer = ObsWriters;
    type Lister = oio::PageLister<ObsLister>;
    type Deleter = oio::OneShotDeleter<ObsDeleter>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let resp = self.core.obs_head_object(path, &args).await?;
        let headers = resp.headers();

        let status = resp.status();

        // The response is very similar to azblob.
        match status {
            StatusCode::OK => {
                let mut meta = parse_into_metadata(path, headers)?;
                let user_meta = headers
                    .iter()
                    .filter_map(|(name, _)| {
                        name.as_str()
                            .strip_prefix(constants::X_OBS_META_PREFIX)
                            .and_then(|stripped_key| {
                                parse_header_to_str(headers, name)
                                    .unwrap_or(None)
                                    .map(|val| (stripped_key.to_string(), val.to_string()))
                            })
                    })
                    .collect::<HashMap<_, _>>();

                if !user_meta.is_empty() {
                    meta.with_user_metadata(user_meta);
                }

                if let Some(v) = parse_header_to_str(headers, constants::X_OBS_VERSION_ID)? {
                    meta.set_version(v);
                }

                Ok(RpStat::new(meta))
            }
            StatusCode::NOT_FOUND if path.ends_with('/') => {
                Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.core.obs_get_object(path, args.range(), &args).await?;

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
        let writer = ObsWriter::new(self.core.clone(), path, args.clone());

        let w = if args.append() {
            ObsWriters::Two(oio::AppendWriter::new(writer))
        } else {
            ObsWriters::One(oio::MultipartWriter::new(
                self.core.info.clone(),
                writer,
                args.concurrent(),
            ))
        };

        Ok((RpWrite::default(), w))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(ObsDeleter::new(self.core.clone())),
        ))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = ObsLister::new(self.core.clone(), path, args.recursive(), args.limit());
        Ok((RpList::default(), oio::PageLister::new(l)))
    }

    async fn copy(&self, from: &str, to: &str, _args: OpCopy) -> Result<RpCopy> {
        let resp = self.core.obs_copy_object(from, to).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(RpCopy::default()),
            _ => Err(parse_error(resp)),
        }
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        let req = match args.operation() {
            PresignOperation::Stat(v) => self.core.obs_head_object_request(path, v),
            PresignOperation::Read(v) => {
                self.core
                    .obs_get_object_request(path, BytesRange::default(), v)
            }
            PresignOperation::Write(v) => {
                self.core
                    .obs_put_object_request(path, None, v, Buffer::new())
            }
            PresignOperation::Delete(_) => Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            )),
        };
        let mut req = req?;
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
