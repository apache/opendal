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
use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;
use http::Uri;
use log::debug;
use reqsign::HuaweicloudObsConfig;
use reqsign::HuaweicloudObsCredentialLoader;
use reqsign::HuaweicloudObsSigner;

use super::core::ObsCore;
use super::error::parse_error;
use super::pager::ObsPager;
use super::writer::ObsWriter;
use crate::raw::*;
use crate::*;

/// Huawei Cloud OBS services support.
///
/// # Capabilities
///
/// This service can be used to:
///
/// - [x] stat
/// - [x] read
/// - [x] write
/// - [x] create_dir
/// - [x] delete
/// - [x] copy
/// - [ ] rename
/// - [x] list
/// - [x] scan
/// - [x] presign
/// - [ ] blocking
///
/// # Configuration
///
/// - `root`: Set the work directory for backend
/// - `bucket`: Set the container name for backend
/// - `endpoint`: Customizable endpoint setting
/// - `access_key_id`: Set the access_key_id for backend.
/// - `secret_access_key`: Set the secret_access_key for backend.
///
/// You can refer to [`ObsBuilder`]'s docs for more information
///
/// # Example
///
/// ## Via Builder
///
/// ```no_run
/// use anyhow::Result;
/// use opendal::services::Obs;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     // create backend builder
///     let mut builder = Obs::default();
///
///     // set the storage bucket for OpenDAL
///     builder.bucket("test");
///     // Set the access_key_id and secret_access_key.
///     //
///     // OpenDAL will try load credential from the env.
///     // If credential not set and no valid credential in env, OpenDAL will
///     // send request without signing like anonymous user.
///     builder.access_key_id("access_key_id");
///     builder.secret_access_key("secret_access_key");
///
///     let op: Operator = Operator::new(builder)?.finish();
///
///     Ok(())
/// }
/// ```

const DEFAULT_WRITE_MIN_SIZE: usize = 100 * 1024;

/// Huawei-Cloud Object Storage Service (OBS) support
#[derive(Default, Clone)]
pub struct ObsBuilder {
    root: Option<String>,
    endpoint: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    bucket: Option<String>,
    http_client: Option<HttpClient>,
    /// the part size of obs multipart upload, which should be 100 KiB to 5 GiB.
    /// There is no minimum size limit on the last part of your multipart upload
    write_min_size: Option<usize>,
}

impl Debug for ObsBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Builder")
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .field("access_key_id", &"<redacted>")
            .field("secret_access_key", &"<redacted>")
            .field("bucket", &self.bucket)
            .finish()
    }
}

impl ObsBuilder {
    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(&mut self, root: &str) -> &mut Self {
        if !root.is_empty() {
            self.root = Some(root.to_string())
        }

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
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        if !endpoint.is_empty() {
            self.endpoint = Some(endpoint.trim_end_matches('/').to_string());
        }

        self
    }

    /// Set access_key_id of this backend.
    /// - If it is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn access_key_id(&mut self, access_key_id: &str) -> &mut Self {
        if !access_key_id.is_empty() {
            self.access_key_id = Some(access_key_id.to_string());
        }

        self
    }

    /// Set secret_access_key of this backend.
    /// - If it is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn secret_access_key(&mut self, secret_access_key: &str) -> &mut Self {
        if !secret_access_key.is_empty() {
            self.secret_access_key = Some(secret_access_key.to_string());
        }

        self
    }

    /// Set bucket of this backend.
    /// The param is required.
    pub fn bucket(&mut self, bucket: &str) -> &mut Self {
        if !bucket.is_empty() {
            self.bucket = Some(bucket.to_string());
        }

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

    /// set the minimum size of unsized write, it should be greater than 100 KB.
    /// Reference: [Huawei Obs multipart upload limits](https://support.huaweicloud.com/intl/en-us/api-obs/obs_04_0099.html)
    pub fn write_min_size(&mut self, write_min_size: usize) -> &mut Self {
        self.write_min_size = Some(write_min_size);

        self
    }
}

impl Builder for ObsBuilder {
    const SCHEME: Scheme = Scheme::Obs;
    type Accessor = ObsBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = ObsBuilder::default();

        map.get("root").map(|v| builder.root(v));
        map.get("bucket").map(|v| builder.bucket(v));
        map.get("endpoint").map(|v| builder.endpoint(v));
        map.get("access_key_id").map(|v| builder.access_key_id(v));
        map.get("secret_access_key")
            .map(|v| builder.secret_access_key(v));
        map.get("write_min_size")
            .map(|v| builder.write_min_size(v.parse().expect("input must be a number")));

        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.root.take().unwrap_or_default());
        debug!("backend use root {}", root);

        let bucket = match &self.bucket {
            Some(bucket) => Ok(bucket.to_string()),
            None => Err(
                Error::new(ErrorKind::ConfigInvalid, "The bucket is misconfigured")
                    .with_context("service", Scheme::Obs),
            ),
        }?;
        debug!("backend use bucket {}", &bucket);

        let uri = match &self.endpoint {
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
            if host.starts_with("obs.") && host.ends_with(".myhuaweicloud.com") {
                (format!("{bucket}.{host}"), true)
            } else {
                (host, false)
            }
        };
        debug!("backend use endpoint {}", &endpoint);

        let client = if let Some(client) = self.http_client.take() {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Obs)
            })?
        };

        let mut cfg = HuaweicloudObsConfig::default();
        // Load cfg from env first.
        cfg = cfg.from_env();

        if let Some(v) = self.access_key_id.take() {
            cfg.access_key_id = Some(v);
        }

        if let Some(v) = self.secret_access_key.take() {
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
        let write_min_size = self.write_min_size.unwrap_or(DEFAULT_WRITE_MIN_SIZE);
        if write_min_size < 100 * 1024 {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "The write minimum buffer size is misconfigured",
            )
            .with_context("service", Scheme::Obs));
        }

        debug!("backend build finished");
        Ok(ObsBackend {
            core: Arc::new(ObsCore {
                bucket,
                root,
                endpoint: format!("{}://{}", &scheme, &endpoint),
                signer,
                loader,
                client,
                write_min_size,
            }),
        })
    }
}

/// Backend for Huaweicloud OBS services.
#[derive(Debug, Clone)]
pub struct ObsBackend {
    core: Arc<ObsCore>,
}

#[async_trait]
impl Accessor for ObsBackend {
    type Reader = IncomingAsyncBody;
    type BlockingReader = ();
    type Writer = oio::TwoWaysWriter<
        oio::MultipartUploadWriter<ObsWriter>,
        oio::AppendObjectWriter<ObsWriter>,
    >;
    type BlockingWriter = ();
    type Pager = ObsPager;
    type BlockingPager = ();

    fn info(&self) -> AccessorInfo {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Obs)
            .set_root(&self.core.root)
            .set_name(&self.core.bucket)
            .set_full_capability(Capability {
                stat: true,
                stat_with_if_match: true,
                stat_with_if_none_match: true,

                read: true,
                read_can_next: true,
                read_with_range: true,
                read_with_if_match: true,
                read_with_if_none_match: true,

                write: true,
                write_can_append: true,
                write_can_sink: true,
                write_with_content_type: true,
                write_with_cache_control: true,
                write_without_content_length: true,

                delete: true,
                create_dir: true,
                copy: true,

                list: true,
                list_with_delimiter_slash: true,
                list_without_delimiter: true,

                presign: true,
                presign_stat: true,
                presign_read: true,
                presign_write: true,

                ..Default::default()
            });

        am
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        let mut req = match args.operation() {
            PresignOperation::Stat(v) => {
                self.core
                    .obs_head_object_request(path, v.if_match(), v.if_none_match())?
            }
            PresignOperation::Read(v) => self.core.obs_get_object_request(
                path,
                v.range(),
                v.if_match(),
                v.if_none_match(),
            )?,
            PresignOperation::Write(v) => self.core.obs_put_object_request(
                path,
                None,
                v.content_type(),
                v.cache_control(),
                AsyncBody::Empty,
            )?,
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

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        let mut req =
            self.core
                .obs_put_object_request(path, Some(0), None, None, AsyncBody::Empty)?;

        self.core.sign(&mut req).await?;

        let resp = self.core.send(req).await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => {
                resp.into_body().consume().await?;
                Ok(RpCreateDir::default())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self
            .core
            .obs_get_object(path, args.range(), args.if_match(), args.if_none_match())
            .await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                let meta = parse_into_metadata(path, resp.headers())?;
                Ok((RpRead::with_metadata(meta), resp.into_body()))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let writer = ObsWriter::new(self.core.clone(), path, args.clone());

        let tw = if args.append() {
            let w =
                oio::AppendObjectWriter::new(writer).with_write_min_size(self.core.write_min_size);

            oio::TwoWaysWriter::Right(w)
        } else {
            let w = oio::MultipartUploadWriter::new(writer);

            oio::TwoWaysWriter::Left(w)
        };

        return Ok((RpWrite::default(), tw));
    }

    async fn copy(&self, from: &str, to: &str, _args: OpCopy) -> Result<RpCopy> {
        let resp = self.core.obs_copy_object(from, to).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                resp.into_body().consume().await?;
                Ok(RpCopy::default())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        let resp = self
            .core
            .obs_head_object(path, args.if_match(), args.if_none_match())
            .await?;

        let status = resp.status();

        // The response is very similar to azblob.
        match status {
            StatusCode::OK => parse_into_metadata(path, resp.headers()).map(RpStat::new),
            StatusCode::NOT_FOUND if path.ends_with('/') => {
                Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let resp = self.core.obs_delete_object(path).await?;

        let status = resp.status();

        match status {
            StatusCode::NO_CONTENT | StatusCode::ACCEPTED | StatusCode::NOT_FOUND => {
                Ok(RpDelete::default())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        Ok((
            RpList::default(),
            ObsPager::new(self.core.clone(), path, args.delimiter(), args.limit()),
        ))
    }
}
