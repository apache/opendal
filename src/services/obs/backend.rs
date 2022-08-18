// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::io::Result;
use std::{fmt::Debug, sync::Arc};

use anyhow::anyhow;
use async_trait::async_trait;
use http::{StatusCode, Uri};
use log::debug;
use log::info;
use reqsign::services::huaweicloud::obs::Signer;

use super::error::parse_error;
use crate::error::{other, BackendError};
use crate::http_util::{
    new_request_build_error, new_request_send_error, new_request_sign_error, parse_error_response,
    percent_encode_path,
};
use crate::ops::BytesRange;
use crate::Scheme;
use crate::{
    http_util::HttpClient,
    ops::{OpCreate, OpDelete, OpList, OpRead, OpStat, OpWrite},
    Accessor, AccessorMetadata, BytesReader, BytesWriter, DirStreamer, ObjectMetadata,
};

/// Builder for Huaweicloud OBS services
#[derive(Default, Clone)]
pub struct Builder {
    root: Option<String>,
    endpoint: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    bucket: Option<String>,
}

impl Debug for Builder {
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

impl Builder {
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

    /// Consume builder to build an OBS backend.
    pub fn build(&mut self) -> Result<Backend> {
        info!("backend build started: {:?}", &self);

        let root = match &self.root {
            // Use "/" as root if user not specified.
            None => "/".to_string(),
            Some(v) => {
                let mut v = v
                    .split('/')
                    .filter(|v| !v.is_empty())
                    .collect::<Vec<&str>>()
                    .join("/");
                if !v.starts_with('/') {
                    v.insert(0, '/');
                }
                if !v.ends_with('/') {
                    v.push('/')
                }
                v
            }
        };

        info!("backend use root {}", root);

        let bucket = match &self.bucket {
            Some(bucket) => Ok(bucket.to_string()),
            None => Err(other(BackendError::new(
                HashMap::from([("bucket".to_string(), "".to_string())]),
                anyhow!("bucket is empty"),
            ))),
        }?;
        debug!("backend use bucket {}", &bucket);

        let uri = match &self.endpoint {
            Some(endpoint) => endpoint.parse::<Uri>().map_err(|_| {
                other(BackendError::new(
                    HashMap::from([("endpoint".to_string(), "".to_string())]),
                    anyhow!("endpoint is invalid"),
                ))
            }),
            None => Err(other(BackendError::new(
                HashMap::from([("endpoint".to_string(), "".to_string())]),
                anyhow!("endpoint is empty"),
            ))),
        }?;

        let scheme = match uri.scheme_str() {
            Some(scheme) => scheme.to_string(),
            None => "https".to_string(),
        };

        let (endpoint, is_obs_default) = {
            let host = uri.host().unwrap_or_default().to_string();
            if host.starts_with("obs.") && host.ends_with(".myhuaweicloud.com") {
                (format!("{}.{}", bucket, host), true)
            } else {
                (host, false)
            }
        };

        debug!("backend use endpoint {}", &endpoint);

        let context = HashMap::from([
            ("bucket".to_string(), bucket.to_string()),
            ("endpoint".to_string(), endpoint.to_string()),
        ]);

        let client = HttpClient::new();

        let mut signer_builder = Signer::builder();
        if let (Some(access_key_id), Some(secret_access_key)) =
            (&self.access_key_id, &self.secret_access_key)
        {
            signer_builder
                .access_key(access_key_id)
                .secret_key(secret_access_key);
        }

        // Set the bucket name in CanonicalizedResource.
        // 1. If the bucket is bound to a user domain name, use the user domain name as the bucket name,
        // for example, `/obs.ccc.com/object`. `obs.ccc.com` is the user domain name bound to the bucket.
        // 2. If you do not access OBS using a user domain name, this field is in the format of `/bucket/object`.
        //
        // Please refer to this doc for more details:
        // https://support.huaweicloud.com/intl/en-us/api-obs/obs_04_0010.html
        if is_obs_default {
            signer_builder.bucket(&bucket);
        } else {
            signer_builder.bucket(&endpoint);
        }

        let signer = signer_builder
            .build()
            .map_err(|e| other(BackendError::new(context, e)))?;

        info!("backend build finished: {:?}", &self);
        Ok(Backend {
            client,
            root,
            endpoint: format!("{}://{}", &scheme, &endpoint),
            signer: Arc::new(signer),
            bucket,
        })
    }
}

/// Backend for Huaweicloud OBS services.
#[derive(Debug, Clone)]
pub struct Backend {
    client: HttpClient,
    root: String,
    endpoint: String,
    signer: Arc<Signer>,
    bucket: String,
}

impl Backend {
    pub(crate) fn from_iter(it: impl Iterator<Item = (String, String)>) -> Result<Self> {
        let mut builder = Builder::default();

        for (k, v) in it {
            let v = v.as_str();
            match k.as_ref() {
                "root" => builder.root(v),
                "bucket" => builder.bucket(v),
                "endpoint" => builder.endpoint(v),
                "access_key_id" => builder.access_key_id(v),
                "secret_access_key" => builder.secret_access_key(v),
                _ => continue,
            };
        }

        builder.build()
    }

    pub(crate) fn get_abs_path(&self, path: &str) -> String {
        if path == "/" {
            return self.root.trim_start_matches('/').to_string();
        }
        // root must be normalized like `/abc/`
        format!("{}{}", self.root, path)
            .trim_start_matches('/')
            .to_string()
    }
}

#[async_trait]
impl Accessor for Backend {
    fn metadata(&self) -> AccessorMetadata {
        let mut am = AccessorMetadata::default();
        am.set_scheme(Scheme::Obs)
            .set_root(&self.root)
            .set_name(&self.bucket);

        am
    }

    async fn create(&self, args: &OpCreate) -> Result<()> {
        todo!()
    }

    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        let p = self.get_abs_path(args.path());

        let resp = self.get_object(&p, args.offset(), args.size()).await?;
        match resp.status() {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => Ok(Box::new(resp.into_body())),
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error("read", args.path(), er);
                Err(err)
            }
        }
    }

    async fn write(&self, args: &OpWrite) -> Result<BytesWriter> {
        todo!()
    }

    async fn stat(&self, args: &OpStat) -> Result<ObjectMetadata> {
        todo!()
    }

    async fn delete(&self, args: &OpDelete) -> Result<()> {
        todo!()
    }

    async fn list(&self, args: &OpList) -> Result<DirStreamer> {
        todo!()
    }
}

impl Backend {
    pub(crate) async fn get_object(
        &self,
        path: &str,
        offset: Option<u64>,
        size: Option<u64>,
    ) -> Result<isahc::Response<isahc::AsyncBody>> {
        let url = format!("{}/{}", self.endpoint, percent_encode_path(path));

        let mut req = isahc::Request::get(&url);

        if offset.is_some() || size.is_some() {
            req = req.header(
                http::header::RANGE,
                BytesRange::new(offset, size).to_string(),
            )
        }

        let mut req = req
            .body(isahc::AsyncBody::empty())
            .map_err(|e| new_request_build_error("read", path, e))?;

        self.signer
            .sign(&mut req)
            .map_err(|e| new_request_sign_error("read", path, e))?;

        self.client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error("read", path, e))
    }
}
