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
use super::error::parse_error;
use super::writer::*;
use crate::raw::*;
use crate::services::SupabaseConfig;
use crate::*;

impl Configurator for SupabaseConfig {
    type Builder = SupabaseBuilder;
    fn into_builder(self) -> Self::Builder {
        SupabaseBuilder {
            config: self,
            http_client: None,
        }
    }
}

/// [Supabase](https://supabase.com/) service support
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct SupabaseBuilder {
    config: SupabaseConfig,
    http_client: Option<HttpClient>,
}

impl Debug for SupabaseBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("SupabaseBuilder");
        d.field("config", &self.config);
        d.finish_non_exhaustive()
    }
}

impl SupabaseBuilder {
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

    /// Set bucket name of this backend.
    pub fn bucket(mut self, bucket: &str) -> Self {
        self.config.bucket = bucket.to_string();
        self
    }

    /// Set endpoint of this backend.
    ///
    /// Endpoint must be full uri
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        self.config.endpoint = if endpoint.is_empty() {
            None
        } else {
            Some(endpoint.trim_end_matches('/').to_string())
        };

        self
    }

    /// Set the authorization key for this backend
    /// Do not set this key if you want to read public bucket
    pub fn key(mut self, key: &str) -> Self {
        self.config.key = Some(key.to_string());
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

impl Builder for SupabaseBuilder {
    const SCHEME: Scheme = Scheme::Supabase;
    type Config = SupabaseConfig;

    fn build(self) -> Result<impl Access> {
        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root {}", &root);

        let bucket = &self.config.bucket;

        let endpoint = self.config.endpoint.unwrap_or_default();

        let http_client = if let Some(client) = self.http_client {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Supabase)
            })?
        };

        let key = self.config.key.as_ref().map(|k| k.to_owned());

        let core = SupabaseCore::new(&root, bucket, &endpoint, key, http_client);

        let core = Arc::new(core);

        Ok(SupabaseBackend { core })
    }
}

#[derive(Debug)]
pub struct SupabaseBackend {
    core: Arc<SupabaseCore>,
}

impl Access for SupabaseBackend {
    type Reader = HttpBody;
    type Writer = oio::OneShotWriter<SupabaseWriter>;
    // todo: implement Lister to support list
    type Lister = ();
    type Deleter = ();
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Supabase)
            .set_root(&self.core.root)
            .set_name(&self.core.bucket)
            .set_native_capability(Capability {
                stat: true,

                read: true,

                write: true,

                shared: true,

                ..Default::default()
            });

        am.into()
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        // The get_object_info does not contain the file size. Therefore
        // we first try the get the metadata through head, if we fail,
        // we then use get_object_info to get the actual error info
        let mut resp = self.core.supabase_head_object(path).await?;

        match resp.status() {
            StatusCode::OK => parse_into_metadata(path, resp.headers()).map(RpStat::new),
            _ => {
                resp = self.core.supabase_get_object_info(path).await?;
                match resp.status() {
                    StatusCode::NOT_FOUND if path.ends_with('/') => {
                        Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
                    }
                    _ => Err(parse_error(resp)),
                }
            }
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.core.supabase_get_object(path, args.range()).await?;

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

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        Ok((
            RpWrite::default(),
            oio::OneShotWriter::new(SupabaseWriter::new(self.core.clone(), path, args)),
        ))
    }
}
