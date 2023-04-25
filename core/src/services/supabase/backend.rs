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

use async_trait::async_trait;
use http::HeaderValue;
use http::StatusCode;
use log::debug;

use super::core::*;
use super::error::parse_error;
use super::writer::*;
use crate::ops::*;
use crate::raw::*;
use crate::*;

/// Supabase service
///
/// # Capabilities
///
/// - [x] read
/// - [x] write
/// - [ ] copy
/// - [ ] list
/// - [ ] scan
/// - [ ] presign
/// - [ ] blocking
///
/// # Configuration
///
/// - `root`: Set the work dir for backend.
/// - `bucket`: Set the container name for backend.
/// - `endpoint`: Set the endpoint for backend.
/// - `service_key`: Set the secret key for the backend, this is prioritized over the client key
/// - `anon_key`: Set the client key for the backend, this cannot access non-public resources
///
/// ## Authorization keys
///
/// There are two types of key in the Supabase, one is anon_key(Client key), another one is
/// service_role_key(Secret key). The former one can only access public resources while the latter one
/// can access all resources.
///
/// # Example
///
/// ## Basic Setup
///
/// ```
///
/// ### Using Builder
///
/// ```no_run
/// use anyhow::Result;
/// use opendal::services::Supabase;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let mut builder = services::Supabase::default();
///     builder.root("/");
///     builder.bucket("test_bucket");
///     builder.endpoint("http://127.0.0.1:54321");
///     // this sets up the anon_key, which means this operator can only access public resource
///     builder.anon_key("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6ImFub24iLCJleHAiOjE5ODM4MTI5OTZ9.CRXP1A7WOeoJeXxjNni43kdQwgnWNReilDMblYTn_I0");
///
///     let op: Operator = Operator::new(builder)?.finish();
///    
///     Ok(())
/// }
/// ```
#[derive(Default)]
pub struct SupabaseBuilder {
    root: Option<String>,

    bucket: String,
    endpoint: Option<String>,

    service_key: Option<String>,
    anon_key: Option<String>,

    // todo: optional public, currently true always
    // todo: optional file_size_limit, currently 0
    // todo: optional allowed_mime_types, currently only string
    http_client: Option<HttpClient>,
}

impl Debug for SupabaseBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SupabaseBuilder")
            .field("root", &self.root)
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl SupabaseBuilder {
    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// Set bucket name of this backend.
    pub fn bucket(&mut self, bucket: &str) -> &mut Self {
        self.bucket = bucket.to_string();
        self
    }

    /// Set endpoint of this backend.
    ///
    /// Endpoint must be full uri
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        self.endpoint = if endpoint.is_empty() {
            None
        } else {
            Some(endpoint.trim_end_matches('/').to_string())
        };

        self
    }

    /// Set the service_role_key for this backend
    pub fn service_key(&mut self, service_key: &str) -> &mut Self {
        self.service_key = Some(service_key.to_string());
        self
    }

    /// Set the anon_key for this backend
    pub fn anon_key(&mut self, anon_key: &str) -> &mut Self {
        self.anon_key = Some(anon_key.to_string());
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

impl Builder for SupabaseBuilder {
    const SCHEME: Scheme = Scheme::Supabase;
    type Accessor = SupabaseBackend;

    fn from_map(map: std::collections::HashMap<String, String>) -> Self {
        let mut builder = SupabaseBuilder::default();

        map.get("root").map(|v| builder.root(v));
        map.get("bucket").map(|v| builder.bucket(v));
        map.get("endpoint").map(|v| builder.endpoint(v));
        map.get("anon_key").map(|v| builder.anon_key(v));
        map.get("service_key").map(|v| builder.service_key(v));

        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        let root = normalize_root(&self.root.take().unwrap_or_default());
        debug!("backend use root {}", &root);

        let bucket = &self.bucket;

        let endpoint = self.endpoint.take().unwrap_or_default();

        let http_client = if let Some(client) = self.http_client.take() {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Supabase)
            })?
        };

        // priority:
        //  if the service key is loaded, it is used.
        //  Else if the anon key is loaded, it is used.
        let (auth_key, auth) = if let Some(k) = &self.service_key {
            (Some(k), true)
        } else if let Some(k) = &self.anon_key {
            (Some(k), false)
        } else {
            (None, false)
        };

        let auth_key = auth_key.map(|k| HeaderValue::from_str(k).unwrap());

        let core = SupabaseCore::new(&root, bucket, &endpoint, auth_key, auth, http_client);

        let core = Arc::new(core);

        Ok(SupabaseBackend { core })
    }
}

#[derive(Debug)]
pub struct SupabaseBackend {
    core: Arc<SupabaseCore>,
}

#[async_trait]
impl Accessor for SupabaseBackend {
    type Reader = IncomingAsyncBody;
    type BlockingReader = ();
    type Writer = SupabaseWriter;
    type BlockingWriter = ();
    // todo: implement Pager to support list and scan
    type Pager = ();
    type BlockingPager = ();

    fn info(&self) -> AccessorInfo {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Supabase)
            .set_root(&self.core.root)
            .set_name(&self.core.bucket)
            .set_capability(Capability {
                stat: true,
                read: true,
                write: true,

                ..Default::default()
            });

        am
    }

    async fn read(&self, path: &str, _args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = if self.core.auth {
            self.core.supabase_get_object_auth(path).await?
        } else {
            self.core.supabase_get_object_public(path).await?
        };

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
        Ok((
            RpWrite::default(),
            SupabaseWriter::new(self.core.clone(), path, args),
        ))
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        let resp = if self.core.auth {
            self.core.supabase_get_object_info_auth(path).await?
        } else {
            self.core.supabase_get_object_info_public(path).await?
        };

        let status = resp.status();

        match status {
            StatusCode::OK => parse_into_metadata(path, resp.headers()).map(RpStat::new),
            StatusCode::NOT_FOUND if path.ends_with('/') => {
                Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
            }
            _ => Err(parse_error(resp).await?),
        }
    }
}
