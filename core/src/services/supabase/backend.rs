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
use http::StatusCode;
use log::debug;

use super::core::*;
use super::error::parse_error;
use super::writer::*;
use crate::ops::*;
use crate::raw::*;
use crate::*;

#[derive(Default)]
pub struct SupabaseBuilder {
    root: Option<String>,

    bucket: String,
    endpoint: Option<String>,

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
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    pub fn bucket(&mut self, bucket: &str) -> &mut Self {
        self.bucket = bucket.to_string();
        self
    }

    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        self.endpoint = if endpoint.is_empty() {
            None
        } else {
            Some(endpoint.to_string())
        };

        self
    }

    pub fn http_client(&mut self, client: HttpClient) -> &mut Self {
        self.http_client = Some(client);
        self
    }
}

impl Builder for SupabaseBuilder {
    const SCHEME: Scheme = Scheme::Supabase;
    type Accessor = SupabaseBackend;

    fn from_map(map: std::collections::HashMap<String, String>) -> Self {
        unimplemented!()
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
                    .with_context("service", Scheme::S3)
            })?
        };

        let mut core = SupabaseCore {
            root,
            bucket: bucket.to_owned(),
            endpoint,
            auth_key: None,
            http_client,
        };

        core.load_auth_key("OPENDAL_SUPABASE_AUTH_KEY");

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
        use AccessorCapability::*;
        use AccessorHint::*;

        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Supabase)
            .set_root(&self.core.root)
            .set_name(&self.core.bucket)
            .set_capabilities(Read | Write)
            .set_hints(ReadStreamable);

        am
    }

    async fn create_dir(&self, path: &str, _: OpCreate) -> Result<RpCreate> {
        unimplemented!()
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.core.supabase_get_object_public(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                let meta = parse_into_metadata(path, resp.headers())?;
                return Ok((RpRead::with_metadata(meta), resp.into_body()));
            }
            _ => {}
        }

        let resp = self.core.supabase_get_object_auth(path).await?;
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

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        unimplemented!()
    }
}
