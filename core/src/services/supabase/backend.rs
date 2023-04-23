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

use super::core::*;
use super::pager::*;
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
        unimplemented!()
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
    type Pager = SupabasePager;
    type BlockingPager = ();

    fn info(&self) -> AccessorInfo {
        unimplemented!()
    }

    async fn create_dir(&self, path: &str, _: OpCreate) -> Result<RpCreate> {
        unimplemented!()
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        unimplemented!()
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        unimplemented!()
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        unimplemented!( )
    }
}
