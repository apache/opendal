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

use std::sync::Arc;

use bytes::Bytes;
use http::StatusCode;

use super::core::AzdlsCore;
use super::error::parse_error;
use crate::raw::*;
use crate::*;

pub type AzdlsWriters = TwoWays<oio::OneShotWriter<AzdlsWriter>, oio::AppendWriter<AzdlsWriter>>;

pub struct AzdlsWriter {
    core: Arc<AzdlsCore>,

    op: OpWrite,
    path: String,
}

impl AzdlsWriter {
    pub fn new(core: Arc<AzdlsCore>, op: OpWrite, path: String) -> Self {
        AzdlsWriter { core, op, path }
    }
}

impl oio::OneShotWrite for AzdlsWriter {
    async fn write_once(&self, bs: Bytes) -> Result<()> {
        self.core.azdls_create_file(&self.path, &self.op).await?;

        self.core
            .azdls_update_request(&self.path, Some(bs.len() as u64), 0, RequestBody::Bytes(bs))
            .await
    }
}

impl oio::AppendWrite for AzdlsWriter {
    async fn offset(&self) -> Result<u64> {
        let meta = self.core.azdls_get_properties(&self.path).await?;
        Ok(meta.content_length())
    }

    async fn append(&self, offset: u64, size: u64, body: RequestBody) -> Result<()> {
        if offset == 0 {
            self.core.azdls_create_file(&self.path, &self.op).await?;
        }

        self.core
            .azdls_update_request(&self.path, Some(size), offset, body)
            .await
    }
}
