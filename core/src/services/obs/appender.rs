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

use async_trait::async_trait;
use bytes::Bytes;
use http::header::CONTENT_LENGTH;
use http::StatusCode;

use super::core::*;
use super::error::parse_error;
use crate::raw::*;
use crate::*;

const X_OBS_BLOB_TYPE: &str = "x-obs-blob-type";
const X_OBS_BLOB_APPEND_OFFSET: &str = "x-obs-blob-append-offset";

pub struct ObsAppender {
    core: Arc<ObsCore>,

    op: OpAppend,
    path: String,

    position: Option<u64>,
}

impl ObsAppender {
    pub fn new(core: Arc<ObsCore>, path: &str, op: OpAppend) -> Self {
        Self {
            core,
            op,
            path: path.to_string(),
            position: None,
        }
    }
}

#[async_trait]
impl oio::Append for ObsAppender {
    async fn append(&mut self, bs: Bytes) -> Result<()> {
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
