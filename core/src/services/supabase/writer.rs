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

use super::core::*;
use crate::ops::OpWrite;
use crate::raw::*;
use crate::*;

pub struct SupabaseWriter {
    core: Arc<SupabaseCore>,

    op: OpWrite,
    path: String,

    buffer: oio::VectorCursor,
    buffer_size: usize,
}

impl SupabaseWriter {
    pub fn new(core: Arc<SupabaseCore>, path: &str, op: OpWrite) -> Self {
        SupabaseWriter {
            core,
            op,
            path: path.to_string(),
            buffer: oio::VectorCursor::new(),
            buffer_size: 8 * 1024 * 1024,
        }
    }
}

#[async_trait]
impl oio::Write for SupabaseWriter {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        unimplemented!()
    }

    async fn abort(&mut self) -> Result<()> {
        unimplemented!()
    }

    async fn close(&mut self) -> Result<()> {
        unimplemented!()
    }
}
