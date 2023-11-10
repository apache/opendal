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

use crate::raw::*;
use crate::Result;

use super::core::AlluxioCore;

pub type AlluxioWriters = oio::OneShotWriter<AlluxioWriter>;

pub struct AlluxioWriter {
    core: Arc<AlluxioCore>,

    _op: OpWrite,
    path: String,
}

impl AlluxioWriter {
    pub fn new(core: Arc<AlluxioCore>, _op: OpWrite, path: String) -> Self {
        AlluxioWriter { core, _op, path }
    }
}

#[async_trait]
impl oio::OneShotWrite for AlluxioWriter {
    async fn write_once(&self, bs: &dyn oio::WriteBuf) -> Result<()> {
        let bs = oio::ChunkedBytes::from_vec(bs.vectored_bytes(bs.remaining()));

        let stream_id = self.core.create_file(&self.path).await?;

        self.core
            .write(stream_id, AsyncBody::ChunkedBytes(bs))
            .await?;

        self.core.close(stream_id).await?;

        Ok(())
    }
}
