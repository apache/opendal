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

use async_trait::async_trait;
use http::StatusCode;
use std::task::{Context, Poll};

use super::backend::IpmfsBackend;
use super::error::parse_error;
use crate::raw::oio::WriteBuf;
use crate::raw::*;
use crate::*;

pub struct IpmfsWriter {
    backend: IpmfsBackend,

    path: String,
}

impl IpmfsWriter {
    pub fn new(backend: IpmfsBackend, path: String) -> Self {
        IpmfsWriter { backend, path }
    }
}

impl oio::OneShotWrite for IpmfsWriter {
    async fn write_once(&self, bs: &dyn WriteBuf) -> Result<()> {
        let size = bs.remaining();
        let resp = self
            .backend
            .ipmfs_write(&self.path, bs.copy_to_bytes(size))
            .await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => {
                resp.into_body().consume().await?;
                Ok(())
            }
            _ => Err(parse_error(resp).await?),
        }
    }
}
