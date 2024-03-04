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
use http::StatusCode;

use super::core::PcloudCore;
use super::core::StatResponse;
use super::core::UploadFileResponse;
use super::error::parse_error;
use super::error::parse_result;
use crate::raw::*;
use crate::*;

pub type PcloudWriters = oio::OneShotWriter<PcloudWriter>;

pub struct PcloudWriter {
    core: Arc<PcloudCore>,
    path: String,
}

impl PcloudWriter {
    pub fn new(core: Arc<PcloudCore>, path: String) -> Self {
        PcloudWriter { core, path }
    }
}

#[async_trait]
impl oio::OneShotWrite for PcloudWriter {
    async fn write_once(&self, bs: &dyn oio::WriteBuf) -> Result<()> {
        let bs = bs.bytes(bs.remaining());

        self.core.ensure_dir_exists(&self.path).await?;

        let mut exist = false;

        while !exist {
            let resp = self.core.upload_file(&self.path, bs.clone()).await?;

            let status = resp.status();

            match status {
                StatusCode::OK => {
                    let bs = resp.into_body().bytes().await?;
                    let resp: UploadFileResponse =
                        serde_json::from_slice(&bs).map_err(new_json_deserialize_error)?;
                    let result = resp.result;

                    parse_result(result)?;

                    if result != 0 {
                        return Err(Error::new(ErrorKind::Unexpected, &format!("{resp:?}")));
                    }

                    if resp.metadata.len() != 1 {
                        return Err(Error::new(ErrorKind::Unexpected, &format!("{resp:?}")));
                    }

                    let resp = self.core.stat(&self.path).await?;

                    let bs = resp.into_body().bytes().await?;
                    let resp: StatResponse =
                        serde_json::from_slice(&bs).map_err(new_json_deserialize_error)?;
                    let result = resp.result;

                    parse_result(result)?;

                    if result == 2010 || result == 2055 || result == 2002 {
                        let mut err = Error::new(ErrorKind::Unexpected, &format!("{resp:?}"));
                        err = err.set_temporary();
                        return Err(err);
                    }
                    exist = true;
                }
                _ => return Err(parse_error(resp).await?),
            }
        }
        Ok(())
    }
}
