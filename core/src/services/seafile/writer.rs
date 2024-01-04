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
use http::header;
use http::Request;
use http::StatusCode;

use super::core::SeafileCore;
use super::error::parse_error;
use crate::raw::*;
use crate::*;

pub type SeafileWriters = oio::OneShotWriter<SeafileWriter>;

pub struct SeafileWriter {
    core: Arc<SeafileCore>,
    _op: OpWrite,
    path: String,
}

impl SeafileWriter {
    pub fn new(core: Arc<SeafileCore>, op: OpWrite, path: String) -> Self {
        SeafileWriter {
            core,
            _op: op,
            path,
        }
    }
}

#[async_trait]
impl oio::OneShotWrite for SeafileWriter {
    async fn write_once(&self, bs: &dyn oio::WriteBuf) -> Result<()> {
        let path = build_abs_path(&self.core.root, &self.path);
        let bs = oio::ChunkedBytes::from_vec(bs.vectored_bytes(bs.remaining()));

        let upload_url = self.core.get_upload_url().await?;

        let req = Request::post(upload_url);

        let paths = path.split('/').collect::<Vec<&str>>();
        let filename = paths[paths.len() - 1];
        let relative_path = path.replace(filename, "");

        let file_part = FormDataPart::new("file")
            .header(
                header::CONTENT_DISPOSITION,
                format!("form-data; name=\"file\"; filename=\"{filename}\"")
                    .parse()
                    .unwrap(),
            )
            .stream(bs.len() as u64, Box::new(bs));

        let multipart = Multipart::new()
            .part(file_part)
            .part(FormDataPart::new("parent_dir").content("/"))
            .part(FormDataPart::new("relative_path").content(relative_path.clone()))
            .part(FormDataPart::new("replace").content("1"));

        let req = multipart.apply(req)?;

        let resp = self.core.send(req).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                resp.into_body().consume().await?;
                Ok(())
            }
            _ => Err(parse_error(resp).await?),
        }
    }
}
