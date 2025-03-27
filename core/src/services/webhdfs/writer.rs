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

use bytes::Buf;
use http::StatusCode;
use uuid::Uuid;

use super::core::WebhdfsCore;
use super::error::parse_error;
use crate::raw::*;
use crate::services::webhdfs::message::FileStatusWrapper;
use crate::*;

pub type WebhdfsWriters =
    TwoWays<oio::BlockWriter<WebhdfsWriter>, oio::AppendWriter<WebhdfsWriter>>;

pub struct WebhdfsWriter {
    core: Arc<WebhdfsCore>,

    op: OpWrite,
    path: String,
}

impl WebhdfsWriter {
    pub fn new(core: Arc<WebhdfsCore>, op: OpWrite, path: String) -> Self {
        WebhdfsWriter { core, op, path }
    }
}

impl oio::BlockWrite for WebhdfsWriter {
    async fn write_once(&self, size: u64, body: Buffer) -> Result<Metadata> {
        let req = self
            .core
            .webhdfs_create_object_request(&self.path, Some(size), &self.op, body)
            .await?;

        let resp = self.core.info.http_client().send(req).await?;

        let status = resp.status();
        match status {
            StatusCode::CREATED | StatusCode::OK => Ok(Metadata::default()),
            _ => Err(parse_error(resp)),
        }
    }

    async fn write_block(&self, block_id: Uuid, size: u64, body: Buffer) -> Result<()> {
        let Some(ref atomic_write_dir) = self.core.atomic_write_dir else {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "write multi is not supported when atomic is not set",
            ));
        };
        let req = self
            .core
            .webhdfs_create_object_request(
                &format!("{}{}", atomic_write_dir, block_id),
                Some(size),
                &self.op,
                body,
            )
            .await?;

        let resp = self.core.info.http_client().send(req).await?;

        let status = resp.status();
        match status {
            StatusCode::CREATED | StatusCode::OK => Ok(()),
            _ => Err(parse_error(resp)),
        }
    }

    async fn complete_block(&self, block_ids: Vec<Uuid>) -> Result<Metadata> {
        let Some(ref atomic_write_dir) = self.core.atomic_write_dir else {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "write multi is not supported when atomic is not set",
            ));
        };
        let first_block_id = format!("{}{}", atomic_write_dir, block_ids[0].clone());
        if block_ids.len() >= 2 {
            let sources: Vec<String> = block_ids[1..]
                .iter()
                .map(|s| format!("{}{}", atomic_write_dir, s))
                .collect();
            // concat blocks
            let req = self.core.webhdfs_concat_request(&first_block_id, sources)?;

            let resp = self.core.info.http_client().send(req).await?;

            let status = resp.status();

            if status != StatusCode::OK {
                return Err(parse_error(resp));
            }
        }
        // delete the path file
        let resp = self.core.webhdfs_delete(&self.path).await?;
        let status = resp.status();
        if status != StatusCode::OK {
            return Err(parse_error(resp));
        }

        // rename concat file to path
        let resp = self
            .core
            .webhdfs_rename_object(&first_block_id, &self.path)
            .await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(Metadata::default()),
            _ => Err(parse_error(resp)),
        }
    }

    async fn abort_block(&self, block_ids: Vec<Uuid>) -> Result<()> {
        for block_id in block_ids {
            let resp = self.core.webhdfs_delete(&block_id.to_string()).await?;
            match resp.status() {
                StatusCode::OK => {}
                _ => return Err(parse_error(resp)),
            }
        }
        Ok(())
    }
}

impl oio::AppendWrite for WebhdfsWriter {
    async fn offset(&self) -> Result<u64> {
        let resp = self.core.webhdfs_get_file_status(&self.path).await?;
        let status = resp.status();
        match status {
            StatusCode::OK => {
                let bs = resp.into_body();
                let file_status = serde_json::from_reader::<_, FileStatusWrapper>(bs.reader())
                    .map_err(new_json_deserialize_error)?
                    .file_status;

                Ok(file_status.length)
            }
            StatusCode::NOT_FOUND => {
                let req = self
                    .core
                    .webhdfs_create_object_request(&self.path, None, &self.op, Buffer::new())
                    .await?;

                let resp = self.core.info.http_client().send(req).await?;
                let status = resp.status();

                match status {
                    StatusCode::CREATED | StatusCode::OK => Ok(0),
                    _ => Err(parse_error(resp)),
                }
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn append(&self, _offset: u64, size: u64, body: Buffer) -> Result<Metadata> {
        let location = self.core.webhdfs_init_append_request(&self.path).await?;
        let req = self.core.webhdfs_append_request(&location, size, body)?;
        let resp = self.core.info.http_client().send(req).await?;

        let status = resp.status();
        match status {
            StatusCode::OK => Ok(Metadata::default()),
            _ => Err(parse_error(resp)),
        }
    }
}
