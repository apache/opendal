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

use http::StatusCode;
use uuid::Uuid;

use super::backend::WebhdfsBackend;
use super::error::parse_error;
use crate::raw::*;
use crate::*;

pub type WebhdfsWriters =
    TwoWays<oio::BlockWriter<WebhdfsWriter>, oio::AppendWriter<WebhdfsWriter>>;

pub struct WebhdfsWriter {
    backend: WebhdfsBackend,

    op: OpWrite,
    path: String,
}

impl WebhdfsWriter {
    pub fn new(backend: WebhdfsBackend, op: OpWrite, path: String) -> Self {
        WebhdfsWriter { backend, op, path }
    }
}

impl oio::BlockWrite for WebhdfsWriter {
    async fn write_once(&self, size: u64, body: RequestBody) -> Result<()> {
        self.backend
            .webhdfs_create_object(&self.path, Some(size), &self.op, body)
            .await
    }

    async fn write_block(&self, block_id: Uuid, size: u64, body: RequestBody) -> Result<()> {
        let Some(ref atomic_write_dir) = self.backend.atomic_write_dir else {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "write multi is not supported when atomic is not set",
            ));
        };
        self.backend
            .webhdfs_create_object(
                &format!("{}{}", atomic_write_dir, block_id),
                Some(size),
                &self.op,
                body,
            )
            .await
    }

    async fn complete_block(&self, block_ids: Vec<Uuid>) -> Result<()> {
        let Some(ref atomic_write_dir) = self.backend.atomic_write_dir else {
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
            let req = self
                .backend
                .webhdfs_concat_request(&first_block_id, sources)?;

            let (parts, body) = self.backend.client.send(req).await?.into_parts();

            if parts.status != StatusCode::OK {
                let bs = body.to_bytes().await?;
                return Err(parse_error(parts, bs)?);
            } else {
                body.consume().await?;
            }
        }
        // delete the path file
        self.backend.webhdfs_delete(&self.path).await?;

        // rename concat file to path
        self.backend
            .webhdfs_rename_object(&first_block_id, &self.path)
            .await
    }

    async fn abort_block(&self, block_ids: Vec<Uuid>) -> Result<()> {
        for block_id in block_ids {
            self.backend.webhdfs_delete(&block_id.to_string()).await?;
        }
        Ok(())
    }
}

impl oio::AppendWrite for WebhdfsWriter {
    async fn offset(&self) -> Result<u64> {
        Ok(0)
    }

    async fn append(&self, _offset: u64, size: u64, body: RequestBody) -> Result<()> {
        let resp = self.backend.webhdfs_get_file_status(&self.path).await;

        let location;

        match resp {
            Ok(_) => {
                location = self.backend.webhdfs_init_append_request(&self.path).await?;
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {
                self.backend
                    .webhdfs_create_object(&self.path, None, &self.op, RequestBody::Empty)
                    .await?;

                location = self.backend.webhdfs_init_append_request(&self.path).await?;
            }
            Err(err) => return Err(err),
        }

        let req = self
            .backend
            .webhdfs_append_request(&location, size, body)
            .await?;

        let (parts, body) = self.backend.client.send(req).await?.into_parts();

        match parts.status {
            StatusCode::OK => {
                body.consume().await?;
                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }
}
