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

use crate::raw::*;
use crate::services::hdfs_native::error::parse_hdfs_error;
use crate::*;
use hdfs_native::file::FileWriter;
use log::error;
use std::sync::Arc;
pub struct HdfsNativeWriter {
    target_path: String,
    tmp_path: Option<String>,
    f: FileWriter,
    client: Arc<hdfs_native::Client>,
    target_path_exists: bool,
    size: u64,
}

impl HdfsNativeWriter {
    pub fn new(
        target_path: String,
        tmp_path: Option<String>,
        f: FileWriter,
        client: Arc<hdfs_native::Client>,
        target_path_exists: bool,
        initial_size: u64,
    ) -> Self {
        HdfsNativeWriter {
            target_path,
            tmp_path,
            f,
            client,
            target_path_exists,
            size: initial_size,
        }
    }
}

impl oio::Write for HdfsNativeWriter {
    async fn write(&mut self, mut buf: Buffer) -> Result<()> {
        let len = buf.len() as u64;

        // error!("HdfsNativeWriter write start");
        for bs in buf.by_ref() {
            // error!("HdfsNativeWriter buf write start");
            self.f.write(bs).await.map_err(|e| {
                error!("write error: {:?}", e);
                parse_hdfs_error(e)
            })?;
            // error!("HdfsNativeWriter buf write end");
        }

        // error!("HdfsNativeWriter write end");
        self.size += len;
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.f.close().await.map_err(parse_hdfs_error)?;

        if let Some(tmp_path) = &self.tmp_path {
            // we must delete the target_path, otherwise the rename_file operation will fail
            if self.target_path_exists {
                self.client
                    .delete(&self.target_path, false)
                    .await
                    .map_err(parse_hdfs_error)?;
            }
            self.client
                .rename(tmp_path, &self.target_path, true)
                .await
                .map_err(parse_hdfs_error)?;
        }

        Ok(Metadata::default().with_content_length(self.size))
    }

    async fn abort(&mut self) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "HdfsNativeWriter doesn't support abort",
        ))
    }
}
