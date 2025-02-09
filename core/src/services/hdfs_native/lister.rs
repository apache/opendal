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

use std::collections::VecDeque;
use std::sync::Arc;

use hdfs_native::client::Client;

use crate::raw::*;
use crate::services::hdfs_native::error::parse_hdfs_error;
use crate::*;

pub struct HdfsNativeLister {
    client: Arc<Client>,
    path: String,
    entries: VecDeque<oio::Entry>,
}

impl HdfsNativeLister {
    pub fn new(path: String, client: Arc<Client>) -> Self {
        HdfsNativeLister {
            client,
            path,
            entries: VecDeque::new(),
        }
    }
}

impl oio::List for HdfsNativeLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        // 如果队列中还有条目，直接返回
        if let Some(entry) = self.entries.pop_front() {
            return Ok(Some(entry));
        }

        // 第一次调用时获取所有条目
        if self.entries.is_empty() {
            let entries = self
                .client
                .list_status(&self.path, true)
                .await
                .map_err(parse_hdfs_error)?;

            // 如果目录为空，返回
            if entries.is_empty() {
                return Ok(None);
            }

            // 转换所有条目并存入队列
            for entry in entries {
                let path = format!("{}/{}", self.path.trim_end_matches('/'), entry.path);
                let path = path.trim_start_matches('/').to_string();

                let mode = if entry.isdir {
                    EntryMode::DIR
                } else {
                    EntryMode::FILE
                };

                let mut meta = Metadata::new(mode);
                meta.set_content_length(entry.length as u64)
                    .set_last_modified(parse_datetime_from_from_timestamp_millis(
                        entry.modification_time as i64,
                    )?);

                self.entries.push_back(oio::Entry::new(&path, meta));
            }

            // 返回第一个条目
            return Ok(self.entries.pop_front());
        }

        // 所有条目都已返回
        Ok(None)
    }
}
