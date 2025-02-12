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

use crate::raw::oio;
use crate::Result;

pub struct HdfsNativeLister {
    root: String,
    stream: BoxStream<'static, Result<FileStatus>>,
    current_path: Option<String>,
}

impl HdfsNativeLister {
    pub fn new(root: &str, stream: BoxStream<'static, Result<FileStatus>>, path: &str) -> Self {
        HdfsNativeLister {
            root: root.to_string(),
            stream,
            current_path: Some(path.to_string()),
        }
    }
}

impl oio::List for HdfsNativeLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        if let Some(path) = self.current_path.take() {
            return Ok(Some(oio::Entry::new(&path, Metadata::new(EntryMode::DIR))));
        }

        let status: FileStatus = match self.stream.next().await.map_err(parse_hdfs_error)? {
            Some(status) => status,
            None => return Ok(None),
        };

        let path = build_rel_path(&self.root, status.path());

        let entry = if status.isdir {
            // Make sure we are returning the correct path.
            oio::Entry::new(&format!("{path}/"), Metadata::new(EntryMode::DIR))
        } else {
            let meta = Metadata::new(EntryMode::FILE)
                .with_content_length(status.length as u64)
                .with_last_modified(parse_datetime_from_from_timestamp_millis(
                    status.modification_time as i64,
                )?);
            oio::Entry::new(&path, meta)
        };

        Ok(Some(entry))
    }
}
