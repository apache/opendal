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

use crate::raw::build_rel_path;
use crate::raw::oio;
use crate::raw::parse_datetime_from_from_timestamp_millis;
use crate::services::hdfs_native::error::parse_hdfs_error;
use crate::EntryMode;
use crate::Metadata;
use crate::Result;
use hdfs_native::client::ListStatusIterator;

pub struct HdfsNativeLister {
    root: String,
    iter: ListStatusIterator,
    current_path: Option<String>,
    iter_to_end: bool,
}

impl HdfsNativeLister {
    pub fn new(
        root: &str,
        client: &hdfs_native::Client,
        abs_path: &str,
        current_path: Option<String>,
    ) -> Self {
        HdfsNativeLister {
            root: root.to_string(),
            iter: client.list_status_iter(abs_path, false),
            current_path,
            iter_to_end: false,
        }
    }
}

impl oio::List for HdfsNativeLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        if self.iter_to_end {
            return Ok(None);
        }

        if let Some(path) = self.current_path.take() {
            return Ok(Some(oio::Entry::new(&path, Metadata::new(EntryMode::DIR))));
        }

        match self.iter.next().await {
            Some(Ok(status)) => {
                let path = build_rel_path(&self.root, &status.path);

                let entry = if status.isdir {
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
            Some(Err(e)) => Err(parse_hdfs_error(e)),
            None => {
                self.iter_to_end = true;
                Ok(None)
            }
        }
    }
}
