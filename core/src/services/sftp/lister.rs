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

use std::pin::Pin;

use futures::StreamExt;
use openssh_sftp_client::fs::DirEntry;
use openssh_sftp_client::fs::ReadDir;

use super::error::parse_sftp_error;
use crate::raw::oio;
use crate::raw::oio::Entry;
use crate::Result;

pub struct SftpLister {
    dir: Pin<Box<ReadDir>>,
    prefix: String,
}

impl SftpLister {
    pub fn new(dir: ReadDir, path: String) -> Self {
        let prefix = if path == "/" { "".to_owned() } else { path };

        SftpLister {
            dir: Box::pin(dir),
            prefix,
        }
    }
}

impl oio::List for SftpLister {
    async fn next(&mut self) -> Result<Option<Entry>> {
        loop {
            let item = self
                .dir
                .next()
                .await
                .transpose()
                .map_err(parse_sftp_error)?;

            match item {
                Some(e) => {
                    if e.filename().to_str() == Some("..") {
                        continue;
                    } else if e.filename().to_str() == Some(".") {
                        let mut path = self.prefix.as_str();
                        if self.prefix.is_empty() {
                            path = "/";
                        }
                        return Ok(Some(Entry::new(path, e.metadata().into())));
                    } else {
                        return Ok(Some(map_entry(self.prefix.as_str(), e)));
                    }
                }
                None => return Ok(None),
            }
        }
    }
}

fn map_entry(prefix: &str, value: DirEntry) -> Entry {
    let path = format!(
        "{}{}{}",
        prefix,
        value.filename().to_str().unwrap(),
        if value.file_type().unwrap().is_dir() {
            "/"
        } else {
            ""
        }
    );

    Entry::new(path.as_str(), value.metadata().into())
}
