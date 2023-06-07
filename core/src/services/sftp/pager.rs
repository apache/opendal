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

use async_trait::async_trait;
use futures::StreamExt;
use openssh_sftp_client::fs::DirEntry;
use openssh_sftp_client::fs::ReadDir;

use crate::raw::oio;
use crate::Result;

pub struct SftpPager {
    dir: Pin<Box<ReadDir>>,
    prefix: String,
    limit: usize,
}

impl SftpPager {
    pub fn new(dir: ReadDir, path: String, limit: Option<usize>) -> Self {
        let prefix = if path == "/" { "".to_owned() } else { path };

        let limit = limit.unwrap_or(usize::MAX);

        SftpPager {
            dir: Box::pin(dir),
            prefix,
            limit,
        }
    }
}

#[async_trait]
impl oio::Page for SftpPager {
    async fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        if self.limit == 0 {
            return Ok(None);
        }

        let item = self.dir.next().await;

        match item {
            Some(Ok(e)) => {
                if e.filename().to_str() == Some(".") || e.filename().to_str() == Some("..") {
                    self.next().await
                } else {
                    self.limit -= 1;
                    Ok(Some(vec![map_entry(self.prefix.as_str(), e.clone())]))
                }
            }
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }
}

fn map_entry(prefix: &str, value: DirEntry) -> oio::Entry {
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

    oio::Entry::new(path.as_str(), value.metadata().into())
}
