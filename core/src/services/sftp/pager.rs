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

use async_trait::async_trait;
use openssh_sftp_client::fs::DirEntry;

use crate::raw::oio;
use crate::Result;

pub struct SftpPager {
    dir: Box<[DirEntry]>,
    path: String,
    limit: Option<usize>,
    complete: bool,
}

impl SftpPager {
    pub fn new(dir: Box<[DirEntry]>, path: String, limit: Option<usize>) -> Self {
        Self {
            dir,
            path,
            limit,
            complete: false,
        }
    }

    pub fn empty() -> Self {
        Self {
            dir: Box::new([]),
            path: String::new(),
            limit: None,
            complete: true,
        }
    }
}

#[async_trait]
impl oio::Page for SftpPager {
    async fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        if self.complete {
            return Ok(None);
        }

        if self.path == "/" {
            self.path = "".to_owned();
        }

        let iter = self
            .dir
            .iter()
            .filter(|e| {
                e.filename().to_str().unwrap() != "." && e.filename().to_str().unwrap() != ".."
            })
            .map(|e| map_entry(self.path.clone(), e.clone()));

        let v: Vec<oio::Entry> = if let Some(limit) = self.limit {
            iter.take(limit).collect()
        } else {
            iter.collect()
        };

        self.complete = true;

        if v.len() == 0 {
            Ok(None)
        } else {
            Ok(Some(v))
        }
    }
}

fn map_entry(prefix: String, value: DirEntry) -> oio::Entry {
    if value.filename().to_str().unwrap() == "file-71" {
        println!("map_entry: {:?}", value);
    }

    oio::Entry::new(
        format!(
            "{}{}{}",
            prefix,
            value.filename().to_str().unwrap(),
            if value.file_type().unwrap().is_dir() {
                "/"
            } else {
                ""
            }
        )
        .as_str(),
        value.metadata().into(),
    )
}
