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
use openssh_sftp_client::fs::{DirEntry, ReadDir};

use crate::raw::oio;
use crate::Result;

pub struct SftpPager {
    dir: ReadDir,
    limit: Option<usize>,
}

impl SftpPager {
    pub fn new(dir: ReadDir, limit: Option<usize>) -> Self {
        Self { dir, limit }
    }
}

#[async_trait]
impl oio::Page for SftpPager {
    async fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        let v: Vec<oio::Entry> = if let Some(limit) = self.limit {
            self.dir.clone().into_iter().map(|e| e.into()).take(limit).collect()
        } else {
            self.dir.clone().into_iter().map(|e| e.into()).collect()
        };

        if v.len() == 0 {
            Ok(None)
        } else {
            Ok(Some(v))
        }
    }
}

impl From<DirEntry> for oio::Entry {
    fn from(value: DirEntry) -> Self {
        oio::Entry::new(
            value.filename().as_os_str().to_str().unwrap(),
            value.metadata().into(),
        )
    }
}
