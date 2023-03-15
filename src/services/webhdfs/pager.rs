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

use super::message::FileStatus;
use crate::raw::*;
use crate::*;

pub struct WebhdfsPager {
    path: String,
    statuses: Vec<FileStatus>,
}

impl WebhdfsPager {
    pub fn new(path: &str, statuses: Vec<FileStatus>) -> Self {
        Self {
            path: path.to_string(),
            statuses,
        }
    }
}

#[async_trait]
impl oio::Page for WebhdfsPager {
    async fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        if self.statuses.is_empty() {
            return Ok(None);
        }

        let mut entries = Vec::with_capacity(self.statuses.len());

        while let Some(status) = self.statuses.pop() {
            let mut path = format!("{}/{}", &self.path, status.path_suffix);

            let meta: Metadata = status.try_into()?;
            if meta.mode().is_dir() {
                path += "/"
            }
            let entry = oio::Entry::new(&path, meta);
            entries.push(entry);
        }

        Ok(Some(entries))
    }
}
