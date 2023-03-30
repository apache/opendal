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

use std::mem;

use async_trait::async_trait;

use super::list_response::Multistatus;
use crate::raw::build_rel_path;
use crate::raw::oio;
use crate::EntryMode;
use crate::Metadata;
use crate::Result;

pub struct WebdavPager {
    root: String,
    path: String,
    multistates: Multistatus,
}

impl WebdavPager {
    pub fn new(root: &str, path: &str, multistates: Multistatus) -> Self {
        Self {
            root: root.into(),
            path: path.into(),
            multistates,
        }
    }
}

#[async_trait]
impl oio::Page for WebdavPager {
    async fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        if self.multistates.response.is_empty() {
            return Ok(None);
        };
        let oes = mem::take(&mut self.multistates.response);

        let oes = oes
            .into_iter()
            .filter_map(|de| {
                let path = de.href;
                let normalized_path = if self.root != path {
                    build_rel_path(&self.root, &path)
                } else {
                    path
                };

                if normalized_path == self.path {
                    // WebDav server may return the current path as an entry.
                    return None;
                }

                let entry = if de.propstat.prop.resourcetype.value
                    == Some(super::list_response::ResourceType::Collection)
                {
                    oio::Entry::new(&normalized_path, Metadata::new(EntryMode::DIR))
                } else {
                    oio::Entry::new(&normalized_path, Metadata::new(EntryMode::FILE))
                };

                Some(entry)
            })
            .collect();

        Ok(Some(oes))
    }
}
