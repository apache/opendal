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

use crate::{
    raw::{
        build_rel_path,
        oio::{self},
    },
    EntryMode, Metadata,
};

use super::graph_model::{GraphApiOnedriveResponse, ItemType};
use crate::Result;
use async_trait::async_trait;

pub(crate) struct OnedrivePager {
    root: String,
    path: String,
    onedrive_response: GraphApiOnedriveResponse,
}

impl OnedrivePager {
    const DRIVE_ROOT_PREFIX: &'static str = "/drive/root:";

    pub(crate) fn new(root: &str, path: &str, onedrive_response: GraphApiOnedriveResponse) -> Self {
        Self {
            root: root.into(),
            path: path.into(),
            onedrive_response,
        }
    }
}

#[async_trait]

impl oio::Page for OnedrivePager {
    async fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        if self.onedrive_response.value.is_empty() {
            return Ok(None);
        };
        let oes = mem::take(&mut self.onedrive_response.value);

        let oes = oes
            .into_iter()
            .filter_map(|de| {
                let name = de.name;
                let parent_path = de.parent_reference.path;
                let parent_path = parent_path
                    .strip_prefix(Self::DRIVE_ROOT_PREFIX)
                    .unwrap_or("");
                let path = format!("{}/{}", parent_path, name);
                debug_assert!(
                    path == self.path,
                    "path: {} must equals self.path: {}",
                    path,
                    self.path
                );

                let normalized_path = build_rel_path(&self.root, &path);

                let entry = match de.item_type {
                    ItemType::Folder { .. } => {
                        oio::Entry::new(&normalized_path, Metadata::new(EntryMode::DIR))
                    }
                    ItemType::File { .. } => {
                        oio::Entry::new(&normalized_path, Metadata::new(EntryMode::FILE))
                    }
                };

                Some(entry)
            })
            .collect();

        Ok(Some(oes))
    }
}
