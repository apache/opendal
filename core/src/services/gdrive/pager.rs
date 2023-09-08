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

use std::sync::Arc;

use crate::{
    raw::{
        build_rel_path, build_rooted_abs_path, new_json_deserialize_error,
        oio::{self},
    },
    EntryMode, Metadata, Result,
};
use async_trait::async_trait;
use http::StatusCode;

use super::{
    core::{GdriveCore, GdriveFileList},
    error::parse_error,
};
pub struct GdrivePager {
    root: String,
    path: String,
    core: Arc<GdriveCore>,
    next_page_token: Option<String>,
    done: bool,
}

impl GdrivePager {
    pub fn new(root: String, path: String, core: Arc<GdriveCore>) -> Self {
        Self {
            root,
            path,
            core,
            next_page_token: None,
            done: false,
        }
    }
}

#[async_trait]
impl oio::Page for GdrivePager {
    async fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        if self.done {
            return Ok(None);
        }

        let resp = self
            .core
            .gdrive_list(&self.path, 100, self.next_page_token.clone())
            .await?;

        match resp.status() {
            StatusCode::OK => {
                let bytes = resp.into_body().bytes().await?;
                let decoded_response = serde_json::from_slice::<GdriveFileList>(&bytes)
                    .map_err(new_json_deserialize_error)?;

                if let Some(next_page_token) = decoded_response.next_page_token {
                    self.next_page_token = Some(next_page_token);
                } else {
                    self.done = true;
                }

                let entries: Vec<oio::Entry> = decoded_response
                    .files
                    .into_iter()
                    .map(|mut file| {
                        let file_type =
                            if file.mime_type.as_str() == "application/vnd.google-apps.folder" {
                                file.name = format!("{}/", file.name);
                                EntryMode::DIR
                            } else {
                                EntryMode::FILE
                            };

                        let path = format!(
                            "{}{}",
                            build_rooted_abs_path(&self.root, &self.path),
                            file.name
                        );
                        let normalized_path = build_rel_path(&self.root, &path);
                        oio::Entry::new(&normalized_path, Metadata::new(file_type))
                    })
                    .collect();

                Ok(Some(entries))
            }
            _ => Err(parse_error(resp).await?),
        }
    }
}
