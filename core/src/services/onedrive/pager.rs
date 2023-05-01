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

use crate::{
    raw::{
        build_rel_path, build_rooted_abs_path, new_json_deserialize_error, new_request_build_error,
        oio::{self},
        percent_encode_path, AsyncBody, HttpClient, IncomingAsyncBody,
    },
    EntryMode, Metadata,
};

use super::{
    error::parse_error,
    graph_model::{GraphApiOnedriveListResponse, ItemType},
};
use crate::Result;
use async_trait::async_trait;
use http::{header, Request, Response};
use log::debug;

#[async_trait]
pub(crate) trait OnedrivePagerTokenProvider {
    async fn get_access_token(&self) -> Result<String>;
}

pub struct OnedrivePager {
    root: String,
    path: String,
    access_token: String,
    client: HttpClient,
    next_link: Option<String>,
    done: bool,
}

impl OnedrivePager {
    const DRIVE_ROOT_PREFIX: &'static str = "/drive/root:";

    pub(crate) fn new(
        root: String,
        path: String,
        access_token: String,
        client: HttpClient,
    ) -> Self {
        Self {
            root: root,
            path: path,
            access_token: access_token,
            client,
            next_link: None,
            done: false,
        }
    }
}

#[async_trait]

impl oio::Page for OnedrivePager {
    async fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        if self.done {
            return Ok(None);
        }
        let response = self.onedrive_get().await?;

        let status_code = response.status();
        if !status_code.is_success() {
            let error = parse_error(response).await?;
            return Err(error);
        }

        let bytes = response.into_body().bytes().await?;
        let decoded_response = serde_json::from_slice::<GraphApiOnedriveListResponse>(&bytes)
            .map_err(new_json_deserialize_error)?;

        if let Some(next_link) = decoded_response.next_link {
            self.next_link = Some(next_link);
        } else {
            self.done = true;
        }

        let entries = decoded_response
            .value
            .into_iter()
            .filter_map(|drive_item| {
                let name = drive_item.name;
                let parent_path = drive_item.parent_reference.path;
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

                let entry = match drive_item.item_type {
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

        Ok(Some(entries))
    }
}

impl OnedrivePager {
    async fn onedrive_get(&mut self) -> Result<Response<IncomingAsyncBody>> {
        let request_url = if let Some(next_link) = &self.next_link {
            let next_link_clone = next_link.clone();
            self.next_link = None;
            next_link_clone
        } else {
            self.build_request_url()
        };

        debug!("request_url: {}", request_url);
        println!("request_url: {}", request_url);
        dbg!(request_url.clone());
        let mut req = Request::get(&request_url);

        let auth_header_content = format!("Bearer {}", self.access_token);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    fn build_request_url(&self) -> String {
        let path = build_rooted_abs_path(&self.root, &self.path);
        let url: String = if path == "." || path == "/" {
            "https://graph.microsoft.com/v1.0/me/drive/root/children".to_string()
        } else {
            format!(
                "https://graph.microsoft.com/v1.0/me/drive/root:{}:/content",
                percent_encode_path(&path),
            )
        };
        url
    }
}
