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

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::raw::HttpClient;
use crate::Error;
use crate::ErrorKind;
use crate::Scheme;

use http::request::Builder;
use http::{header, Request, Response};
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{
    raw::{build_rooted_abs_path, new_request_build_error, AsyncBody, IncomingAsyncBody},
    types::Result,
};

pub struct GdriveCore {
    pub root: String,
    pub access_token: String,
    pub client: HttpClient,
    pub path_cache: Arc<Mutex<HashMap<String, String>>>,
}

impl Debug for GdriveCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("GdriveCore");
        de.field("root", &self.root);
        de.finish()
    }
}

impl GdriveCore {
    async fn get_abs_root_id(&self) -> Result<String> {
        let root = "root";

        if let Some(root_id) = self.path_cache.lock().await.get(root) {
            return Ok(root_id.to_string());
        }

        let req = self
            .sign(Request::get(
                "https://www.googleapis.com/drive/v3/files/root",
            ))
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        let resp = self.client.send(req).await?;
        let resp_body = &resp.into_body().bytes().await.map_err(|e| {
            Error::new(ErrorKind::Unexpected, "read response body error")
                .with_context("service", Scheme::Gdrive)
                .set_source(e)
        })?;

        let gdrive_file: GdriveFile = serde_json::from_slice(resp_body).map_err(|e| {
            Error::new(ErrorKind::Unexpected, "deserialize json error")
                .with_context("service", Scheme::Gdrive)
                .with_context("origin json value", String::from_utf8_lossy(resp_body))
                .set_source(e)
        })?;

        let root_id = gdrive_file.id;

        let mut cache_guard = self.path_cache.lock().await;
        cache_guard.insert(root.to_owned(), root_id.clone());

        Ok(root_id)
    }

    async fn get_file_id_by_path(&self, file_path: &str) -> Result<String> {
        let path = build_rooted_abs_path(&self.root, file_path);

        if let Some(file_id) = self.path_cache.lock().await.get(&path) {
            return Ok(file_id.to_string());
        }

        let mut parent_id = self.get_abs_root_id().await?;
        let file_path_items: Vec<&str> = path.split('/').filter(|&x| !x.is_empty()).collect();

        for (i, item) in file_path_items.iter().enumerate() {
            let mut query = format!(
                "name = '{}' and parents = '{}' and trashed = false",
                item, parent_id
            );
            if i != file_path_items.len() - 1 {
                query += "and mimeType = 'application/vnd.google-apps.folder'";
            }
            let query: String = query.chars().filter(|c| !c.is_whitespace()).collect();

            let req = self
                .sign(Request::get(format!(
                    "https://www.googleapis.com/drive/v3/files?q={}",
                    query
                )))
                .body(AsyncBody::default())
                .map_err(new_request_build_error)?;

            let resp = self.client.send(req).await?;
            let resp_body = &resp.into_body().bytes().await.map_err(|e| {
                Error::new(ErrorKind::Unexpected, "read response body error")
                    .with_context("service", Scheme::Gdrive)
                    .set_source(e)
            })?;

            let gdrive_file_list: GdriveFileList =
                serde_json::from_slice(resp_body).map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "deserialize json error")
                        .with_context("service", Scheme::Gdrive)
                        .with_context("origin json value", String::from_utf8_lossy(resp_body))
                        .set_source(e)
                })?;

            if gdrive_file_list.files.len() != 1 {
                return Err(Error::new(ErrorKind::Unexpected, &format!("Please ensure that the file corresponding to the path exists and is unique. The response body is {}", String::from_utf8_lossy(resp_body))));
            }

            parent_id = gdrive_file_list.files[0].id.clone();
        }

        let mut cache_guard = self.path_cache.lock().await;
        cache_guard.insert(path, parent_id.clone());

        Ok(parent_id)
    }

    pub async fn gdrive_get(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let url: String = format!(
            "https://www.googleapis.com/drive/v3/files/{}?alt=media",
            self.get_file_id_by_path(path).await?
        );

        let auth_header_content = format!("Bearer {}", self.access_token);
        let mut req = Request::get(&url);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    pub async fn gdrive_update(
        &self,
        path: &str,
        size: Option<usize>,
        content_type: Option<&str>,
        body: AsyncBody,
    ) -> Result<Response<IncomingAsyncBody>> {
        let url = format!(
            "https://www.googleapis.com/upload/drive/v3/files/{}",
            self.get_file_id_by_path(path).await?
        );

        let mut req = Request::patch(&url);

        let auth_header_content = format!("Bearer {}", self.access_token);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        if let Some(size) = size {
            req = req.header(header::CONTENT_LENGTH, size)
        }

        if let Some(mime) = content_type {
            req = req.header(header::CONTENT_TYPE, mime)
        }

        let req = req.body(body).map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    pub async fn gdrive_delete(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let url = format!(
            "https://www.googleapis.com/drive/v3/files/{}",
            self.get_file_id_by_path(path).await?
        );

        let mut req = Request::delete(&url);

        let auth_header_content = format!("Bearer {}", self.access_token);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    fn sign(&self, mut req: Builder) -> Builder {
        let auth_header_content = format!("Bearer {}", self.access_token);
        req = req.header(header::AUTHORIZATION, auth_header_content);
        req
    }
}

// refer to https://developers.google.com/drive/api/reference/rest/v3/files#File
#[derive(Deserialize)]
struct GdriveFile {
    id: String,
}

// refer to https://developers.google.com/drive/api/reference/rest/v3/files/list
#[derive(Deserialize)]
struct GdriveFileList {
    files: Vec<GdriveFile>,
}
