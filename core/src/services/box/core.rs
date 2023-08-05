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
use std::default::Default;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use chrono::DateTime;
use chrono::Utc;
use http::header;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::Request;
use http::Response;
use http::StatusCode;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Mutex;

use crate::raw::new_json_deserialize_error;
use crate::raw::new_json_serialize_error;
use crate::raw::new_request_build_error;
use crate::raw::AsyncBody;
use crate::raw::BatchedReply;
use crate::raw::HttpClient;
use crate::raw::IncomingAsyncBody;
use crate::raw::RpBatch;
use crate::raw::RpDelete;
use crate::raw::{build_rooted_abs_path, percent_encode_path};
use crate::types::Result;
use crate::Error;
use crate::ErrorKind;

pub struct BoxCore {
    pub signer: Arc<Mutex<BoxSigner>>,
    pub client: HttpClient,
    pub root: String,
    pub path_cache: Arc<Mutex<HashMap<String, String>>>,
}

impl Debug for BoxCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("BoxCore");
        de.field("root", &self.root);
        de.finish()
    }
}

impl BoxCore {
    fn truncate_filename(&self, full_path: &str) -> String {
        let last_slash_idx = match full_path.rfind('/') {
            Some(idx) => idx,
            None => return "".to_string(), // If no slash is found, return an empty string
        };

        let truncated_path = &full_path[0..last_slash_idx];
        truncated_path.to_string()
    }
    async fn get_abs_root_id(&self) -> Result<String> {
        let root = self.root.clone();

        // The root folder of a Box account is always represented by the ID 0
        if self.root == "/" {
            return Ok("0".to_string());
        }

        if let Some(root_id) = self.path_cache.lock().await.get(&root) {
            return Ok(root_id.to_string());
        }

        let mut parent_id = "0".to_string();
        let file_path_items: Vec<&str> = root.split('/').filter(|&x| !x.is_empty()).collect();
        for (i, item_name) in file_path_items.iter().enumerate() {
            let url = format!("https://api.box.com/2.0/folders/{}/items", parent_id).to_string();
            let mut request = Request::get(&url)
                .header(CONTENT_TYPE, "application/json")
                .body(AsyncBody::Empty)
                .map_err(new_request_build_error)?;

            self.sign(&mut request).await?;
            let resp = self.client.send(req).await?;
            let status = resp.status();

            match status {
                StatusCode::OK => {
                    let resp_body = &resp.into_body().bytes().await?;

                    let result: BoxListResponse =
                        serde_json::from_slice(resp_body).map_err(new_json_deserialize_error)?;
                    let target_type = "folder".to_string();
                    let target_name = item_name.to_string();

                    if let Some(entry) = result
                        .entries
                        .iter()
                        .find(|entry| entry.item_type == target_type && entry.name == target_name)
                    {
                        parent_id = entry.id.clone();
                    } else {
                        return Err(Error::new(ErrorKind::Unexpected, &format!("Can't find root path, Please ensure that the root path exists and is unique." )));
                    }
                }
                _ => Err(parse_error(resp).await?),
            }
        }
        let mut cache_guard = self.path_cache.lock().await;
        cache_guard.insert(root, parent_id.clone());

        Ok(parent_id)
    }

    async fn get_file_id_by_path(&self, file_path: &str) -> Result<String> {
        let path = build_rooted_abs_path(&self.root, file_path).trim_end_matches('/').to_string();
        if let Some(file_id) = self.path_cache.lock().await.get(&path) {
            return Ok(file_id.to_string());
        }

        let file_path_items: Vec<&str> = file_path.split('/').filter(|&x| !x.is_empty()).collect();
        let mut parent_id = self.get_abs_root_id().await?;
        for (i, item_name) in file_path_items.iter().enumerate() {
            let url = format!("https://api.box.com/2.0/folders/{}/items", parent_id).to_string();
            let mut request = Request::get(&url)
                .header(CONTENT_TYPE, "application/json")
                .body(AsyncBody::Empty)
                .map_err(new_request_build_error)?;

            self.sign(&mut request).await?;
            let resp = self.client.send(req).await?;
            let status = resp.status();

            match status {
                StatusCode::OK => {
                    let resp_body = &resp.into_body().bytes().await?;

                    let result: BoxListResponse =
                        serde_json::from_slice(resp_body).map_err(new_json_deserialize_error)?;
                    let target_type = if i != file_path_items.len() - 1 {
                        "folder"
                    } else {
                        "file"
                    }
                    .to_string();
                    let target_name = item_name.to_string();

                    if let Some(entry) = result
                        .entries
                        .iter()
                        .find(|entry| entry.item_type == target_type && entry.name == target_name)
                    {
                        parent_id = entry.id.clone();
                    } else {
                        return Err(Error::new(ErrorKind::Unexpected, &format!("Can't find given file, Please ensure that the file path exists and is unique." )));
                    }
                }
                _ => Err(parse_error(resp).await?),
            }
        }
        let mut cache_guard = self.path_cache.lock().await;
        cache_guard.insert(path, parent_id.clone());

        Ok(parent_id)
    }
    async fn get_folder_id_by_path(&self, folder_path: &str) -> Result<String> {
        let path = build_rooted_abs_path(&self.root, folder_path).trim_end_matches('/').to_string();
        if let Some(folder_id) = self.path_cache.lock().await.get(&path) {
            return Ok(folder_id.to_string());
        }

        let folder_path_items: Vec<&str> = folder_path.split('/').filter(|&x| !x.is_empty()).collect();
        let mut parent_id = self.get_abs_root_id().await?;
        for (i, item_name) in folder_path_items.iter().enumerate() {
            let url = format!("https://api.box.com/2.0/folders/{}/items", parent_id).to_string();
            let mut request = Request::get(&url)
                .header(CONTENT_TYPE, "application/json")
                .body(AsyncBody::Empty)
                .map_err(new_request_build_error)?;

            self.sign(&mut request).await?;
            let resp = self.client.send(req).await?;
            let status = resp.status();

            match status {
                StatusCode::OK => {
                    let resp_body = &resp.into_body().bytes().await?;

                    let result: BoxListResponse =
                        serde_json::from_slice(resp_body).map_err(new_json_deserialize_error)?;
                    let target_type = "folder".to_string();
                    let target_name = item_name.to_string();

                    if let Some(entry) = result
                        .entries
                        .iter()
                        .find(|entry| entry.item_type == target_type && entry.name == target_name)
                    {
                        parent_id = entry.id.clone();
                    } else {
                        return Err(Error::new(ErrorKind::Unexpected, &format!("Can't find given folder, Please ensure that the folder path exists and is unique." )));
                    }
                }
                _ => Err(parse_error(resp).await?),
            }
        }
        let mut cache_guard = self.path_cache.lock().await;
        cache_guard.insert(path, parent_id.clone());

        Ok(parent_id)
    }
    pub async fn box_get(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let url: String = format!(
            "https://api.box.com/2.0/files/{}/content",
            self.get_file_id_by_path(path).await?
        );
        let mut request = Request::get(&url)
            .header(CONTENT_TYPE, "application/json")
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.sign(&mut request).await?;
        self.client.send(request).await
    }

    pub async fn box_upload(
        &self,
        path: &str,
        size: Option<usize>,
        content_type: Option<&str>,
        body: AsyncBody,
    ) -> Result<Response<IncomingAsyncBody>> {
        let folder_path=self.truncate_filename(path.trim_end_matches('/'));
        let file_name="test.txt";
        let url: String = "https://upload.box.com/api/2.0/files/content".to_string();
        let mut req = Request::post(&url);
        req = req.header(CONTENT_TYPE, "multipart/form-data; boundary=my-boundary");
        let mut req_body = BytesMut::with_capacity(100);
        write!(
            &mut req_body,
            "--my-boundary\ncontent-disposition: form-data; name=\"attributes\"\n\n{{\"name\":\"{}\", \"parent\":{{\"id\":\"{}\"}}}}\n--my-boundary\n",
            file_name,
            folder_path// should be folder id
        ).unwrap();

        write!(
            &mut req_body,
            "content-disposition: form-data; name=\"file\"; filename=\"{}\"\n",
            file_name,
        ).unwrap();
        if let Some(mime) = content_type {
            write!(&mut req_body, "Content-Type: {}\n\n", mime).unwrap();
        } else {
            write!(&mut req_body, "Content-Type: application/octet-stream\n\n").unwrap();
        }
        if let AsyncBody::Bytes(bytes) = body {
            req_body.extend_from_slice(&bytes);
        }
        write!(&mut req_body, "\n--my-boundary").unwrap();
        let req_body = AsyncBody::Bytes(req_body.freeze());
        let mut request = req.body(req_body).map_err(new_request_build_error)?;


        self.sign(&mut request).await?;
        self.client.send(request).await
    }


    pub async fn sign<T>(&self, req: &mut Request<T>) -> Result<()> {
        let mut signer = self.signer.lock().await;

        // Access token is valid, use it directly.
        if !signer.access_token.is_empty() && signer.expires_in > Utc::now() {
            let value = format!("Bearer {}", signer.access_token)
                .parse()
                .expect("token must be valid header value");
            req.headers_mut().insert(header::AUTHORIZATION, value);
            return Ok(());
        }

        // Refresh invalid token.
        let url = "https://api.box.com/oauth2/token".to_string();

        let content = format!(
            "grant_type=refresh_token&refresh_token={}&client_id={}&client_secret={}",
            signer.refresh_token, signer.client_id, signer.client_secret
        );
        let bs = Bytes::from(content);

        let request = Request::post(&url)
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header(CONTENT_LENGTH, bs.len())
            .body(AsyncBody::Bytes(bs))
            .map_err(new_request_build_error)?;

        let resp = self.client.send(request).await?;
        let body = resp.into_body().bytes().await?;

        let token: BoxTokenResponse =
            serde_json::from_slice(&body).map_err(new_json_deserialize_error)?;

        // Update signer after token refreshed.
        signer.access_token = token.access_token.clone();

        // A Refresh Token is valid for 60 days.
        // which can be used to obtain a new Access Token and Refresh Token only once.
        // After used or not refreshed within 60 days, the refresh token is invalidated.
        // Reference: https://developer.box.com/guides/authentication/tokens/refresh/
        signer.refresh_token = token.refresh_token.clone();

        // Refresh it 2 minutes earlier like DropBox.
        signer.expires_in = Utc::now() + chrono::Duration::seconds(token.expires_in as i64)
            - chrono::Duration::seconds(120);

        let value = format!("Bearer {}", token.access_token)
            .parse()
            .expect("token must be valid header value");
        req.headers_mut().insert(header::AUTHORIZATION, value);

        Ok(())
    }
}

#[derive(Clone)]
pub struct BoxSigner {
    pub client_id: String,
    pub client_secret: String,
    pub refresh_token: String,
    pub access_token: String,
    pub expires_in: DateTime<Utc>,
}

impl Default for BoxSigner {
    fn default() -> Self {
        BoxSigner {
            refresh_token: "".to_string(),
            client_id: String::new(),
            client_secret: String::new(),
            access_token: "".to_string(),
            expires_in: DateTime::<Utc>::MIN_UTC,
        }
    }
}


#[derive(Clone, Deserialize)]
struct BoxTokenResponse {
    access_token: String,
    expires_in: usize,
    refresh_token: String,
}
#[derive(Clone, Debug, Deserialize)]
struct BoxListItem {
    #[serde(rename = "type")]
    item_type: String,
    name: String,
    id: String,
}

#[derive(Clone, Debug, Deserialize)]
struct BoxListResponse {
    entries: Vec<BoxListItem>,
}
