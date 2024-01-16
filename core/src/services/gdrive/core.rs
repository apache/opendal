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
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use bytes;
use bytes::Bytes;
use chrono::DateTime;
use chrono::Utc;
use http::header;
use http::Request;
use http::Response;
use http::StatusCode;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::Mutex;

use super::error::parse_error;
use crate::raw::*;
use crate::types::Result;
use crate::Error;
use crate::ErrorKind;

pub struct GdriveCore {
    pub root: String,

    pub client: HttpClient,

    pub signer: Arc<Mutex<GdriveSigner>>,

    /// Cache the mapping from path to file id
    pub path_cache: PathCacher<GdrivePathQuery>,
}

impl Debug for GdriveCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("GdriveCore");
        de.field("root", &self.root);
        de.finish()
    }
}

impl GdriveCore {
    /// Ensure the parent path exists.
    /// If the parent path does not exist, create it.
    ///
    /// # Notes
    ///
    /// - The path is rooted at the root of the Google Drive.
    /// - Will create the parent path recursively.
    pub(crate) async fn ensure_parent_path(&self, path: &str) -> Result<String> {
        let path = build_abs_path(&self.root, path);
        let current_path = if path.ends_with('/') {
            path.as_str()
        } else {
            get_parent(&path)
        };

        let components = current_path.split('/').collect::<Vec<_>>();
        let mut tmp = "".to_string();
        // All parents that need to check.
        let mut parents = vec![];
        for component in components {
            tmp.push_str(component);
            tmp.push('/');
            parents.push(tmp.to_string());
        }

        let mut parent_id = self
            .path_cache
            .get("/")
            .await?
            .expect("the id for root must exist");
        for parent in parents {
            parent_id = match self.path_cache.get(&parent).await? {
                Some(value) => value,
                None => {
                    let value = self
                        .gdrive_create_folder(&parent_id, get_basename(&parent))
                        .await?;
                    self.path_cache.insert(&parent, &value).await;
                    value
                }
            }
        }

        Ok(parent_id)
    }

    /// Create a folder.
    ///
    /// # Input
    ///
    /// `parent_id` is the parent folder id.
    ///
    /// # Output
    ///
    /// Returns created folder's id while success, otherwise returns an error.
    pub async fn gdrive_create_folder(&self, parent_id: &str, name: &str) -> Result<String> {
        let url = "https://www.googleapis.com/drive/v3/files";

        let content = serde_json::to_vec(&json!({
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
            // If the parent is not provided, the folder will be created in the root folder.
            "parents": [parent_id],
        }))
        .map_err(new_json_serialize_error)?;

        let mut req = Request::post(url)
            .header(header::CONTENT_TYPE, "application/json")
            .body(AsyncBody::Bytes(Bytes::from(content)))
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;

        let resp = self.client.send(req).await?;
        if !resp.status().is_success() {
            return Err(parse_error(resp).await?);
        }

        let body = resp.into_body().bytes().await?;
        let file: GdriveFile = serde_json::from_slice(&body).map_err(new_json_deserialize_error)?;
        Ok(file.id)
    }

    pub async fn gdrive_stat(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let path = build_abs_path(&self.root, path);
        let path_id = self.path_cache.get(&path).await?.ok_or(Error::new(
            ErrorKind::NotFound,
            &format!("path not found: {}", path),
        ))?;

        // The file metadata in the Google Drive API is very complex.
        // For now, we only need the file id, name, mime type and modified time.
        let mut req = Request::get(&format!(
            "https://www.googleapis.com/drive/v3/files/{}?fields=id,name,mimeType,size,modifiedTime",
            path_id
        ))
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;
        self.sign(&mut req).await?;

        self.client.send(req).await
    }

    pub async fn gdrive_get(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let path = build_abs_path(&self.root, path);
        let path_id = self.path_cache.get(&path).await?.ok_or(Error::new(
            ErrorKind::NotFound,
            &format!("path not found: {}", path),
        ))?;

        let url: String = format!(
            "https://www.googleapis.com/drive/v3/files/{}?alt=media",
            path_id
        );

        let mut req = Request::get(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;
        self.sign(&mut req).await?;

        self.client.send(req).await
    }

    pub async fn gdrive_list(
        &self,
        file_id: &str,
        page_size: i32,
        next_page_token: &str,
    ) -> Result<Response<IncomingAsyncBody>> {
        let q = format!("'{}' in parents and trashed = false", file_id);
        let mut url = format!(
            "https://www.googleapis.com/drive/v3/files?pageSize={}&q={}",
            page_size,
            percent_encode_path(&q)
        );
        if !next_page_token.is_empty() {
            url += &format!("&pageToken={next_page_token}");
        };

        let mut req = Request::get(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;
        self.sign(&mut req).await?;

        self.client.send(req).await
    }

    // Update with content and metadata
    pub async fn gdrive_patch_metadata_request(
        &self,
        source: &str,
        target: &str,
    ) -> Result<Response<IncomingAsyncBody>> {
        let source = build_abs_path(&self.root, source);
        let target = build_abs_path(&self.root, target);

        let file_id = self.path_cache.get(&source).await?.ok_or(Error::new(
            ErrorKind::NotFound,
            &format!("source path not found: {}", source),
        ))?;
        let parent_id = self.ensure_parent_path(&target).await?;
        let file_name = get_basename(&target);

        let source_parent = get_parent(&source);
        let source_parent_id = self
            .path_cache
            .get(source_parent)
            .await?
            .expect("old parent must exist");
        let metadata = &json!({
            "name": file_name,
            "removeParents": [source_parent_id],
            "addParents": [parent_id],
        });

        let url = format!("https://www.googleapis.com/drive/v3/files/{}", file_id);
        let mut req = Request::patch(url)
            .body(AsyncBody::Bytes(Bytes::from(metadata.to_string())))
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;

        self.client.send(req).await
    }

    pub async fn gdrive_delete(&self, file_id: &str) -> Result<Response<IncomingAsyncBody>> {
        let url = format!("https://www.googleapis.com/drive/v3/files/{}", file_id);

        let mut req = Request::delete(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;

        self.client.send(req).await
    }

    /// Create a file with the content.
    pub async fn gdrive_upload_simple_request(
        &self,
        path: &str,
        size: u64,
        body: Bytes,
    ) -> Result<Response<IncomingAsyncBody>> {
        let parent = self.ensure_parent_path(path).await?;

        let url = "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart";

        let file_name = get_basename(path);

        let metadata = &json!({
            "name": file_name,
            "parents": [parent],
        });

        let req = Request::post(url).header("X-Upload-Content-Length", size);

        let multipart = Multipart::new()
            .part(
                FormDataPart::new("metadata")
                    .header(
                        header::CONTENT_TYPE,
                        "application/json; charset=UTF-8".parse().unwrap(),
                    )
                    .content(metadata.to_string()),
            )
            .part(
                FormDataPart::new("file")
                    .header(
                        header::CONTENT_TYPE,
                        "application/octet-stream".parse().unwrap(),
                    )
                    .content(body),
            );

        let mut req = multipart.apply(req)?;

        self.sign(&mut req).await?;

        self.client.send(req).await
    }

    /// Overwrite the file with the content.
    ///
    /// # Notes
    ///
    /// - The file id is required. Do not use this method to create a file.
    pub async fn gdrive_upload_overwrite_simple_request(
        &self,
        file_id: &str,
        size: u64,
        body: Bytes,
    ) -> Result<Response<IncomingAsyncBody>> {
        let url = format!(
            "https://www.googleapis.com/upload/drive/v3/files/{}?uploadType=media",
            file_id
        );

        let mut req = Request::patch(url)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(header::CONTENT_LENGTH, size)
            .header("X-Upload-Content-Length", size)
            .body(AsyncBody::Bytes(body))
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;

        self.client.send(req).await
    }

    pub async fn sign<T>(&self, req: &mut Request<T>) -> Result<()> {
        let mut signer = self.signer.lock().await;
        signer.sign(req).await
    }
}

#[derive(Clone)]
pub struct GdriveSigner {
    pub client: HttpClient,

    pub client_id: String,
    pub client_secret: String,
    pub refresh_token: String,

    pub access_token: String,
    pub expires_in: DateTime<Utc>,
}

impl GdriveSigner {
    /// Create a new signer.
    pub fn new(client: HttpClient) -> Self {
        GdriveSigner {
            client,

            client_id: "".to_string(),
            client_secret: "".to_string(),
            refresh_token: "".to_string(),
            access_token: "".to_string(),
            expires_in: DateTime::<Utc>::MIN_UTC,
        }
    }

    /// Sign a request.
    pub async fn sign<T>(&mut self, req: &mut Request<T>) -> Result<()> {
        if !self.access_token.is_empty() && self.expires_in > Utc::now() {
            let value = format!("Bearer {}", self.access_token)
                .parse()
                .expect("access token must be valid header value");

            req.headers_mut().insert(header::AUTHORIZATION, value);
            return Ok(());
        }

        let url = format!(
            "https://oauth2.googleapis.com/token?refresh_token={}&client_id={}&client_secret={}&grant_type=refresh_token",
            self.refresh_token, self.client_id, self.client_secret
        );

        {
            let req = Request::post(url)
                .body(AsyncBody::Empty)
                .map_err(new_request_build_error)?;

            let resp = self.client.send(req).await?;
            let status = resp.status();

            match status {
                StatusCode::OK => {
                    let resp_body = &resp.into_body().bytes().await?;
                    let token = serde_json::from_slice::<GdriveTokenResponse>(resp_body)
                        .map_err(new_json_deserialize_error)?;
                    self.access_token = token.access_token.clone();
                    self.expires_in = Utc::now() + chrono::Duration::seconds(token.expires_in)
                        - chrono::Duration::seconds(120);
                }
                _ => {
                    return Err(parse_error(resp).await?);
                }
            }
        }

        let auth_header_content = format!("Bearer {}", self.access_token);
        req.headers_mut()
            .insert(header::AUTHORIZATION, auth_header_content.parse().unwrap());

        Ok(())
    }
}

pub struct GdrivePathQuery {
    pub client: HttpClient,
    pub signer: Arc<Mutex<GdriveSigner>>,
}

impl GdrivePathQuery {
    pub fn new(client: HttpClient, signer: Arc<Mutex<GdriveSigner>>) -> Self {
        GdrivePathQuery { client, signer }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl PathQuery for GdrivePathQuery {
    async fn root(&self) -> Result<String> {
        Ok("root".to_string())
    }

    async fn query(&self, parent_id: &str, name: &str) -> Result<Option<String>> {
        let mut queries = vec![
            // Make sure name has been replaced with escaped name.
            //
            // ref: <https://developers.google.com/drive/api/guides/ref-search-terms>
            format!("name = '{}'", name.replace('\'', "\\'")),
            format!("'{}' in parents", parent_id),
            "trashed = false".to_string(),
        ];
        if name.ends_with('/') {
            queries.push("mimeType = 'application/vnd.google-apps.folder'".to_string());
        }
        let query = queries.join(" and ");

        let url = format!(
            "https://www.googleapis.com/drive/v3/files?q={}",
            percent_encode_path(query.as_str())
        );

        let mut req = Request::get(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.signer.lock().await.sign(&mut req).await?;

        let resp = self.client.send(req).await?;
        let status = resp.status();

        match status {
            StatusCode::OK => {
                let body = resp.into_body().bytes().await?;
                let meta: GdriveFileList =
                    serde_json::from_slice(&body).map_err(new_json_deserialize_error)?;

                if let Some(f) = meta.files.first() {
                    Ok(Some(f.id.clone()))
                } else {
                    Ok(None)
                }
            }
            _ => Err(parse_error(resp).await?),
        }
    }
}

#[derive(Deserialize)]
pub struct GdriveTokenResponse {
    access_token: String,
    expires_in: i64,
}

// This is the file struct returned by the Google Drive API.
// This is a complex struct, but we only add the fields we need.
// refer to https://developers.google.com/drive/api/reference/rest/v3/files#File
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct GdriveFile {
    pub mime_type: String,
    pub id: String,
    pub name: String,
    pub size: Option<String>,
    // The modified time is not returned unless the `fields`
    // query parameter contains `modifiedTime`.
    // As we only need the modified time when we do `stat` operation,
    // if other operations(such as search) do not specify the `fields` query parameter,
    // try to access this field, it will be `None`.
    pub modified_time: Option<String>,
}

// refer to https://developers.google.com/drive/api/reference/rest/v3/files/list
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GdriveFileList {
    pub(crate) files: Vec<GdriveFile>,
    pub(crate) next_page_token: Option<String>,
}
