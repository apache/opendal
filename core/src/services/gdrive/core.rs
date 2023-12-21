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

use bytes;
use bytes::Bytes;
use chrono::DateTime;
use chrono::Utc;
use futures::stream;
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
    ///
    /// Google Drive uses file id to identify a file.
    /// As the path is immutable, we can cache the mapping from path to file id.
    ///
    /// # Notes
    ///
    /// - The path is rooted at the root of the Google Drive.
    /// - The path is absolute path, like `foo/bar`.
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
    /// Get the file id by path.
    /// Including file and folder.
    ///
    /// The path is rooted at the root of the Google Drive.
    ///
    /// # Notes
    ///
    /// - A path is a sequence of file names separated by slashes.
    /// - A file only knows its parent id, but not its name.
    /// - To find the file id of a file, we need to traverse the path from the root to the file.
    pub(crate) async fn get_file_id_by_path(&self, file_path: &str) -> Result<Option<String>> {
        let path = build_abs_path(&self.root, file_path);

        let mut cache = self.path_cache.lock().await;

        if let Some(id) = cache.get(&path) {
            return Ok(Some(id.to_owned()));
        }

        let mut parent_id = "root".to_owned();
        let file_path_items: Vec<&str> = path.split('/').filter(|&x| !x.is_empty()).collect();

        for (i, item) in file_path_items.iter().enumerate() {
            let path_part = file_path_items[0..=i].join("/");
            if let Some(id) = cache.get(&path_part) {
                parent_id = id.to_owned();
                continue;
            }

            let id = if i != file_path_items.len() - 1 || path.ends_with('/') {
                self.gdrive_search_folder(&parent_id, item).await?
            } else {
                self.gdrive_search_file(&parent_id, item)
                    .await?
                    .map(|v| v.id)
            };

            if let Some(id) = id {
                parent_id = id;
                cache.insert(path_part, parent_id.clone());
            } else {
                return Ok(None);
            };
        }

        Ok(Some(parent_id))
    }

    /// Ensure the parent path exists.
    /// If the parent path does not exist, create it.
    ///
    /// # Notes
    ///
    /// - The path is rooted at the root of the Google Drive.
    /// - Will create the parent path recursively.
    pub(crate) async fn ensure_parent_path(&self, path: &str) -> Result<String> {
        let path = build_abs_path(&self.root, path);

        let mut parent: String = "root".to_owned();
        let mut file_path_items: Vec<&str> = path.split('/').filter(|&x| !x.is_empty()).collect();
        file_path_items.pop();

        let mut cache = self.path_cache.lock().await;

        for (i, item) in file_path_items.iter().enumerate() {
            let path_part = file_path_items[0..=i].join("/");
            if let Some(id) = cache.get(&path_part) {
                parent = id.to_owned();
                continue;
            }

            let folder_id = self.gdrive_search_folder(&parent, item).await?;
            let folder_id = if let Some(id) = folder_id {
                id
            } else {
                self.gdrive_create_folder(&parent, item).await?
            };

            parent = folder_id;
            cache.insert(path_part, parent.clone());
        }

        Ok(parent.to_owned())
    }

    /// Search a folder by name
    ///
    /// returns it's file id if exists, otherwise returns `None`.
    pub async fn gdrive_search_file(
        &self,
        parent: &str,
        basename: &str,
    ) -> Result<Option<GdriveFile>> {
        let query =
            format!("name = \"{basename}\" and \"{parent}\" in parents and trashed = false");
        let url = format!(
            "https://www.googleapis.com/drive/v3/files?q={}",
            percent_encode_path(&query)
        );

        let mut req = Request::get(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;

        let resp = self.client.send(req).await?;
        let status = resp.status();
        if !status.is_success() {
            return Err(parse_error(resp).await?);
        }

        let body = resp.into_body().bytes().await?;
        let mut file_list: GdriveFileList =
            serde_json::from_slice(&body).map_err(new_json_deserialize_error)?;

        if file_list.files.len() > 1 {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "please ensure that the file corresponding to the path is unique.",
            ));
        }

        Ok(file_list.files.pop())
    }

    /// Search a folder by name
    ///
    /// returns it's file id if exists, otherwise returns `None`.
    pub async fn gdrive_search_folder(
        &self,
        parent: &str,
        basename: &str,
    ) -> Result<Option<String>> {
        let query = format!(
            "name = \"{}\" and \"{}\" in parents and trashed = false and mimeType = 'application/vnd.google-apps.folder'",
            basename, parent
        );
        let url = format!(
            "https://www.googleapis.com/drive/v3/files?q={}",
            percent_encode_path(query.as_str())
        );

        let mut req = Request::get(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;

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
        let path_id = self.get_file_id_by_path(path).await?.ok_or(Error::new(
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
        let path_id = self.get_file_id_by_path(path).await?.ok_or(Error::new(
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
        path: &str,
        page_size: i32,
        next_page_token: &str,
    ) -> Result<Response<IncomingAsyncBody>> {
        let file_id = self.get_file_id_by_path(path).await;

        // when list over a no exist dir, we should return a empty list in this case.
        let q = match file_id {
            Ok(Some(file_id)) => {
                format!("'{}' in parents and trashed = false", file_id)
            }
            Ok(None) => {
                return Response::builder()
                    .status(StatusCode::OK)
                    .body(IncomingAsyncBody::new(
                        Box::new(oio::into_stream(stream::empty())),
                        Some(0),
                    ))
                    .map_err(|e| {
                        Error::new(
                            ErrorKind::Unexpected,
                            &format!("failed to create a empty response for list: {}", e),
                        )
                        .set_source(e)
                    });
            }
            Err(e) => {
                return Err(e);
            }
        };

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
        let file_id = self.get_file_id_by_path(source).await?.ok_or(Error::new(
            ErrorKind::NotFound,
            &format!("source path not found: {}", source),
        ))?;

        let parent = self.ensure_parent_path(target).await?;

        let url = format!("https://www.googleapis.com/drive/v3/files/{}", file_id);

        let source_abs_path = build_abs_path(&self.root, source);
        let mut source_parent: Vec<&str> = source_abs_path
            .split('/')
            .filter(|&x| !x.is_empty())
            .collect();
        source_parent.pop();

        let cache = self.path_cache.lock().await;

        let file_name = build_abs_path(&self.root, target)
            .split('/')
            .filter(|&x| !x.is_empty())
            .last()
            .unwrap()
            .to_string();

        let metadata = &json!({
            "name": file_name,
            "removeParents": [cache.get(&source_parent.join("/")).unwrap().to_string()],
            "addParents": [parent],
        });

        let mut req = Request::patch(url)
            .body(AsyncBody::Bytes(Bytes::from(metadata.to_string())))
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;

        self.client.send(req).await
    }

    pub async fn gdrive_delete(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let file_id = self.get_file_id_by_path(path).await?.ok_or(Error::new(
            ErrorKind::NotFound,
            &format!("path not found: {}", path),
        ))?;
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

        let file_name = path.split('/').filter(|&x| !x.is_empty()).last().unwrap();

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

        if !signer.access_token.is_empty() && signer.expires_in > Utc::now() {
            let value = format!("Bearer {}", signer.access_token)
                .parse()
                .expect("access token must be valid header value");

            req.headers_mut().insert(header::AUTHORIZATION, value);
            return Ok(());
        }

        let url = format!(
            "https://oauth2.googleapis.com/token?refresh_token={}&client_id={}&client_secret={}&grant_type=refresh_token",
            signer.refresh_token, signer.client_id, signer.client_secret
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
                    signer.access_token = token.access_token.clone();
                    signer.expires_in = Utc::now() + chrono::Duration::seconds(token.expires_in)
                        - chrono::Duration::seconds(120);
                }
                _ => {
                    return Err(parse_error(resp).await?);
                }
            }
        }

        let auth_header_content = format!("Bearer {}", signer.access_token);
        req.headers_mut()
            .insert(header::AUTHORIZATION, auth_header_content.parse().unwrap());

        Ok(())
    }
}

#[derive(Clone)]
pub struct GdriveSigner {
    pub client_id: String,
    pub client_secret: String,
    pub refresh_token: String,

    pub access_token: String,
    pub expires_in: DateTime<Utc>,
}

impl Default for GdriveSigner {
    fn default() -> Self {
        GdriveSigner {
            access_token: String::new(),
            expires_in: DateTime::<Utc>::MIN_UTC,

            refresh_token: String::new(),
            client_id: String::new(),
            client_secret: String::new(),
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
