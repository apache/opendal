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
use http::header;
use http::Request;
use http::Response;
use http::StatusCode;
use reqwest::multipart::Form;
use reqwest::multipart::Part;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::Mutex;

use super::error::parse_error;
use crate::raw::build_rooted_abs_path;
use crate::raw::new_json_deserialize_error;
use crate::raw::new_request_build_error;
use crate::raw::percent_encode_path;
use crate::raw::AsyncBody;
use crate::raw::HttpClient;
use crate::raw::IncomingAsyncBody;
use crate::types::Result;
use crate::Error;
use crate::ErrorKind;

pub struct GdriveCore {
    pub root: String,
    pub access_token: String,

    pub client: HttpClient,

    /// Cache the mapping from path to file id
    ///
    /// Google Drive uses file id to identify a file.
    /// As the path is immutable, we can cache the mapping from path to file id.
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
    async fn get_file_id_by_path(&self, file_path: &str) -> Result<String> {
        let path = build_rooted_abs_path(&self.root, file_path);

        println!("file path: {}", path);

        let mut parent_id = "root".to_owned();
        let file_path_items: Vec<&str> = path.split('/').filter(|&x| !x.is_empty()).collect();

        for (i, item) in file_path_items.iter().enumerate() {
            let mut query = format!(
                "name = \"{}\" and \"{}\" in parents and trashed = false",
                item, parent_id
            );
            if i != file_path_items.len() - 1 {
                query += " and mimeType = 'application/vnd.google-apps.folder'";
            }

            println!("query: {}", query);

            let req = self.sign(
                Request::get(format!(
                    "https://www.googleapis.com/drive/v3/files?q={}",
                    percent_encode_path(&query)
                ))
                .body(AsyncBody::default())
                .map_err(new_request_build_error)?,
            );

            let resp = self.client.send(req).await?;
            let status = resp.status();

            println!("status in: {:?}", status);

            match status {
                StatusCode::OK => {
                    let resp_body = &resp.into_body().bytes().await?;

                    let gdrive_file_list: GdriveFileList =
                        serde_json::from_slice(resp_body).map_err(new_json_deserialize_error)?;

                    println!("gdrive_file_list: {:?}", gdrive_file_list.files.len());

                    if gdrive_file_list.files.is_empty() {
                        return Err(Error::new(
                            ErrorKind::NotFound,
                            &format!("path not found: {}", item),
                        ));
                    }

                    if gdrive_file_list.files.len() > 1 {
                        return Err(Error::new(ErrorKind::Unexpected, &format!("please ensure that the file corresponding to the path exists and is unique. the response body is {}", String::from_utf8_lossy(resp_body))));
                    }

                    parent_id = gdrive_file_list.files[0].id.clone();
                }
                _ => {
                    return Err(parse_error(resp).await?);
                }
            }
        }

        Ok(parent_id)
    }

    /// Ensure the parent path exists.
    /// If the parent path does not exist, create it.
    ///
    /// # Notes
    ///
    /// - The path is rooted at the root of the Google Drive.
    /// - Will create the parent path recursively.
    async fn ensure_parent_path(&self, path: &str) -> Result<String> {
        let path = build_rooted_abs_path(&self.root, path);

        let mut parent: String = "root".to_owned();
        let mut file_path_items: Vec<&str> = path.split('/').filter(|&x| !x.is_empty()).collect();
        file_path_items.pop();

        for (i, item) in file_path_items.iter().enumerate() {
            let query = format!(
                "name = \"{}\" and \"{}\" in parents and trashed = false and mimeType = 'application/vnd.google-apps.folder'",
                item, parent
            );

            let req = self.sign(
                Request::get(format!(
                    "https://www.googleapis.com/drive/v3/files?q={}",
                    percent_encode_path(&query)
                ))
                .body(AsyncBody::Empty)
                .map_err(new_request_build_error)?,
            );

            let resp = self.client.send(req).await?;
            let status = resp.status();

            match status {
                StatusCode::OK => {
                    let resp_body = &resp.into_body().bytes().await?;

                    let gdrive_file_list: GdriveFileList =
                        serde_json::from_slice(resp_body).map_err(new_json_deserialize_error)?;

                    if gdrive_file_list.files.len() != 1 {
                        let parent_name = file_path_items[i];
                        let resp_body = self
                            .gdrive_create_folder(parent_name, Some(parent.to_owned()))
                            .await?
                            .into_body()
                            .bytes()
                            .await?;
                        let parent_meta: GdriveFile = serde_json::from_slice(&resp_body)
                            .map_err(new_json_deserialize_error)?;

                        parent = parent_meta.id;
                        println!("parent: {}", &parent);
                    } else {
                        parent = gdrive_file_list.files[0].id.clone();
                    }
                }
                StatusCode::NOT_FOUND => {
                    let parent_name = file_path_items[i];
                    let res = self
                        .gdrive_create_folder(parent_name, Some(parent.to_owned()))
                        .await?;

                    let status = res.status();

                    match status {
                        StatusCode::OK => {
                            let parent_id = res.into_body().bytes().await?;
                            parent = String::from_utf8_lossy(&parent_id).to_string();
                        }
                        _ => {
                            println!("404 了");
                            return Err(parse_error(res).await?);
                        }
                    }
                }
                _ => {
                    return Err(parse_error(resp).await?);
                }
            }
        }

        Ok(parent.to_owned())
    }

    pub async fn gdrive_search(
        &self,
        target: &str,
        parent: &str,
    ) -> Result<Response<IncomingAsyncBody>> {
        let query = format!(
            "name = '{}' and '{}' in parents and trashed = false &fields=files(id,name,mimeType)",
            target, parent
        );
        let url = format!(
            "https://www.googleapis.com/drive/v3/files?q={}",
            percent_encode_path(query.as_str())
        );

        let req = self.sign(
            Request::get(&url)
                .body(AsyncBody::Empty)
                .map_err(new_request_build_error)?,
        );

        self.client.send(req).await
    }

    /// Create a folder.
    /// Should provide the parent folder id.
    /// Or will create the folder in the root folder.
    pub async fn gdrive_create_folder(
        &self,
        name: &str,
        parent: Option<String>,
    ) -> Result<Response<IncomingAsyncBody>> {
        println!(
            "create folder: {} at {}",
            name,
            parent.clone().unwrap_or("root".to_owned())
        );
        let url = "https://www.googleapis.com/drive/v3/files";

        let req = Request::post(url)
            .header(header::CONTENT_TYPE, "application/json")
            .body(AsyncBody::Bytes(bytes::Bytes::from(
                serde_json::to_vec(&json!({
                    "name": name,
                    "mimeType": "application/vnd.google-apps.folder",
                    // If the parent is not provided, the folder will be created in the root folder.
                    "parents": [parent.unwrap_or("root".to_owned())],
                }))
                .map_err(|e| {
                    Error::new(
                        ErrorKind::Unexpected,
                        &format!("failed to serialize json(create folder result): {}", e),
                    )
                })?,
            )))
            .map_err(new_request_build_error)?;

        let req = self.sign(req);

        self.client.send(req).await
    }

    /// Create a folder.
    /// Will create the path recursively.
    pub async fn gdrive_create_dir(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let parent = self.ensure_parent_path(path).await?;

        let path = path.split('/').filter(|&x| !x.is_empty()).last().unwrap();

        self.gdrive_create_folder(path, Some(parent)).await
    }

    pub async fn gdrive_stat(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let path_id = self.get_file_id_by_path(path).await?;

        // The file metadata in the Google Drive API is very complex.
        // For now, we only need the file id, name, mime type and modified time.
        let req = self.sign(
            Request::get(&format!(
                "https://www.googleapis.com/drive/v3/files/{}?fields=id,name,mimeType,size,modifiedTime",
                path_id.as_str()
            ))
                .body(AsyncBody::Empty)
                .map_err(new_request_build_error)?,
        );

        self.client.send(req).await
    }

    pub async fn gdrive_get(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let url: String = format!(
            "https://www.googleapis.com/drive/v3/files/{}?alt=media",
            self.get_file_id_by_path(path).await?
        );

        let req = self.sign(
            Request::get(&url)
                .body(AsyncBody::Empty)
                .map_err(new_request_build_error)?,
        );

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
            "https://www.googleapis.com/upload/drive/v3/files/{}?uploadType=media",
            self.get_file_id_by_path(path).await?
        );

        let mut req = Request::put(&url);

        if let Some(size) = size {
            req = req.header(header::CONTENT_LENGTH, size)
        }

        if let Some(mime) = content_type {
            req = req.header(header::CONTENT_TYPE, mime)
        }

        let req = self.sign(req.body(body).map_err(new_request_build_error)?);

        self.client.send(req).await
    }

    pub async fn gdrive_delete(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let url = format!(
            "https://www.googleapis.com/drive/v3/files/{}",
            self.get_file_id_by_path(path).await?
        );

        let mut req = Request::delete(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        req = self.sign(req);

        self.client.send(req).await
    }

    pub async fn gdrive_upload_simple_request(
        &self,
        path: &str,
        size: u64,
        body: AsyncBody,
    ) -> Result<Response<IncomingAsyncBody>> {
        let parent = self.ensure_parent_path(path).await?;

        let url = "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart";

        let file_name = path.split('/').filter(|&x| !x.is_empty()).last().unwrap();

        let metadata = &json!({
            "name": file_name,
            "parents": [parent],
        });

        let mut fd = Form::new().text("metadata", metadata.to_string());

        match body {
            AsyncBody::Bytes(bs) => {
                fd = fd.part("file", Part::bytes(bs.to_vec()));
            }
            AsyncBody::Stream(bs) => {
                fd = fd.part("file", Part::stream(reqwest::Body::wrap_stream(bs)));
            }
            AsyncBody::Empty => {
                // We do nothing here.
                fd = fd.part("file", Part::bytes(Vec::new()));
            }
        };

        let mut req = Request::post(url)
            .header(header::CONTENT_TYPE, "application/json; charset=UTF-8")
            .header(header::CONTENT_LENGTH, size)
            .header("X-Upload-Content-Length", size)
            .body(fd)
            .map_err(new_request_build_error)?;

        req = self.sign(req);

        self.client.send_form_data(req).await
    }

    /// Start an upload session.
    ///
    /// ## Notes
    ///
    /// - If we know the size of the file, we should provide it here.
    pub async fn gdrive_upload_initial_request(
        &self,
        path: &str,
        size: Option<u64>,
    ) -> Result<Response<IncomingAsyncBody>> {
        let parent = self.ensure_parent_path(path).await?;

        let url = "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable";

        let file_name = path.split('/').filter(|&x| !x.is_empty()).last().unwrap();

        let bs = serde_json::to_vec(&json!({
            "name": file_name,
            "parents": [parent],
        }))
        .map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                &format!("failed to serialize json(upload initial request): {}", e),
            )
        })?;

        let mut req = Request::post(url)
            .header(header::CONTENT_TYPE, "application/json; charset=UTF-8")
            .header(header::CONTENT_LENGTH, bs.len());

        if let Some(size) = size {
            req = req.header("X-Upload-Content-Length", size);
        }

        let mut req = req
            .body(AsyncBody::Bytes(bytes::Bytes::from(bs)))
            .map_err(new_request_build_error)?;

        req = self.sign(req);

        self.client.send(req).await
    }

    /// Upload a part.
    /// It's important to know the size of the part and the position of the part in the whole file.
    ///
    /// ## Notes
    ///
    /// - If don't know the whole size of the file,
    /// it will be replaced by `*` in `content-range` header automatically.
    /// - Please keep the whole size of the file consistent during the upload.
    pub async fn gdrive_upload_part_request(
        &self,
        target: &str,
        size: u64,
        position: u64,
        whole_size: Option<u64>,
        body: AsyncBody,
    ) -> Result<Response<IncomingAsyncBody>> {
        let mut req = Request::put(target)
            .header("Content-Length", size)
            .header(
                "Content-Range",
                format!(
                    "bytes {}-{}/{}",
                    position,
                    position + size - 1,
                    match whole_size {
                        Some(ws) => ws.to_string(),
                        _ => "*".to_string(),
                    }
                ),
            )
            .body(body)
            .map_err(new_request_build_error)?;

        req = self.sign(req);

        self.client.send(req).await
    }

    /// Finish the upload.
    ///
    /// For Google Drive, to finish an upload session, we should send a request to the upload session
    /// has received the whole file.
    ///
    /// So if we don't know the size of the file, when we have uploaded the whole file, we should
    /// tell the server that the upload session has received the whole file.
    ///
    /// It could be done by sending a request to the upload session with the size of the file we have
    /// uploaded.
    /// So the server will know that the upload session has received the whole file.
    /// We can count the size of all parts we have uploaded by ourselves.
    ///
    /// ## Notes
    ///
    /// **It's no need to call this if we know the size of the file.**
    ///
    /// When we know the file size,
    /// we can indicate the total size in the Content-range header during the upload,
    /// and this allows the server to know that it has received all parts,
    /// like `Content-range: bytes 0-1023/1024`.
    pub async fn gdrive_finish_upload_request(
        &self,
        target: &str,
        size: u64,
    ) -> Result<Response<IncomingAsyncBody>> {
        let mut req = Request::put(target)
            .header("Content-Length", 0)
            .header("Content-Range", format!("bytes */{}", size))
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        req = self.sign(req);

        self.client.send(req).await
    }

    /// Cancel the upload.
    /// Similar to the delete operation, but the target is an upload session.
    pub async fn gdrive_cancel_upload_request(
        &self,
        target: &str,
    ) -> Result<Response<IncomingAsyncBody>> {
        let mut req = Request::delete(target)
            .header("Content-Length", 0)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        req = self.sign(req);

        self.client.send(req).await
    }

    fn sign<T>(&self, mut req: Request<T>) -> Request<T> {
        let auth_header_content = format!("Bearer {}", self.access_token);
        req.headers_mut().insert(
            header::AUTHORIZATION,
            auth_header_content
                .parse()
                .expect("failed to parse auth header"),
        );
        req
    }
}

// This is the file struct returned by the Google Drive API.
// This is a complex struct, but we only add the fields we need.
// refer to https://developers.google.com/drive/api/reference/rest/v3/files#File
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GdriveFile {
    pub mime_type: String,
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub size: String,
    #[serde(default)]
    pub modified_time: String,
}

// refer to https://developers.google.com/drive/api/reference/rest/v3/files/list
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GdriveFileList {
    files: Vec<GdriveFile>,
}
