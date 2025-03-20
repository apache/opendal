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

use std::default::Default;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use bytes::Buf;
use bytes::Bytes;
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

use super::error::parse_error;
use crate::raw::*;
use crate::*;

pub struct DropboxCore {
    pub info: Arc<AccessorInfo>,
    pub root: String,
    pub signer: Arc<Mutex<DropboxSigner>>,
}

impl Debug for DropboxCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DropboxCore")
            .field("root", &self.root)
            .finish()
    }
}

impl DropboxCore {
    fn build_path(&self, path: &str) -> String {
        let path = build_rooted_abs_path(&self.root, path);
        // For dropbox, even the path is a directory,
        // we still need to remove the trailing slash.
        path.trim_end_matches('/').to_string()
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
        let url = "https://api.dropboxapi.com/oauth2/token".to_string();

        let content = format!(
            "grant_type=refresh_token&refresh_token={}&client_id={}&client_secret={}",
            signer.refresh_token, signer.client_id, signer.client_secret
        );
        let bs = Bytes::from(content);

        let request = Request::post(&url)
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header(CONTENT_LENGTH, bs.len())
            .body(Buffer::from(bs))
            .map_err(new_request_build_error)?;

        let resp = self.info.http_client().send(request).await?;
        let body = resp.into_body();

        let token: DropboxTokenResponse =
            serde_json::from_reader(body.reader()).map_err(new_json_deserialize_error)?;

        // Update signer after token refreshed.
        signer.access_token.clone_from(&token.access_token);

        // Refresh it 2 minutes earlier.
        signer.expires_in = Utc::now()
            + chrono::TimeDelta::try_seconds(token.expires_in as i64)
                .expect("expires_in must be valid seconds")
            - chrono::TimeDelta::try_seconds(120).expect("120 must be valid seconds");

        let value = format!("Bearer {}", token.access_token)
            .parse()
            .expect("token must be valid header value");
        req.headers_mut().insert(header::AUTHORIZATION, value);

        Ok(())
    }

    pub async fn dropbox_get(
        &self,
        path: &str,
        range: BytesRange,
        _: &OpRead,
    ) -> Result<Response<HttpBody>> {
        let url: String = "https://content.dropboxapi.com/2/files/download".to_string();
        let download_args = DropboxDownloadArgs {
            path: build_rooted_abs_path(&self.root, path),
        };
        let request_payload =
            serde_json::to_string(&download_args).map_err(new_json_serialize_error)?;

        let mut req = Request::post(&url)
            .header("Dropbox-API-Arg", request_payload)
            .header(CONTENT_LENGTH, 0);

        if !range.is_full() {
            req = req.header(header::RANGE, range.to_header());
        }

        let mut request = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.sign(&mut request).await?;
        self.info.http_client().fetch(request).await
    }

    pub async fn dropbox_update(
        &self,
        path: &str,
        size: Option<usize>,
        args: &OpWrite,
        body: Buffer,
    ) -> Result<Response<Buffer>> {
        let url = "https://content.dropboxapi.com/2/files/upload".to_string();
        let dropbox_update_args = DropboxUploadArgs {
            path: build_rooted_abs_path(&self.root, path),
            ..Default::default()
        };
        let mut request_builder = Request::post(&url);
        if let Some(size) = size {
            request_builder = request_builder.header(CONTENT_LENGTH, size);
        }
        request_builder = request_builder.header(
            CONTENT_TYPE,
            args.content_type().unwrap_or("application/octet-stream"),
        );

        let mut request = request_builder
            .header(
                "Dropbox-API-Arg",
                serde_json::to_string(&dropbox_update_args).map_err(new_json_serialize_error)?,
            )
            .body(body)
            .map_err(new_request_build_error)?;

        self.sign(&mut request).await?;
        self.info.http_client().send(request).await
    }

    pub async fn dropbox_delete(&self, path: &str) -> Result<Response<Buffer>> {
        let url = "https://api.dropboxapi.com/2/files/delete_v2".to_string();
        let args = DropboxDeleteArgs {
            path: self.build_path(path),
        };

        let bs = Bytes::from(serde_json::to_string(&args).map_err(new_json_serialize_error)?);

        let mut request = Request::post(&url)
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_LENGTH, bs.len())
            .body(Buffer::from(bs))
            .map_err(new_request_build_error)?;

        self.sign(&mut request).await?;
        self.info.http_client().send(request).await
    }

    pub async fn dropbox_create_folder(&self, path: &str) -> Result<RpCreateDir> {
        let url = "https://api.dropboxapi.com/2/files/create_folder_v2".to_string();
        let args = DropboxCreateFolderArgs {
            path: self.build_path(path),
        };

        let bs = Bytes::from(serde_json::to_string(&args).map_err(new_json_serialize_error)?);

        let mut request = Request::post(&url)
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_LENGTH, bs.len())
            .body(Buffer::from(bs))
            .map_err(new_request_build_error)?;

        self.sign(&mut request).await?;
        let resp = self.info.http_client().send(request).await?;
        let status = resp.status();
        match status {
            StatusCode::OK => Ok(RpCreateDir::default()),
            _ => {
                let err = parse_error(resp);
                match err.kind() {
                    ErrorKind::AlreadyExists => Ok(RpCreateDir::default()),
                    _ => Err(err),
                }
            }
        }
    }

    pub async fn dropbox_list(
        &self,
        path: &str,
        recursive: bool,
        limit: Option<usize>,
    ) -> Result<Response<Buffer>> {
        let url = "https://api.dropboxapi.com/2/files/list_folder".to_string();

        // The default settings here align with the DropboxAPI default settings.
        // Refer: https://www.dropbox.com/developers/documentation/http/documentation#files-list_folder
        let args = DropboxListArgs {
            path: self.build_path(path),
            recursive,
            limit: limit.unwrap_or(1000),
        };

        let bs = Bytes::from(serde_json::to_string(&args).map_err(new_json_serialize_error)?);

        let mut request = Request::post(&url)
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_LENGTH, bs.len())
            .body(Buffer::from(bs))
            .map_err(new_request_build_error)?;

        self.sign(&mut request).await?;
        self.info.http_client().send(request).await
    }

    pub async fn dropbox_list_continue(&self, cursor: &str) -> Result<Response<Buffer>> {
        let url = "https://api.dropboxapi.com/2/files/list_folder/continue".to_string();

        let args = DropboxListContinueArgs {
            cursor: cursor.to_string(),
        };

        let bs = Bytes::from(serde_json::to_string(&args).map_err(new_json_serialize_error)?);

        let mut request = Request::post(&url)
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_LENGTH, bs.len())
            .body(Buffer::from(bs))
            .map_err(new_request_build_error)?;

        self.sign(&mut request).await?;
        self.info.http_client().send(request).await
    }

    pub async fn dropbox_copy(&self, from: &str, to: &str) -> Result<Response<Buffer>> {
        let url = "https://api.dropboxapi.com/2/files/copy_v2".to_string();

        let args = DropboxCopyArgs {
            from_path: self.build_path(from),
            to_path: self.build_path(to),
        };

        let bs = Bytes::from(serde_json::to_string(&args).map_err(new_json_serialize_error)?);

        let mut request = Request::post(&url)
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_LENGTH, bs.len())
            .body(Buffer::from(bs))
            .map_err(new_request_build_error)?;

        self.sign(&mut request).await?;
        self.info.http_client().send(request).await
    }

    pub async fn dropbox_move(&self, from: &str, to: &str) -> Result<Response<Buffer>> {
        let url = "https://api.dropboxapi.com/2/files/move_v2".to_string();

        let args = DropboxMoveArgs {
            from_path: self.build_path(from),
            to_path: self.build_path(to),
        };

        let bs = Bytes::from(serde_json::to_string(&args).map_err(new_json_serialize_error)?);

        let mut request = Request::post(&url)
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_LENGTH, bs.len())
            .body(Buffer::from(bs))
            .map_err(new_request_build_error)?;

        self.sign(&mut request).await?;
        self.info.http_client().send(request).await
    }

    pub async fn dropbox_get_metadata(&self, path: &str) -> Result<Response<Buffer>> {
        let url = "https://api.dropboxapi.com/2/files/get_metadata".to_string();
        let args = DropboxMetadataArgs {
            path: self.build_path(path),
            ..Default::default()
        };

        let bs = Bytes::from(serde_json::to_string(&args).map_err(new_json_serialize_error)?);

        let mut request = Request::post(&url)
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_LENGTH, bs.len())
            .body(Buffer::from(bs))
            .map_err(new_request_build_error)?;

        self.sign(&mut request).await?;

        self.info.http_client().send(request).await
    }
}

#[derive(Clone)]
pub struct DropboxSigner {
    pub client_id: String,
    pub client_secret: String,
    pub refresh_token: String,

    pub access_token: String,
    pub expires_in: DateTime<Utc>,
}

impl Default for DropboxSigner {
    fn default() -> Self {
        DropboxSigner {
            refresh_token: String::new(),
            client_id: String::new(),
            client_secret: String::new(),

            access_token: String::new(),
            expires_in: DateTime::<Utc>::MIN_UTC,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DropboxDownloadArgs {
    path: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DropboxUploadArgs {
    path: String,
    mode: String,
    mute: bool,
    autorename: bool,
    strict_conflict: bool,
}

impl Default for DropboxUploadArgs {
    fn default() -> Self {
        DropboxUploadArgs {
            mode: "overwrite".to_string(),
            path: "".to_string(),
            mute: true,
            autorename: false,
            strict_conflict: false,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DropboxDeleteArgs {
    path: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DropboxDeleteBatchEntry {
    path: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DropboxDeleteBatchArgs {
    entries: Vec<DropboxDeleteBatchEntry>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DropboxDeleteBatchCheckArgs {
    async_job_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DropboxCreateFolderArgs {
    path: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DropboxListArgs {
    path: String,
    recursive: bool,
    limit: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DropboxListContinueArgs {
    cursor: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DropboxCopyArgs {
    from_path: String,
    to_path: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DropboxMoveArgs {
    from_path: String,
    to_path: String,
}

#[derive(Default, Clone, Debug, Deserialize, Serialize)]
struct DropboxMetadataArgs {
    include_deleted: bool,
    include_has_explicit_shared_members: bool,
    include_media_info: bool,
    path: String,
}

#[derive(Clone, Deserialize)]
struct DropboxTokenResponse {
    access_token: String,
    expires_in: usize,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default)]
pub struct DropboxMetadataResponse {
    #[serde(rename(deserialize = ".tag"))]
    pub tag: String,
    pub client_modified: String,
    pub content_hash: Option<String>,
    pub file_lock_info: Option<DropboxMetadataFileLockInfo>,
    pub has_explicit_shared_members: Option<bool>,
    pub id: String,
    pub is_downloadable: Option<bool>,
    pub name: String,
    pub path_display: String,
    pub path_lower: String,
    pub property_groups: Option<Vec<DropboxMetadataPropertyGroup>>,
    pub rev: Option<String>,
    pub server_modified: Option<String>,
    pub sharing_info: Option<DropboxMetadataSharingInfo>,
    pub size: Option<u64>,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default)]
pub struct DropboxMetadataFileLockInfo {
    pub created: Option<String>,
    pub is_lockholder: bool,
    pub lockholder_name: Option<String>,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default)]
pub struct DropboxMetadataPropertyGroup {
    pub fields: Vec<DropboxMetadataPropertyGroupField>,
    pub template_id: String,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default)]
pub struct DropboxMetadataPropertyGroupField {
    pub name: String,
    pub value: String,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default)]
pub struct DropboxMetadataSharingInfo {
    pub modified_by: Option<String>,
    pub parent_shared_folder_id: Option<String>,
    pub read_only: Option<bool>,
    pub shared_folder_id: Option<String>,
    pub traverse_only: Option<bool>,
    pub no_access: Option<bool>,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default)]
pub struct DropboxListResponse {
    pub entries: Vec<DropboxMetadataResponse>,
    pub cursor: String,
    pub has_more: bool,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default)]
pub struct DropboxDeleteBatchResponse {
    #[serde(rename(deserialize = ".tag"))]
    pub tag: String,
    pub async_job_id: Option<String>,
    pub entries: Option<Vec<DropboxDeleteBatchResponseEntry>>,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default)]
pub struct DropboxDeleteBatchResponseEntry {
    #[serde(rename(deserialize = ".tag"))]
    pub tag: String,
    pub metadata: Option<DropboxMetadataResponse>,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default)]
pub struct DropboxDeleteBatchFailureResponseCause {
    #[serde(rename(deserialize = ".tag"))]
    pub tag: String,
}
