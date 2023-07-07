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

use bytes::Bytes;
use chrono::DateTime;
use chrono::Utc;
use http::header;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::Request;
use http::Response;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Mutex;

use crate::raw::build_rooted_abs_path;
use crate::raw::new_json_deserialize_error;
use crate::raw::new_json_serialize_error;
use crate::raw::new_request_build_error;
use crate::raw::AsyncBody;
use crate::raw::HttpClient;
use crate::raw::IncomingAsyncBody;
use crate::types::Result;

pub struct DropboxCore {
    pub signer: Arc<Mutex<DropboxSigner>>,
    pub client: HttpClient,
    pub root: String,
}

impl Debug for DropboxCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("DropboxCore");
        de.finish()
    }
}

impl DropboxCore {
    fn build_path(&self, path: &str) -> String {
        let path = build_rooted_abs_path(&self.root, path);
        // For dropbox, even the path is a directory,
        // we still need to remove the trailing slash.
        path.trim_end_matches('/').to_string()
    }

    pub async fn dropbox_get(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let url: String = "https://content.dropboxapi.com/2/files/download".to_string();
        let download_args = DropboxDownloadArgs {
            path: build_rooted_abs_path(&self.root, path),
        };
        let request_payload =
            serde_json::to_string(&download_args).map_err(new_json_serialize_error)?;
        let mut request = Request::post(&url)
            .header("Dropbox-API-Arg", request_payload)
            .header(CONTENT_LENGTH, 0)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.sign(&mut request).await?;
        self.client.send(request).await
    }

    pub async fn dropbox_update(
        &self,
        path: &str,
        size: Option<usize>,
        content_type: Option<&str>,
        body: AsyncBody,
    ) -> Result<Response<IncomingAsyncBody>> {
        let url = "https://content.dropboxapi.com/2/files/upload".to_string();
        let args = DropboxUploadArgs {
            path: build_rooted_abs_path(&self.root, path),
            ..Default::default()
        };
        let mut request_builder = Request::post(&url);
        if let Some(size) = size {
            request_builder = request_builder.header(CONTENT_LENGTH, size);
        }
        request_builder = request_builder.header(
            CONTENT_TYPE,
            content_type.unwrap_or("application/octet-stream"),
        );

        let mut request = request_builder
            .header(
                "Dropbox-API-Arg",
                serde_json::to_string(&args).map_err(new_json_serialize_error)?,
            )
            .body(body)
            .map_err(new_request_build_error)?;

        self.sign(&mut request).await?;
        self.client.send(request).await
    }

    pub async fn dropbox_delete(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let url = "https://api.dropboxapi.com/2/files/delete_v2".to_string();
        let args = DropboxDeleteArgs {
            path: self.build_path(path),
        };

        let bs = Bytes::from(serde_json::to_string(&args).map_err(new_json_serialize_error)?);

        let mut request = Request::post(&url)
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_LENGTH, bs.len())
            .body(AsyncBody::Bytes(bs))
            .map_err(new_request_build_error)?;

        self.sign(&mut request).await?;
        self.client.send(request).await
    }

    pub async fn dropbox_delete_batch(
        &self,
        paths: Vec<String>,
    ) -> Result<Response<IncomingAsyncBody>> {
        let url = "https://api.dropboxapi.com/2/files/delete_batch".to_string();
        let args = DropboxDeleteBatchArgs {
            entries: paths
                .into_iter()
                .map(|path| DropboxDeleteBatchEntry {
                    path: self.build_path(&path),
                })
                .collect(),
        };

        let bs = Bytes::from(serde_json::to_string(&args).map_err(new_json_serialize_error)?);

        let mut request = Request::post(&url)
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_LENGTH, bs.len())
            .body(AsyncBody::Bytes(bs))
            .map_err(new_request_build_error)?;

        self.sign(&mut request).await?;
        self.client.send(request).await
    }

    pub async fn dropbox_delete_batch_check(
        &self,
        async_job_id: String,
    ) -> Result<Response<IncomingAsyncBody>> {
        let url = "https://api.dropboxapi.com/2/files/delete_batch/check".to_string();
        let args = DropboxDeleteBatchCheckArgs { async_job_id };

        let bs = Bytes::from(serde_json::to_string(&args).map_err(new_json_serialize_error)?);

        let mut request = Request::post(&url)
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_LENGTH, bs.len())
            .body(AsyncBody::Bytes(bs))
            .map_err(new_request_build_error)?;

        self.sign(&mut request).await?;
        self.client.send(request).await
    }

    pub async fn dropbox_create_folder(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let url = "https://api.dropboxapi.com/2/files/create_folder_v2".to_string();
        let args = DropboxCreateFolderArgs {
            path: self.build_path(path),
        };

        let bs = Bytes::from(serde_json::to_string(&args).map_err(new_json_serialize_error)?);

        let mut request = Request::post(&url)
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_LENGTH, bs.len())
            .body(AsyncBody::Bytes(bs))
            .map_err(new_request_build_error)?;

        self.sign(&mut request).await?;
        self.client.send(request).await
    }

    pub async fn dropbox_get_metadata(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let url = "https://api.dropboxapi.com/2/files/get_metadata".to_string();
        let args = DropboxMetadataArgs {
            path: self.build_path(path),
            ..Default::default()
        };

        let bs = Bytes::from(serde_json::to_string(&args).map_err(new_json_serialize_error)?);

        let mut request = Request::post(&url)
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_LENGTH, bs.len())
            .body(AsyncBody::Bytes(bs))
            .map_err(new_request_build_error)?;

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
        let url = "https://api.dropboxapi.com/oauth2/token".to_string();

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

        let token: DropboxTokenResponse =
            serde_json::from_slice(&body).map_err(new_json_deserialize_error)?;

        // Update signer after token refreshed.
        signer.access_token = token.access_token.clone();

        // Refresh it 2 minutes earlier.
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
            refresh_token: "".to_string(),
            client_id: String::new(),
            client_secret: String::new(),

            access_token: "".to_string(),
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
struct DropboxMetadataArgs {
    include_deleted: bool,
    include_has_explicit_shared_members: bool,
    include_media_info: bool,
    path: String,
}

impl Default for DropboxMetadataArgs {
    fn default() -> Self {
        DropboxMetadataArgs {
            include_deleted: false,
            include_has_explicit_shared_members: false,
            include_media_info: false,
            path: "".to_string(),
        }
    }
}

#[derive(Clone, Deserialize)]
struct DropboxTokenResponse {
    access_token: String,
    expires_in: usize,
}
