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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use bytes::Buf;
use bytes::Bytes;
use chrono::DateTime;
use chrono::Utc;
use http::header;
use http::Request;
use http::Response;

use http::StatusCode;
use tokio::sync::Mutex;

use super::error::parse_error;
use super::graph_model::CreateDirPayload;
use super::graph_model::GraphOAuthRefreshTokenResponseBody;
use super::graph_model::ItemType;
use super::graph_model::OneDriveItem;
use super::graph_model::OneDriveUploadSessionCreationRequestBody;
use crate::raw::*;
use crate::*;

pub struct OneDriveCore {
    pub info: Arc<AccessorInfo>,
    pub root: String,
    pub signer: Arc<Mutex<OneDriveSigner>>,
}

impl Debug for OneDriveCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OneDriveCore")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

// OneDrive returns 400 when try to access a dir with the POSIX special directory entries
const SPECIAL_POSIX_ENTRIES: [&str; 3] = [".", "/", ""];

// OneDrive API parameters allows using with a parameter of:
//
// - ID
// - file path
//
// `services-onedrive` uses the file path based API for simplicity.
// Read more at https://learn.microsoft.com/en-us/graph/onedrive-addressing-driveitems
//
// When debugging and running behavior tests against `services-onedrive`,
// please try to keep the drive clean to reduce the likelihood of flaky results.
impl OneDriveCore {
    // OneDrive personal's base URL. `me` is an alias that represents the user's "Drive".
    pub(crate) const DRIVE_ROOT_URL: &str = "https://graph.microsoft.com/v1.0/me/drive/root";

    /// Get a URL to an OneDrive item
    ///
    /// This function is useful for get an item and listing where OneDrive requires a more precise file path.
    pub(crate) fn onedrive_item_url(root: &str, path: &str) -> String {
        // OneDrive requires the root to be the same as `DRIVE_ROOT_URL`.
        // For files under the root, the URL pattern becomes `https://graph.microsoft.com/v1.0/me/drive/root:<path>:`
        if root == "/" && SPECIAL_POSIX_ENTRIES.contains(&path) {
            Self::DRIVE_ROOT_URL.to_string()
        } else {
            // OneDrive returns 400 when try to access a folder with a ending slash
            let path = build_rooted_abs_path(root, path);
            let path = path.strip_suffix('/').unwrap_or(path.as_str());
            format!("{}:{}", Self::DRIVE_ROOT_URL, percent_encode_path(path))
        }
    }

    pub(crate) async fn onedrive_stat(&self, path: &str) -> Result<Metadata> {
        let response = self.onedrive_get_stat(path).await?;
        let status = response.status();

        if !status.is_success() {
            return Err(parse_error(response));
        }

        let bytes = response.into_body();
        let decoded_response: OneDriveItem =
            serde_json::from_reader(bytes.reader()).map_err(new_json_deserialize_error)?;

        let entry_mode: EntryMode = match decoded_response.item_type {
            ItemType::Folder { .. } => EntryMode::DIR,
            ItemType::File { .. } => EntryMode::FILE,
        };

        let mut meta = Metadata::new(entry_mode)
            .with_etag(decoded_response.e_tag)
            .with_content_length(decoded_response.size.max(0) as u64);

        let last_modified = decoded_response.last_modified_date_time;
        let date_utc_last_modified = parse_datetime_from_rfc3339(&last_modified)?;
        meta.set_last_modified(date_utc_last_modified);

        Ok(meta)
    }

    pub(crate) async fn onedrive_get_stat(&self, path: &str) -> Result<Response<Buffer>> {
        let url: String = format!("{}:{}", Self::DRIVE_ROOT_URL, percent_encode_path(path));

        let mut request = Request::get(&url)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(&mut request).await?;

        self.info.http_client().send(request).await
    }

    pub(crate) async fn onedrive_get_next_list_page(&self, url: &str) -> Result<Response<Buffer>> {
        let mut request = Request::get(url)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(&mut request).await?;

        self.info.http_client().send(request).await
    }

    /// Download a file
    ///
    /// OneDrive handles a download in 2 steps:
    /// 1. Returns a 302 with a presigned URL. If `If-None-Match` succeed, returns 304.
    /// 2. With the presigned URL, we can send a GET:
    ///   1. When getting an item succeed with a `Range` header, we get a 206 Partial Content response.
    ///   2. When succeed, we get a 200 response.
    ///
    /// Read more at https://learn.microsoft.com/en-us/graph/api/driveitem-get-content
    pub(crate) async fn onedrive_get_content(
        &self,
        path: &str,
        range: BytesRange,
        etag: Option<&str>,
    ) -> Result<Response<HttpBody>> {
        let path = build_rooted_abs_path(&self.root, path);
        let url: String = format!(
            "{}:{}:/content",
            Self::DRIVE_ROOT_URL,
            percent_encode_path(&path),
        );

        let mut request = Request::get(&url).header(header::RANGE, range.to_header());
        if let Some(etag) = etag {
            request = request.header(header::IF_NONE_MATCH, etag);
        }

        let mut request = request
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(&mut request).await?;

        self.info.http_client().fetch(request).await
    }

    pub async fn onedrive_upload_simple(
        &self,
        path: &str,
        size: Option<usize>,
        args: &OpWrite,
        body: Buffer,
    ) -> Result<Response<Buffer>> {
        let url = format!(
            "{}:{}:/content",
            Self::DRIVE_ROOT_URL,
            percent_encode_path(path)
        );

        let mut request = Request::put(&url);

        if let Some(size) = size {
            request = request.header(header::CONTENT_LENGTH, size)
        }

        if let Some(mime) = args.content_type() {
            request = request.header(header::CONTENT_TYPE, mime)
        }

        let mut request = request.body(body).map_err(new_request_build_error)?;

        self.sign(&mut request).await?;

        self.info.http_client().send(request).await
    }

    pub(crate) async fn onedrive_chunked_upload(
        &self,
        url: &str,
        args: &OpWrite,
        offset: usize,
        chunk_end: usize,
        total_len: usize,
        body: Buffer,
    ) -> Result<Response<Buffer>> {
        let mut request = Request::put(url);

        let range = format!("bytes {}-{}/{}", offset, chunk_end, total_len);
        request = request.header("Content-Range".to_string(), range);

        let size = chunk_end - offset + 1;
        request = request.header(header::CONTENT_LENGTH, size.to_string());

        if let Some(mime) = args.content_type() {
            request = request.header(header::CONTENT_TYPE, mime)
        }

        let mut request = request.body(body).map_err(new_request_build_error)?;

        self.sign(&mut request).await?;

        self.info.http_client().send(request).await
    }

    pub(crate) async fn onedrive_create_upload_session(
        &self,
        url: &str,
        body: OneDriveUploadSessionCreationRequestBody,
    ) -> Result<Response<Buffer>> {
        let body_bytes = serde_json::to_vec(&body).map_err(new_json_serialize_error)?;
        let body = Buffer::from(Bytes::from(body_bytes));
        let mut request = Request::post(url)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .map_err(new_request_build_error)?;

        self.sign(&mut request).await?;

        self.info.http_client().send(request).await
    }

    /// Create a directory
    ///
    /// When creates a folder, OneDrive returns a status code with 201.
    /// When using `microsoft.graph.conflictBehavior=replace` to replace a folder, OneDrive returns 200.
    pub(crate) async fn onedrive_create_dir(&self, path: &str) -> Result<Response<Buffer>> {
        let path = build_rooted_abs_path(&self.root, path);
        let path_before_last_slash = get_parent(&path);
        let normalized = path_before_last_slash
            .strip_suffix('/')
            .unwrap_or(path_before_last_slash);
        let encoded_path = percent_encode_path(normalized);

        let url = format!("{}:{}:/children", Self::DRIVE_ROOT_URL, encoded_path);

        let folder_name = get_basename(&path);
        let folder_name = folder_name.strip_suffix('/').unwrap_or(folder_name);

        let payload = CreateDirPayload::new(folder_name.to_string());
        let body_bytes = serde_json::to_vec(&payload).map_err(new_json_serialize_error)?;
        let body = Buffer::from(bytes::Bytes::from(body_bytes));

        let mut request = Request::post(url)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .map_err(new_request_build_error)?;

        self.sign(&mut request).await?;

        self.info.http_client().send(request).await
    }

    pub(crate) async fn onedrive_delete(&self, path: &str) -> Result<Response<Buffer>> {
        let path = build_abs_path(&self.root, path);
        let url = format!("{}:/{}:", Self::DRIVE_ROOT_URL, percent_encode_path(&path));

        let mut request = Request::delete(&url)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(&mut request).await?;

        self.info.http_client().send(request).await
    }

    pub async fn sign<T>(&self, request: &mut Request<T>) -> Result<()> {
        let mut signer = self.signer.lock().await;
        signer.sign(request).await
    }
}

// keeps track of OAuth 2.0 tokens and refreshes the access token.
pub struct OneDriveSigner {
    pub info: Arc<AccessorInfo>, // to use `http_client`

    pub client_id: String,
    pub client_secret: String,
    pub refresh_token: String,

    pub access_token: String,
    pub expires_in: DateTime<Utc>,
}

// OneDrive is part of Graph API hence shares the same authentication and authorization processes.
// `common` applies to account types:
//
// - consumers
// - work and school account
//
// set to `common` for simplicity
const ONEDRIVE_REFRESH_TOKEN: &str = "https://login.microsoftonline.com/common/oauth2/v2.0/token";

impl OneDriveSigner {
    pub fn new(info: Arc<AccessorInfo>) -> Self {
        OneDriveSigner {
            info,

            client_id: "".to_string(),
            client_secret: "".to_string(),
            refresh_token: "".to_string(),
            access_token: "".to_string(),
            expires_in: DateTime::<Utc>::MIN_UTC,
        }
    }

    async fn refresh_tokens(&mut self) -> Result<()> {
        // OneDrive users must provide at least this required permission scope
        let encoded_payload = format!(
            "client_id={}&client_secret={}&scope=Files.ReadWrite&refresh_token={}&grant_type=refresh_token",
            percent_encode_path(self.client_id.as_str()),
            percent_encode_path(self.client_secret.as_str()),
            percent_encode_path(self.refresh_token.as_str())
        );
        let request = Request::post(ONEDRIVE_REFRESH_TOKEN)
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Buffer::from(encoded_payload))
            .map_err(new_request_build_error)?;

        let response = self.info.http_client().send(request).await?;
        let status = response.status();
        match status {
            StatusCode::OK => {
                let resp_body = response.into_body();
                let data: GraphOAuthRefreshTokenResponseBody =
                    serde_json::from_reader(resp_body.reader())
                        .map_err(new_json_deserialize_error)?;
                self.access_token = data.access_token;
                self.refresh_token = data.refresh_token;
                self.expires_in = Utc::now()
                    + chrono::TimeDelta::try_seconds(data.expires_in)
                        .expect("expires_in must be valid seconds")
                    - chrono::TimeDelta::minutes(2); // assumes 2 mins graceful transmission for implementation simplicity
                Ok(())
            }
            _ => Err(parse_error(response)),
        }
    }

    /// Sign a request.
    pub async fn sign<T>(&mut self, request: &mut Request<T>) -> Result<()> {
        if !self.access_token.is_empty() && self.expires_in > Utc::now() {
            let value = format!("Bearer {}", self.access_token)
                .parse()
                .expect("access_token must be valid header value");

            request.headers_mut().insert(header::AUTHORIZATION, value);
            return Ok(());
        }

        self.refresh_tokens().await?;

        let auth_header_content = format!("Bearer {}", self.access_token)
            .parse()
            .expect("Fetched access_token is invalid as a header value");

        request
            .headers_mut()
            .insert(header::AUTHORIZATION, auth_header_content);

        Ok(())
    }
}
