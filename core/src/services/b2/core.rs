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
use std::time::Duration;

use bytes::Buf;
use chrono::DateTime;
use chrono::Utc;
use http::header;
use http::Request;
use http::Response;
use http::StatusCode;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::RwLock;

use self::constants::X_BZ_CONTENT_SHA1;
use self::constants::X_BZ_FILE_NAME;
use super::core::constants::X_BZ_PART_NUMBER;
use super::error::parse_error;
use crate::raw::*;
use crate::*;

pub(super) mod constants {
    pub const X_BZ_FILE_NAME: &str = "X-Bz-File-Name";
    pub const X_BZ_CONTENT_SHA1: &str = "X-Bz-Content-Sha1";
    pub const X_BZ_PART_NUMBER: &str = "X-Bz-Part-Number";
}

/// Core of [b2](https://www.backblaze.com/cloud-storage) services support.
#[derive(Clone)]
pub struct B2Core {
    pub info: Arc<AccessorInfo>,
    pub signer: Arc<RwLock<B2Signer>>,

    /// The root of this core.
    pub root: String,
    /// The bucket name of this backend.
    pub bucket: String,
    /// The bucket id of this backend.
    pub bucket_id: String,
}

impl Debug for B2Core {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend")
            .field("root", &self.root)
            .field("bucket", &self.bucket)
            .field("bucket_id", &self.bucket_id)
            .finish_non_exhaustive()
    }
}

impl B2Core {
    #[inline]
    pub async fn send(&self, req: Request<Buffer>) -> Result<Response<Buffer>> {
        self.info.http_client().send(req).await
    }

    /// [b2_authorize_account](https://www.backblaze.com/apidocs/b2-authorize-account)
    pub async fn get_auth_info(&self) -> Result<AuthInfo> {
        {
            let signer = self.signer.read().await;

            if !signer.auth_info.authorization_token.is_empty()
                && signer.auth_info.expires_in > Utc::now()
            {
                let auth_info = signer.auth_info.clone();
                return Ok(auth_info);
            }
        }

        {
            let mut signer = self.signer.write().await;
            let req = Request::get("https://api.backblazeb2.com/b2api/v2/b2_authorize_account")
                .header(
                    header::AUTHORIZATION,
                    format_authorization_by_basic(
                        &signer.application_key_id,
                        &signer.application_key,
                    )?,
                )
                .body(Buffer::new())
                .map_err(new_request_build_error)?;

            let resp = self.info.http_client().send(req).await?;
            let status = resp.status();

            match status {
                StatusCode::OK => {
                    let resp_body = resp.into_body();
                    let token: AuthorizeAccountResponse =
                        serde_json::from_reader(resp_body.reader())
                            .map_err(new_json_deserialize_error)?;
                    signer.auth_info = AuthInfo {
                        authorization_token: token.authorization_token.clone(),
                        api_url: token.api_url.clone(),
                        download_url: token.download_url.clone(),
                        // This authorization token is valid for at most 24 hours.
                        expires_in: Utc::now()
                            + chrono::TimeDelta::try_hours(20).expect("20 hours must be valid"),
                    };
                }
                _ => {
                    return Err(parse_error(resp));
                }
            }
            Ok(signer.auth_info.clone())
        }
    }
}

impl B2Core {
    pub async fn download_file_by_name(
        &self,
        path: &str,
        range: BytesRange,
        _args: &OpRead,
    ) -> Result<Response<HttpBody>> {
        let path = build_abs_path(&self.root, path);

        let auth_info = self.get_auth_info().await?;

        // Construct headers to add to the request
        let url = format!(
            "{}/file/{}/{}",
            auth_info.download_url,
            self.bucket,
            percent_encode_path(&path)
        );

        let mut req = Request::get(&url);

        req = req.header(header::AUTHORIZATION, auth_info.authorization_token);

        if !range.is_full() {
            req = req.header(header::RANGE, range.to_header());
        }

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.info.http_client().fetch(req).await
    }

    pub(super) async fn get_upload_url(&self) -> Result<GetUploadUrlResponse> {
        let auth_info = self.get_auth_info().await?;

        let url = format!(
            "{}/b2api/v2/b2_get_upload_url?bucketId={}",
            auth_info.api_url, self.bucket_id
        );

        let mut req = Request::get(&url);

        req = req.header(header::AUTHORIZATION, auth_info.authorization_token);

        // Set body
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        let resp = self.send(req).await?;
        let status = resp.status();
        match status {
            StatusCode::OK => {
                let resp_body = resp.into_body();
                let resp = serde_json::from_reader(resp_body.reader())
                    .map_err(new_json_deserialize_error)?;
                Ok(resp)
            }
            _ => Err(parse_error(resp)),
        }
    }

    pub async fn get_download_authorization(
        &self,
        path: &str,
        expire: Duration,
    ) -> Result<GetDownloadAuthorizationResponse> {
        let path = build_abs_path(&self.root, path);

        let auth_info = self.get_auth_info().await?;

        // Construct headers to add to the request
        let url = format!(
            "{}/b2api/v2/b2_get_download_authorization",
            auth_info.api_url
        );
        let mut req = Request::post(&url);

        req = req.header(header::AUTHORIZATION, auth_info.authorization_token);

        let body = GetDownloadAuthorizationRequest {
            bucket_id: self.bucket_id.clone(),
            file_name_prefix: path,
            valid_duration_in_seconds: expire.as_secs(),
        };
        let body = serde_json::to_vec(&body).map_err(new_json_serialize_error)?;
        let body = bytes::Bytes::from(body);

        let req = req
            .body(Buffer::from(body))
            .map_err(new_request_build_error)?;

        let resp = self.send(req).await?;

        let status = resp.status();
        match status {
            StatusCode::OK => {
                let resp_body = resp.into_body();
                let resp = serde_json::from_reader(resp_body.reader())
                    .map_err(new_json_deserialize_error)?;
                Ok(resp)
            }
            _ => Err(parse_error(resp)),
        }
    }

    pub async fn upload_file(
        &self,
        path: &str,
        size: Option<u64>,
        args: &OpWrite,
        body: Buffer,
    ) -> Result<Response<Buffer>> {
        let resp = self.get_upload_url().await?;

        let p = build_abs_path(&self.root, path);

        let mut req = Request::post(resp.upload_url);

        req = req.header(X_BZ_FILE_NAME, percent_encode_path(&p));

        req = req.header(header::AUTHORIZATION, resp.authorization_token);

        req = req.header(X_BZ_CONTENT_SHA1, "do_not_verify");

        if let Some(size) = size {
            req = req.header(header::CONTENT_LENGTH, size.to_string())
        }

        if let Some(mime) = args.content_type() {
            req = req.header(header::CONTENT_TYPE, mime)
        } else {
            req = req.header(header::CONTENT_TYPE, "b2/x-auto")
        }

        if let Some(pos) = args.content_disposition() {
            req = req.header(header::CONTENT_DISPOSITION, pos)
        }

        // Set body
        let req = req.body(body).map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn start_large_file(&self, path: &str, args: &OpWrite) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let auth_info = self.get_auth_info().await?;

        let url = format!("{}/b2api/v2/b2_start_large_file", auth_info.api_url);

        let mut req = Request::post(&url);

        req = req.header(header::AUTHORIZATION, auth_info.authorization_token);

        let mut start_large_file_request = StartLargeFileRequest {
            bucket_id: self.bucket_id.clone(),
            file_name: percent_encode_path(&p),
            content_type: "b2/x-auto".to_owned(),
        };

        if let Some(mime) = args.content_type() {
            mime.clone_into(&mut start_large_file_request.content_type)
        }

        let body =
            serde_json::to_vec(&start_large_file_request).map_err(new_json_serialize_error)?;
        let body = bytes::Bytes::from(body);

        let req = req
            .body(Buffer::from(body))
            .map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn get_upload_part_url(&self, file_id: &str) -> Result<GetUploadPartUrlResponse> {
        let auth_info = self.get_auth_info().await?;

        let url = format!(
            "{}/b2api/v2/b2_get_upload_part_url?fileId={}",
            auth_info.api_url, file_id
        );

        let mut req = Request::get(&url);

        req = req.header(header::AUTHORIZATION, auth_info.authorization_token);

        // Set body
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        let resp = self.send(req).await?;

        let status = resp.status();
        match status {
            StatusCode::OK => {
                let resp_body = resp.into_body();
                let resp = serde_json::from_reader(resp_body.reader())
                    .map_err(new_json_deserialize_error)?;
                Ok(resp)
            }
            _ => Err(parse_error(resp)),
        }
    }

    pub async fn upload_part(
        &self,
        file_id: &str,
        part_number: usize,
        size: u64,
        body: Buffer,
    ) -> Result<Response<Buffer>> {
        let resp = self.get_upload_part_url(file_id).await?;

        let mut req = Request::post(resp.upload_url);

        req = req.header(X_BZ_PART_NUMBER, part_number.to_string());

        req = req.header(header::CONTENT_LENGTH, size.to_string());

        req = req.header(header::AUTHORIZATION, resp.authorization_token);

        req = req.header(X_BZ_CONTENT_SHA1, "do_not_verify");

        // Set body
        let req = req.body(body).map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn finish_large_file(
        &self,
        file_id: &str,
        part_sha1_array: Vec<String>,
    ) -> Result<Response<Buffer>> {
        let auth_info = self.get_auth_info().await?;

        let url = format!("{}/b2api/v2/b2_finish_large_file", auth_info.api_url);

        let mut req = Request::post(&url);

        req = req.header(header::AUTHORIZATION, auth_info.authorization_token);

        let body = serde_json::to_vec(&FinishLargeFileRequest {
            file_id: file_id.to_owned(),
            part_sha1_array,
        })
        .map_err(new_json_serialize_error)?;
        let body = bytes::Bytes::from(body);

        // Set body
        let req = req
            .body(Buffer::from(body))
            .map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn cancel_large_file(&self, file_id: &str) -> Result<Response<Buffer>> {
        let auth_info = self.get_auth_info().await?;

        let url = format!("{}/b2api/v2/b2_cancel_large_file", auth_info.api_url);

        let mut req = Request::post(&url);

        req = req.header(header::AUTHORIZATION, auth_info.authorization_token);

        let body = serde_json::to_vec(&CancelLargeFileRequest {
            file_id: file_id.to_owned(),
        })
        .map_err(new_json_serialize_error)?;
        let body = bytes::Bytes::from(body);

        // Set body
        let req = req
            .body(Buffer::from(body))
            .map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn list_file_names(
        &self,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        limit: Option<usize>,
        start_after: Option<String>,
    ) -> Result<Response<Buffer>> {
        let auth_info = self.get_auth_info().await?;

        let mut url = format!(
            "{}/b2api/v2/b2_list_file_names?bucketId={}",
            auth_info.api_url, self.bucket_id
        );

        if let Some(prefix) = prefix {
            let prefix = build_abs_path(&self.root, prefix);
            url.push_str(&format!("&prefix={}", percent_encode_path(&prefix)));
        }

        if let Some(limit) = limit {
            url.push_str(&format!("&maxFileCount={}", limit));
        }

        if let Some(start_after) = start_after {
            url.push_str(&format!(
                "&startFileName={}",
                percent_encode_path(&start_after)
            ));
        }

        if let Some(delimiter) = delimiter {
            url.push_str(&format!("&delimiter={}", delimiter));
        }

        let mut req = Request::get(&url);

        req = req.header(header::AUTHORIZATION, auth_info.authorization_token);

        // Set body
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn copy_file(&self, source_file_id: String, to: &str) -> Result<Response<Buffer>> {
        let to = build_abs_path(&self.root, to);

        let auth_info = self.get_auth_info().await?;

        let url = format!("{}/b2api/v2/b2_copy_file", auth_info.api_url);

        let mut req = Request::post(url);

        req = req.header(header::AUTHORIZATION, auth_info.authorization_token);

        let body = CopyFileRequest {
            source_file_id,
            file_name: to,
        };

        let body = serde_json::to_vec(&body).map_err(new_json_serialize_error)?;
        let body = bytes::Bytes::from(body);

        // Set body
        let req = req
            .body(Buffer::from(body))
            .map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn hide_file(&self, path: &str) -> Result<Response<Buffer>> {
        let path = build_abs_path(&self.root, path);

        let auth_info = self.get_auth_info().await?;

        let url = format!("{}/b2api/v2/b2_hide_file", auth_info.api_url);

        let mut req = Request::post(url);

        req = req.header(header::AUTHORIZATION, auth_info.authorization_token);

        let body = HideFileRequest {
            bucket_id: self.bucket_id.clone(),
            file_name: path.to_string(),
        };

        let body = serde_json::to_vec(&body).map_err(new_json_serialize_error)?;
        let body = bytes::Bytes::from(body);

        // Set body
        let req = req
            .body(Buffer::from(body))
            .map_err(new_request_build_error)?;

        self.send(req).await
    }
}

#[derive(Clone)]
pub struct B2Signer {
    /// The application_key_id of this core.
    pub application_key_id: String,
    /// The application_key of this core.
    pub application_key: String,

    pub auth_info: AuthInfo,
}

#[derive(Clone)]
pub struct AuthInfo {
    pub authorization_token: String,
    /// The base URL to use for all API calls except for uploading and downloading files.
    pub api_url: String,
    /// The base URL to use for downloading files.
    pub download_url: String,

    pub expires_in: DateTime<Utc>,
}

impl Default for B2Signer {
    fn default() -> Self {
        B2Signer {
            application_key: String::new(),
            application_key_id: String::new(),

            auth_info: AuthInfo {
                authorization_token: String::new(),
                api_url: String::new(),
                download_url: String::new(),
                expires_in: DateTime::<Utc>::MIN_UTC,
            },
        }
    }
}

/// Request of [b2_start_large_file](https://www.backblaze.com/apidocs/b2-start-large-file).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StartLargeFileRequest {
    pub bucket_id: String,
    pub file_name: String,
    pub content_type: String,
}

/// Response of [b2_start_large_file](https://www.backblaze.com/apidocs/b2-start-large-file).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartLargeFileResponse {
    pub file_id: String,
}

/// Response of [b2_authorize_account](https://www.backblaze.com/apidocs/b2-authorize-account).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthorizeAccountResponse {
    /// An authorization token to use with all calls, other than b2_authorize_account, that need an Authorization header. This authorization token is valid for at most 24 hours.
    /// So we should call b2_authorize_account every 24 hours.
    pub authorization_token: String,
    pub api_url: String,
    pub download_url: String,
}

/// Response of [b2_get_upload_url](https://www.backblaze.com/apidocs/b2-get-upload-url).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetUploadUrlResponse {
    /// The authorizationToken that must be used when uploading files to this bucket.
    /// This token is valid for 24 hours or until the uploadUrl endpoint rejects an upload, see b2_upload_file
    pub authorization_token: String,
    pub upload_url: String,
}

/// Response of [b2_get_upload_url](https://www.backblaze.com/apidocs/b2-get-upload-part-url).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetUploadPartUrlResponse {
    /// The authorizationToken that must be used when uploading files to this bucket.
    /// This token is valid for 24 hours or until the uploadUrl endpoint rejects an upload, see b2_upload_file
    pub authorization_token: String,
    pub upload_url: String,
}

/// Response of [b2_upload_part](https://www.backblaze.com/apidocs/b2-upload-part).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadPartResponse {
    pub content_sha1: String,
}

/// Response of [b2_finish_large_file](https://www.backblaze.com/apidocs/b2-finish-large-file).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FinishLargeFileRequest {
    pub file_id: String,
    pub part_sha1_array: Vec<String>,
}

/// Response of [b2_cancel_large_file](https://www.backblaze.com/apidocs/b2-cancel-large-file).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelLargeFileRequest {
    pub file_id: String,
}

/// Response of [list_file_names](https://www.backblaze.com/apidocs/b2-list-file-names).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListFileNamesResponse {
    pub files: Vec<File>,
    pub next_file_name: Option<String>,
}

/// Response of [b2-finish-large-file](https://www.backblaze.com/apidocs/b2-finish-large-file).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadResponse {
    pub content_length: u64,
    pub content_md5: Option<String>,
    pub content_type: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct File {
    pub file_id: Option<String>,
    pub content_length: u64,
    pub content_md5: Option<String>,
    pub content_type: Option<String>,
    pub file_name: String,
}

pub(super) fn parse_file_info(file: &File) -> Metadata {
    if file.file_name.ends_with('/') {
        return Metadata::new(EntryMode::DIR);
    }

    let mut metadata = Metadata::new(EntryMode::FILE);

    metadata.set_content_length(file.content_length);

    if let Some(content_md5) = &file.content_md5 {
        metadata.set_content_md5(content_md5);
    }

    if let Some(content_type) = &file.content_type {
        metadata.set_content_type(content_type);
    }

    metadata
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CopyFileRequest {
    pub source_file_id: String,
    pub file_name: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HideFileRequest {
    pub bucket_id: String,
    pub file_name: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetDownloadAuthorizationRequest {
    pub bucket_id: String,
    pub file_name_prefix: String,
    pub valid_duration_in_seconds: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetDownloadAuthorizationResponse {
    pub authorization_token: String,
}
