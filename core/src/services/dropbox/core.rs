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
use std::time::Duration;

use backon::ExponentialBuilder;

use bytes::Bytes;
use chrono::DateTime;
use chrono::Utc;
use http::header;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::Request;

use http::StatusCode;
use once_cell::sync::Lazy;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Mutex;

use super::error::parse_error;
use crate::raw::*;
use crate::*;

/// BACKOFF is the backoff used inside dropbox to make sure dropbox async task succeed.
pub static BACKOFF: Lazy<ExponentialBuilder> = Lazy::new(|| {
    ExponentialBuilder::default()
        .with_max_delay(Duration::from_secs(10))
        .with_max_times(10)
        .with_jitter()
});

pub struct DropboxCore {
    pub root: String,

    pub client: HttpClient,

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
            .body(RequestBody::Bytes(bs))
            .map_err(new_request_build_error)?;

        let (_, body) = self.client.send(request).await?.into_parts();
        let token: DropboxTokenResponse = body.to_json().await?;

        // Update signer after token refreshed.
        signer.access_token = token.access_token.clone();

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
        buf: oio::WritableBuf,
    ) -> Result<usize> {
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

        let mut request = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        self.sign(&mut request).await?;
        let (parts, body) = self.client.send(request).await?.into_parts();
        match parts.status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => body.read(buf).await,
            StatusCode::RANGE_NOT_SATISFIABLE => {
                body.consume().await?;
                Ok(0)
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn dropbox_update(
        &self,
        path: &str,
        size: Option<usize>,
        args: &OpWrite,
        body: RequestBody,
    ) -> Result<()> {
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
        let (parts, body) = self.client.send(request).await?.into_parts();
        match parts.status {
            StatusCode::OK => {
                body.consume().await?;
                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn dropbox_delete(&self, path: &str) -> Result<()> {
        let url = "https://api.dropboxapi.com/2/files/delete_v2 ".to_string();
        let args = DropboxDeleteArgs {
            path: self.build_path(path),
        };

        let bs = Bytes::from(serde_json::to_string(&args).map_err(new_json_serialize_error)?);

        let mut request = Request::post(&url)
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_LENGTH, bs.len())
            .body(RequestBody::Bytes(bs))
            .map_err(new_request_build_error)?;

        self.sign(&mut request).await?;
        let (parts, body) = self.client.send(request).await?.into_parts();
        match parts.status {
            StatusCode::OK => {
                body.consume().await?;
                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                let err = parse_error(parts, bs)?;
                match err.kind() {
                    ErrorKind::NotFound => Ok(()),
                    _ => Err(err),
                }
            }
        }
    }

    pub async fn dropbox_delete_batch(
        &self,
        paths: Vec<String>,
    ) -> Result<DropboxDeleteBatchResponse> {
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
            .body(RequestBody::Bytes(bs))
            .map_err(new_request_build_error)?;

        self.sign(&mut request).await?;
        let (parts, body) = self.client.send(request).await?.into_parts();
        match parts.status {
            StatusCode::OK => {
                let resp = body.to_json().await?;
                Ok(resp)
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    /// REMOVE ME: duplicated with batch.
    pub async fn dropbox_delete_batch_check(&self, async_job_id: String) -> Result<RpBatch> {
        let url = "https://api.dropboxapi.com/2/files/delete_batch/check".to_string();
        let args = DropboxDeleteBatchCheckArgs { async_job_id };

        let bs = Bytes::from(serde_json::to_vec(&args).map_err(new_json_serialize_error)?);

        let mut request = Request::post(&url)
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_LENGTH, bs.len())
            .body(RequestBody::Bytes(bs))
            .map_err(new_request_build_error)?;

        self.sign(&mut request).await?;

        let (parts, body) = self.client.send(request).await?.into_parts();
        if parts.status != StatusCode::OK {
            let bs = body.to_bytes().await?;
            return Err(parse_error(parts, bs)?);
        }

        let decoded_response: DropboxDeleteBatchResponse = body.to_json().await?;
        match decoded_response.tag.as_str() {
            "in_progress" => Err(Error::new(
                ErrorKind::Unexpected,
                "delete batch job still in progress",
            )
            .set_temporary()),
            "complete" => {
                let entries = decoded_response.entries.unwrap_or_default();
                let results = self.handle_batch_delete_complete_result(entries);
                Ok(RpBatch::new(results))
            }
            _ => Err(Error::new(
                ErrorKind::Unexpected,
                &format!(
                    "delete batch check failed with unexpected tag {}",
                    decoded_response.tag
                ),
            )),
        }
    }

    pub async fn dropbox_create_folder(&self, path: &str) -> Result<()> {
        let url = "https://api.dropboxapi.com/2/files/create_folder_v2".to_string();
        let args = DropboxCreateFolderArgs {
            path: self.build_path(path),
        };

        let bs = Bytes::from(serde_json::to_string(&args).map_err(new_json_serialize_error)?);

        let mut request = Request::post(&url)
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_LENGTH, bs.len())
            .body(RequestBody::Bytes(bs))
            .map_err(new_request_build_error)?;

        self.sign(&mut request).await?;
        let (parts, body) = self.client.send(request).await?.into_parts();
        match parts.status {
            StatusCode::OK => {
                body.consume().await?;
                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                let err = parse_error(parts, bs)?;
                match err.kind() {
                    ErrorKind::AlreadyExists => Ok(()),
                    _ => Err(err),
                }
            }
        }
    }

    pub async fn dropbox_get_metadata(&self, path: &str) -> Result<Metadata> {
        let url = "https://api.dropboxapi.com/2/files/get_metadata".to_string();
        let args = DropboxMetadataArgs {
            path: self.build_path(path),
            ..Default::default()
        };

        let bs = Bytes::from(serde_json::to_string(&args).map_err(new_json_serialize_error)?);

        let mut request = Request::post(&url)
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_LENGTH, bs.len())
            .body(RequestBody::Bytes(bs))
            .map_err(new_request_build_error)?;

        self.sign(&mut request).await?;

        let (parts, body) = self.client.send(request).await?.into_parts();
        match parts.status {
            StatusCode::OK => {
                let decoded_response: DropboxMetadataResponse = body.to_json().await?;
                let entry_mode: EntryMode = match decoded_response.tag.as_str() {
                    "file" => EntryMode::FILE,
                    "folder" => EntryMode::DIR,
                    _ => EntryMode::Unknown,
                };

                let mut metadata = Metadata::new(entry_mode);
                // Only set last_modified and size if entry_mode is FILE, because Dropbox API
                // returns last_modified and size only for files.
                // FYI: https://www.dropbox.com/developers/documentation/http/documentation#files-get_metadata
                if entry_mode == EntryMode::FILE {
                    let date_utc_last_modified =
                        parse_datetime_from_rfc3339(&decoded_response.client_modified)?;
                    metadata.set_last_modified(date_utc_last_modified);

                    if let Some(size) = decoded_response.size {
                        metadata.set_content_length(size);
                    } else {
                        return Err(Error::new(
                            ErrorKind::Unexpected,
                            &format!("no size found for file {}", path),
                        ));
                    }
                }
                Ok(metadata)
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub fn handle_batch_delete_complete_result(
        &self,
        entries: Vec<DropboxDeleteBatchResponseEntry>,
    ) -> Vec<(String, Result<BatchedReply>)> {
        let mut results = Vec::with_capacity(entries.len());
        for entry in entries {
            let result = match entry.tag.as_str() {
                // Only success response has metadata and then path,
                // so we cannot tell which path failed.
                "success" => {
                    let path = entry
                        .metadata
                        .expect("metadata should be present")
                        .path_display;
                    (path, Ok(RpDelete::default().into()))
                }
                "failure" => {
                    let error = entry.failure.expect("error should be present");
                    let error_cause = &error
                        .failure_cause_map
                        .get(&error.tag)
                        .expect("error should be present")
                        .tag;
                    // Ignore errors about path lookup not found and report others.
                    if error.tag == "path_lookup" && error_cause == "not_found" {
                        ("".to_string(), Ok(RpDelete::default().into()))
                    } else {
                        let err = Error::new(
                            ErrorKind::Unexpected,
                            &format!("delete failed with error {} {}", error.tag, error_cause),
                        );
                        ("".to_string(), Err(err))
                    }
                }
                _ => (
                    "".to_string(),
                    Err(Error::new(
                        ErrorKind::Unexpected,
                        &format!("delete failed with unexpected tag {}", entry.tag),
                    )),
                ),
            };
            results.push(result);
        }
        results
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
    pub failure: Option<DropboxDeleteBatchFailureResponse>,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default)]
pub struct DropboxDeleteBatchFailureResponse {
    #[serde(rename(deserialize = ".tag"))]
    pub tag: String,
    // During the batch deletion process, Dropbox returns
    // part of the error information in the form of a JSON key.
    // Since it is impossible to determine the JSON key in advance,
    // the error information is parsed into a HashMap here.
    // The key of the HashMap is equal to the value of the tag above.
    #[serde(flatten)]
    pub failure_cause_map: HashMap<String, DropboxDeleteBatchFailureResponseCause>,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default)]
pub struct DropboxDeleteBatchFailureResponseCause {
    #[serde(rename(deserialize = ".tag"))]
    pub tag: String,
}
