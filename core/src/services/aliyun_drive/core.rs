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
use std::sync::Arc;

use bytes::Buf;
use chrono::Utc;
use http::header::HeaderValue;
use http::header::{self};
use http::Method;
use http::Request;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Mutex;

use super::error::parse_error;
use crate::raw::*;
use crate::*;

/// Available Aliyun Drive Type.
#[derive(Debug, Deserialize, Default, Clone)]
pub enum DriveType {
    /// Use the default type of Aliyun Drive.
    #[default]
    Default,
    /// Use the backup type of Aliyun Drive.
    ///
    /// Fallback to the default type if no backup drive is found.
    Backup,
    /// Use the resource type of Aliyun Drive.
    ///
    /// Fallback to the default type if no resource drive is found.
    Resource,
}

/// Available Aliyun Drive Signer Set
pub enum AliyunDriveSign {
    Refresh(String, String, String, Option<String>, i64),
    Access(String),
}

pub struct AliyunDriveSigner {
    pub drive_id: Option<String>,
    pub sign: AliyunDriveSign,
}

pub struct AliyunDriveCore {
    pub info: Arc<AccessorInfo>,

    pub endpoint: String,
    pub root: String,
    pub drive_type: DriveType,

    pub signer: Arc<Mutex<AliyunDriveSigner>>,
    pub dir_lock: Arc<Mutex<()>>,
}

impl Debug for AliyunDriveCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AliyunDriveCore")
            .field("root", &self.root)
            .field("drive_type", &self.drive_type)
            .finish_non_exhaustive()
    }
}

impl AliyunDriveCore {
    async fn send(&self, mut req: Request<Buffer>, token: Option<&str>) -> Result<Buffer> {
        // AliyunDrive raise NullPointerException if you haven't set a user-agent.
        req.headers_mut().insert(
            header::USER_AGENT,
            HeaderValue::from_str(&format!("opendal/{}", VERSION))
                .expect("user agent must be valid header value"),
        );
        if req.method() == Method::POST {
            req.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json;charset=UTF-8"),
            );
        }
        if let Some(token) = token {
            req.headers_mut().insert(
                header::AUTHORIZATION,
                HeaderValue::from_str(&format_authorization_by_bearer(token)?)
                    .expect("access token must be valid header value"),
            );
        }
        let res = self.info.http_client().send(req).await?;
        if !res.status().is_success() {
            return Err(parse_error(res));
        }
        Ok(res.into_body())
    }

    async fn get_access_token(
        &self,
        client_id: &str,
        client_secret: &str,
        refresh_token: &str,
    ) -> Result<Buffer> {
        let body = serde_json::to_vec(&AccessTokenRequest {
            refresh_token,
            grant_type: "refresh_token",
            client_id,
            client_secret,
        })
        .map_err(new_json_serialize_error)?;
        let req = Request::post(format!("{}/oauth/access_token", self.endpoint))
            .body(Buffer::from(body))
            .map_err(new_request_build_error)?;
        self.send(req, None).await
    }

    async fn get_drive_id(&self, token: Option<&str>) -> Result<Buffer> {
        let req = Request::post(format!("{}/adrive/v1.0/user/getDriveInfo", self.endpoint))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;
        self.send(req, token).await
    }

    pub async fn get_token_and_drive(&self) -> Result<(Option<String>, String)> {
        let mut signer = self.signer.lock().await;
        let token = match &mut signer.sign {
            AliyunDriveSign::Access(access_token) => Some(access_token.clone()),
            AliyunDriveSign::Refresh(
                client_id,
                client_secret,
                refresh_token,
                access_token,
                expire_at,
            ) => {
                if *expire_at < Utc::now().timestamp() || access_token.is_none() {
                    let res = self
                        .get_access_token(client_id, client_secret, refresh_token)
                        .await?;
                    let output: RefreshTokenResponse = serde_json::from_reader(res.reader())
                        .map_err(new_json_deserialize_error)?;
                    *access_token = Some(output.access_token);
                    *expire_at = output.expires_in + Utc::now().timestamp();
                    *refresh_token = output.refresh_token;
                }
                access_token.clone()
            }
        };
        let Some(drive_id) = &signer.drive_id else {
            let res = self.get_drive_id(token.as_deref()).await?;
            let output: DriveInfoResponse =
                serde_json::from_reader(res.reader()).map_err(new_json_deserialize_error)?;
            let drive_id = match self.drive_type {
                DriveType::Default => output.default_drive_id,
                DriveType::Backup => output.backup_drive_id.unwrap_or(output.default_drive_id),
                DriveType::Resource => output.resource_drive_id.unwrap_or(output.default_drive_id),
            };
            signer.drive_id = Some(drive_id.clone());
            return Ok((token, drive_id));
        };
        Ok((token, drive_id.clone()))
    }

    pub fn build_path(&self, path: &str, rooted: bool) -> String {
        let file_path = if rooted {
            build_rooted_abs_path(&self.root, path)
        } else {
            build_abs_path(&self.root, path)
        };
        let file_path = file_path.strip_suffix('/').unwrap_or(&file_path);
        if file_path.is_empty() {
            return "/".to_string();
        }
        file_path.to_string()
    }

    pub async fn get_by_path(&self, path: &str) -> Result<Buffer> {
        let file_path = self.build_path(path, true);
        let req = Request::post(format!(
            "{}/adrive/v1.0/openFile/get_by_path",
            self.endpoint
        ));
        let (token, drive_id) = self.get_token_and_drive().await?;
        let body = serde_json::to_vec(&GetByPathRequest {
            drive_id: &drive_id,
            file_path: &file_path,
        })
        .map_err(new_json_serialize_error)?;
        let req = req
            // Inject operation to the request.
            .extension(Operation::Read)
            .body(Buffer::from(body))
            .map_err(new_request_build_error)?;
        self.send(req, token.as_deref()).await
    }

    pub async fn ensure_dir_exists(&self, path: &str) -> Result<String> {
        let file_path = self.build_path(path, false);
        if file_path == "/" {
            return Ok("root".to_string());
        }
        let file_path = file_path.strip_suffix('/').unwrap_or(&file_path);
        let paths = file_path.split('/').collect::<Vec<&str>>();
        let mut parent: Option<String> = None;
        for path in paths {
            let _guard = self.dir_lock.lock().await;
            let res = self
                .create(
                    parent.as_deref(),
                    path,
                    CreateType::Folder,
                    CheckNameMode::Refuse,
                )
                .await?;
            let output: CreateResponse =
                serde_json::from_reader(res.reader()).map_err(new_json_deserialize_error)?;
            parent = Some(output.file_id);
        }
        Ok(parent.expect("ensure_dir_exists must succeed"))
    }

    pub async fn create_with_rapid_upload(
        &self,
        parent_file_id: Option<&str>,
        name: &str,
        typ: CreateType,
        check_name_mode: CheckNameMode,
        size: Option<u64>,
        rapid_upload: Option<RapidUpload>,
    ) -> Result<Buffer> {
        let mut content_hash = None;
        let mut proof_code = None;
        let mut pre_hash = None;
        if let Some(rapid_upload) = rapid_upload {
            content_hash = rapid_upload.content_hash;
            proof_code = rapid_upload.proof_code;
            pre_hash = rapid_upload.pre_hash;
        }

        let (token, drive_id) = self.get_token_and_drive().await?;
        let body = serde_json::to_vec(&CreateRequest {
            drive_id: &drive_id,
            parent_file_id: parent_file_id.unwrap_or("root"),
            name,
            typ,
            check_name_mode,
            size,
            pre_hash: pre_hash.as_deref(),
            content_hash: content_hash.as_deref(),
            content_hash_name: content_hash.is_some().then_some("sha1"),
            proof_code: proof_code.as_deref(),
            proof_version: proof_code.is_some().then_some("v1"),
        })
        .map_err(new_json_serialize_error)?;
        let req = Request::post(format!("{}/adrive/v1.0/openFile/create", self.endpoint))
            // Inject operation to the request.
            .extension(Operation::Write)
            .body(Buffer::from(body))
            .map_err(new_request_build_error)?;
        self.send(req, token.as_deref()).await
    }

    pub async fn create(
        &self,
        parent_file_id: Option<&str>,
        name: &str,
        typ: CreateType,
        check_name_mode: CheckNameMode,
    ) -> Result<Buffer> {
        self.create_with_rapid_upload(parent_file_id, name, typ, check_name_mode, None, None)
            .await
    }

    pub async fn get_download_url(&self, file_id: &str) -> Result<String> {
        let (token, drive_id) = self.get_token_and_drive().await?;
        let body = serde_json::to_vec(&FileRequest {
            drive_id: &drive_id,
            file_id,
        })
        .map_err(new_json_serialize_error)?;
        let req = Request::post(format!(
            "{}/adrive/v1.0/openFile/getDownloadUrl",
            self.endpoint
        ))
        // Inject operation to the request.
        .extension(Operation::Read)
        .body(Buffer::from(body))
        .map_err(new_request_build_error)?;
        let res = self.send(req, token.as_deref()).await?;
        let output: GetDownloadUrlResponse =
            serde_json::from_reader(res.reader()).map_err(new_json_serialize_error)?;
        Ok(output.url)
    }

    pub async fn move_path(&self, file_id: &str, to_parent_file_id: &str) -> Result<()> {
        let (token, drive_id) = self.get_token_and_drive().await?;
        let body = serde_json::to_vec(&MovePathRequest {
            drive_id: &drive_id,
            file_id,
            to_parent_file_id,
            check_name_mode: CheckNameMode::AutoRename,
        })
        .map_err(new_json_serialize_error)?;
        let req = Request::post(format!("{}/adrive/v1.0/openFile/move", self.endpoint))
            // Inject operation to the request.
            .extension(Operation::Write)
            .body(Buffer::from(body))
            .map_err(new_request_build_error)?;
        self.send(req, token.as_deref()).await?;
        Ok(())
    }

    pub async fn update_path(&self, file_id: &str, name: &str) -> Result<()> {
        let (token, drive_id) = self.get_token_and_drive().await?;
        let body = serde_json::to_vec(&UpdatePathRequest {
            drive_id: &drive_id,
            file_id,
            name,
            check_name_mode: CheckNameMode::Refuse,
        })
        .map_err(new_json_serialize_error)?;
        let req = Request::post(format!("{}/adrive/v1.0/openFile/update", self.endpoint))
            // Inject operation to the request.
            .extension(Operation::Write)
            .body(Buffer::from(body))
            .map_err(new_request_build_error)?;
        self.send(req, token.as_deref()).await?;
        Ok(())
    }

    pub async fn copy_path(
        &self,
        file_id: &str,
        to_parent_file_id: &str,
        auto_rename: bool,
    ) -> Result<Buffer> {
        let (token, drive_id) = self.get_token_and_drive().await?;
        let body = serde_json::to_vec(&CopyPathRequest {
            drive_id: &drive_id,
            file_id,
            to_parent_file_id,
            auto_rename,
        })
        .map_err(new_json_serialize_error)?;
        let req = Request::post(format!("{}/adrive/v1.0/openFile/copy", self.endpoint))
            // Inject operation to the request.
            .extension(Operation::Copy)
            .body(Buffer::from(body))
            .map_err(new_request_build_error)?;
        self.send(req, token.as_deref()).await
    }

    pub async fn delete_path(&self, file_id: &str) -> Result<()> {
        let (token, drive_id) = self.get_token_and_drive().await?;
        let body = serde_json::to_vec(&FileRequest {
            drive_id: &drive_id,
            file_id,
        })
        .map_err(new_json_serialize_error)?;
        let req = Request::post(format!("{}/adrive/v1.0/openFile/delete", self.endpoint))
            // Inject operation to the request.
            .extension(Operation::Delete)
            .body(Buffer::from(body))
            .map_err(new_request_build_error)?;
        self.send(req, token.as_deref()).await?;
        Ok(())
    }

    pub async fn list(
        &self,
        parent_file_id: &str,
        limit: Option<usize>,
        marker: Option<String>,
    ) -> Result<Buffer> {
        let (token, drive_id) = self.get_token_and_drive().await?;
        let body = serde_json::to_vec(&ListRequest {
            drive_id: &drive_id,
            parent_file_id,
            limit,
            marker: marker.as_deref(),
        })
        .map_err(new_json_serialize_error)?;
        let req = Request::post(format!("{}/adrive/v1.0/openFile/list", self.endpoint))
            // Inject operation to the request.
            .extension(Operation::List)
            .body(Buffer::from(body))
            .map_err(new_request_build_error)?;
        self.send(req, token.as_deref()).await
    }

    pub async fn upload(&self, upload_url: &str, body: Buffer) -> Result<Buffer> {
        let req = Request::put(upload_url)
            // Inject operation to the request.
            .extension(Operation::Write)
            .body(body)
            .map_err(new_request_build_error)?;
        self.send(req, None).await
    }

    pub async fn complete(&self, file_id: &str, upload_id: &str) -> Result<Buffer> {
        let (token, drive_id) = self.get_token_and_drive().await?;
        let body = serde_json::to_vec(&CompleteRequest {
            drive_id: &drive_id,
            file_id,
            upload_id,
        })
        .map_err(new_json_serialize_error)?;
        let req = Request::post(format!("{}/adrive/v1.0/openFile/complete", self.endpoint))
            // Inject operation to the request.
            .extension(Operation::Write)
            .body(Buffer::from(body))
            .map_err(new_request_build_error)?;
        self.send(req, token.as_deref()).await
    }

    pub async fn get_upload_url(
        &self,
        file_id: &str,
        upload_id: &str,
        part_number: Option<usize>,
    ) -> Result<Buffer> {
        let (token, drive_id) = self.get_token_and_drive().await?;
        let part_info_list = part_number.map(|part_number| {
            vec![PartInfoItem {
                part_number: Some(part_number),
            }]
        });
        let body = serde_json::to_vec(&GetUploadRequest {
            drive_id: &drive_id,
            file_id,
            upload_id,
            part_info_list,
        })
        .map_err(new_json_serialize_error)?;
        let req = Request::post(format!(
            "{}/adrive/v1.0/openFile/getUploadUrl",
            self.endpoint
        ))
        // Inject operation to the request.
        .extension(Operation::Write)
        .body(Buffer::from(body))
        .map_err(new_request_build_error)?;
        self.send(req, token.as_deref()).await
    }
}

pub struct RapidUpload {
    pub pre_hash: Option<String>,
    pub content_hash: Option<String>,
    pub proof_code: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct RefreshTokenResponse {
    pub access_token: String,
    pub expires_in: i64,
    pub refresh_token: String,
}

#[derive(Debug, Deserialize)]
pub struct DriveInfoResponse {
    pub default_drive_id: String,
    pub resource_drive_id: Option<String>,
    pub backup_drive_id: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CreateType {
    File,
    Folder,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckNameMode {
    Refuse,
    AutoRename,
}

#[derive(Deserialize)]
pub struct UploadUrlResponse {
    pub part_info_list: Option<Vec<PartInfo>>,
}

#[derive(Deserialize)]
pub struct CreateResponse {
    pub file_id: String,
    pub upload_id: Option<String>,
    pub exist: Option<bool>,
}

#[derive(Serialize, Deserialize)]
pub struct PartInfo {
    pub etag: Option<String>,
    pub part_number: usize,
    pub part_size: Option<u64>,
    pub upload_url: String,
    pub content_type: Option<String>,
}

#[derive(Deserialize)]
pub struct AliyunDriveFileList {
    pub items: Vec<AliyunDriveFile>,
    pub next_marker: Option<String>,
}

#[derive(Deserialize)]
pub struct CopyResponse {
    pub file_id: String,
}

#[derive(Deserialize)]
pub struct AliyunDriveFile {
    pub file_id: String,
    pub parent_file_id: String,
    pub name: String,
    pub size: Option<u64>,
    pub content_type: Option<String>,
    #[serde(rename = "type")]
    pub path_type: String,
    pub updated_at: String,
}

#[derive(Deserialize)]
pub struct GetDownloadUrlResponse {
    pub url: String,
}

#[derive(Serialize)]
pub struct AccessTokenRequest<'a> {
    refresh_token: &'a str,
    grant_type: &'a str,
    client_id: &'a str,
    client_secret: &'a str,
}

#[derive(Serialize)]
pub struct GetByPathRequest<'a> {
    drive_id: &'a str,
    file_path: &'a str,
}

#[derive(Serialize)]
pub struct CreateRequest<'a> {
    drive_id: &'a str,
    parent_file_id: &'a str,
    name: &'a str,
    #[serde(rename = "type")]
    typ: CreateType,
    check_name_mode: CheckNameMode,
    size: Option<u64>,
    pre_hash: Option<&'a str>,
    content_hash: Option<&'a str>,
    content_hash_name: Option<&'a str>,
    proof_code: Option<&'a str>,
    proof_version: Option<&'a str>,
}

#[derive(Serialize)]
pub struct FileRequest<'a> {
    drive_id: &'a str,
    file_id: &'a str,
}

#[derive(Serialize)]
pub struct MovePathRequest<'a> {
    drive_id: &'a str,
    file_id: &'a str,
    to_parent_file_id: &'a str,
    check_name_mode: CheckNameMode,
}

#[derive(Serialize)]
pub struct UpdatePathRequest<'a> {
    drive_id: &'a str,
    file_id: &'a str,
    name: &'a str,
    check_name_mode: CheckNameMode,
}

#[derive(Serialize)]
pub struct CopyPathRequest<'a> {
    drive_id: &'a str,
    file_id: &'a str,
    to_parent_file_id: &'a str,
    auto_rename: bool,
}

#[derive(Serialize)]
pub struct ListRequest<'a> {
    drive_id: &'a str,
    parent_file_id: &'a str,
    limit: Option<usize>,
    marker: Option<&'a str>,
}

#[derive(Serialize)]
pub struct CompleteRequest<'a> {
    drive_id: &'a str,
    file_id: &'a str,
    upload_id: &'a str,
}

#[derive(Serialize)]
pub struct GetUploadRequest<'a> {
    drive_id: &'a str,
    file_id: &'a str,
    upload_id: &'a str,
    part_info_list: Option<Vec<PartInfoItem>>,
}

#[derive(Serialize)]
pub struct PartInfoItem {
    part_number: Option<usize>,
}
