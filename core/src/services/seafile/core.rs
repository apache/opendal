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
use http::header;
use http::Request;
use http::Response;
use http::StatusCode;
use serde::Deserialize;
use tokio::sync::RwLock;

use super::error::parse_error;
use crate::raw::*;
use crate::*;

/// Core of [seafile](https://www.seafile.com) services support.
#[derive(Clone)]
pub struct SeafileCore {
    /// The root of this core.
    pub root: String,
    /// The endpoint of this backend.
    pub endpoint: String,
    /// The username of this backend.
    pub username: String,
    /// The password id of this backend.
    pub password: String,
    /// The repo name of this backend.
    pub repo_name: String,

    /// signer of this backend.
    pub signer: Arc<RwLock<SeafileSigner>>,

    pub client: HttpClient,
}

impl Debug for SeafileCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend")
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .field("username", &self.username)
            .field("repo_name", &self.repo_name)
            .finish_non_exhaustive()
    }
}

impl SeafileCore {
    /// get auth info
    pub async fn get_auth_info(&self) -> Result<AuthInfo> {
        {
            let signer = self.signer.read().await;

            if !signer.auth_info.token.is_empty() {
                let auth_info = signer.auth_info.clone();
                return Ok(auth_info.clone());
            }
        }

        {
            let mut signer = self.signer.write().await;
            let body = format!(
                "username={}&password={}",
                percent_encode_path(&self.username),
                percent_encode_path(&self.password)
            );
            let req = Request::post(format!("{}/api2/auth-token/", self.endpoint))
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(RequestBody::Bytes(Bytes::from(body)))
                .map_err(new_request_build_error)?;

            let (parts, body) = self.client.send(req).await?.into_parts();

            match parts.status {
                StatusCode::OK => {
                    let auth_response: AuthTokenResponse = body.to_json().await?;
                    signer.auth_info = AuthInfo {
                        token: auth_response.token,
                        repo_id: "".to_string(),
                    };
                }
                _ => {
                    {
                        let bs = body.to_bytes().await?;
                        return Err(parse_error(parts, bs)?);
                    };
                }
            }

            let url = format!("{}/api2/repos", self.endpoint);

            let req = Request::get(url)
                .header(
                    header::AUTHORIZATION,
                    format!("Token {}", signer.auth_info.token),
                )
                .body(RequestBody::Empty)
                .map_err(new_request_build_error)?;

            let (parts, body) = self.client.send(req).await?.into_parts();

            match parts.status {
                StatusCode::OK => {
                    let list_library_response: Vec<ListLibraryResponse> = body.to_json().await?;

                    for library in list_library_response {
                        if library.name == self.repo_name {
                            signer.auth_info.repo_id = library.id;
                            break;
                        }
                    }

                    // repo not found
                    if signer.auth_info.repo_id.is_empty() {
                        return Err(Error::new(
                            ErrorKind::NotFound,
                            &format!("repo {} not found", self.repo_name),
                        ));
                    }
                }
                _ => {
                    {
                        let bs = body.to_bytes().await?;
                        return Err(parse_error(parts, bs)?);
                    };
                }
            }
            Ok(signer.auth_info.clone())
        }
    }
}

impl SeafileCore {
    /// get upload url
    pub async fn get_upload_url(&self) -> Result<String> {
        let auth_info = self.get_auth_info().await?;

        let req = Request::get(format!(
            "{}/api2/repos/{}/upload-link/",
            self.endpoint, auth_info.repo_id
        ));

        let req = req
            .header(header::AUTHORIZATION, format!("Token {}", auth_info.token))
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();

        match parts.status {
            StatusCode::OK => {
                // FIXME: use to_json here seems incorrect.
                let upload_url = body.to_json().await?;
                Ok(upload_url)
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    /// get download
    pub async fn get_download_url(&self, path: &str) -> Result<String> {
        let path = build_abs_path(&self.root, path);
        let path = percent_encode_path(&path);

        let auth_info = self.get_auth_info().await?;

        let req = Request::get(format!(
            "{}/api2/repos/{}/file/?p={}",
            self.endpoint, auth_info.repo_id, path
        ));

        let req = req
            .header(header::AUTHORIZATION, format!("Token {}", auth_info.token))
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();

        match parts.status {
            StatusCode::OK => {
                let download_url = body.to_json().await?;
                Ok(download_url)
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    /// download file
    pub async fn download_file(
        &self,
        path: &str,
        range: BytesRange,
        buf: oio::WritableBuf,
    ) -> Result<usize> {
        let download_url = self.get_download_url(path).await?;

        let req = Request::get(download_url);

        let req = req
            .header(header::RANGE, range.to_header())
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
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

    /// file detail
    pub async fn file_detail(&self, path: &str) -> Result<FileDetail> {
        let path = build_abs_path(&self.root, path);
        let path = percent_encode_path(&path);

        let auth_info = self.get_auth_info().await?;

        let req = Request::get(format!(
            "{}/api2/repos/{}/file/detail/?p={}",
            self.endpoint, auth_info.repo_id, path
        ));

        let req = req
            .header(header::AUTHORIZATION, format!("Token {}", auth_info.token))
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();

        match parts.status {
            StatusCode::OK => {
                let file_detail: FileDetail = body.to_json().await?;
                Ok(file_detail)
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    /// dir detail
    pub async fn dir_detail(&self, path: &str) -> Result<DirDetail> {
        let path = build_abs_path(&self.root, path);
        let path = percent_encode_path(&path);

        let auth_info = self.get_auth_info().await?;

        let req = Request::get(format!(
            "{}/api/v2.1/repos/{}/dir/detail/?path={}",
            self.endpoint, auth_info.repo_id, path
        ));

        let req = req
            .header(header::AUTHORIZATION, format!("Token {}", auth_info.token))
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();

        match parts.status {
            StatusCode::OK => {
                let dir_detail: DirDetail = body.to_json().await?;
                Ok(dir_detail)
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    /// create dir
    pub async fn create_dir(&self, path: &str) -> Result<()> {
        let path = build_abs_path(&self.root, path);
        let path = format!("/{}", &path[..path.len() - 1]);
        let path = percent_encode_path(&path);

        let auth_info = self.get_auth_info().await?;

        let req = Request::post(format!(
            "{}/api2/repos/{}/dir/?p={}",
            self.endpoint, auth_info.repo_id, path,
        ));

        let body = "operation=mkdir".to_string();

        let req = req
            .header(header::AUTHORIZATION, format!("Token {}", auth_info.token))
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(RequestBody::Bytes(Bytes::from(body)))
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::CREATED => {
                body.consume().await?;
                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    /// delete file or dir
    pub async fn delete(&self, path: &str) -> Result<()> {
        let path = build_abs_path(&self.root, path);
        let path = percent_encode_path(&path);

        let auth_info = self.get_auth_info().await?;

        let url = if path.ends_with('/') {
            format!(
                "{}/api2/repos/{}/dir/?p={}",
                self.endpoint, auth_info.repo_id, path
            )
        } else {
            format!(
                "{}/api2/repos/{}/file/?p={}",
                self.endpoint, auth_info.repo_id, path
            )
        };

        let req = Request::delete(url);

        let req = req
            .header(header::AUTHORIZATION, format!("Token {}", auth_info.token))
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();

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
}

#[derive(Deserialize)]
pub struct AuthTokenResponse {
    pub token: String,
}

#[derive(Deserialize)]
pub struct FileDetail {
    pub last_modified: String,
    pub size: u64,
}

#[derive(Debug, Deserialize)]
pub struct DirDetail {
    mtime: String,
}

pub fn parse_dir_detail(dir_detail: DirDetail) -> Result<Metadata> {
    let mut md = Metadata::new(EntryMode::DIR);

    md.set_last_modified(parse_datetime_from_rfc3339(&dir_detail.mtime)?);

    Ok(md)
}

pub fn parse_file_detail(file_detail: FileDetail) -> Result<Metadata> {
    let mut md = Metadata::new(EntryMode::FILE);

    md.set_content_length(file_detail.size);
    md.set_last_modified(parse_datetime_from_rfc3339(&file_detail.last_modified)?);

    Ok(md)
}

#[derive(Clone, Default)]
pub struct SeafileSigner {
    pub auth_info: AuthInfo,
}

#[derive(Clone, Default)]
pub struct AuthInfo {
    /// The repo id of this auth info.
    pub repo_id: String,
    /// The token of this auth info,
    pub token: String,
}

#[derive(Deserialize)]
pub struct ListLibraryResponse {
    pub name: String,
    pub id: String,
}
