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

use std::collections::VecDeque;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use bytes::Buf;
use bytes::Bytes;
use http::header;
use http::request;
use http::Request;
use http::Response;
use http::StatusCode;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::Mutex;
use tokio::sync::OnceCell;

use super::error::parse_error;
use crate::raw::*;
use crate::*;

#[derive(Clone)]
pub struct KoofrCore {
    /// The root of this core.
    pub root: String,
    /// The endpoint of this backend.
    pub endpoint: String,
    /// Koofr email
    pub email: String,
    /// Koofr password
    pub password: String,

    /// signer of this backend.
    pub signer: Arc<Mutex<KoofrSigner>>,

    // Koofr mount_id.
    pub mount_id: OnceCell<String>,

    pub client: HttpClient,
}

impl Debug for KoofrCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend")
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .field("email", &self.email)
            .finish_non_exhaustive()
    }
}

impl KoofrCore {
    pub async fn get_mount_id(&self) -> Result<&String> {
        self.mount_id
            .get_or_try_init(|| async {
                let req = Request::get(format!("{}/api/v2/mounts", self.endpoint));

                let req = self.sign(req).await?;

                let req = req
                    .body(RequestBody::Empty)
                    .map_err(new_request_build_error)?;

                let (parts, body) = self.client.send(req).await?.into_parts();

                if parts.status != StatusCode::OK {
                    let bs = body.to_bytes().await?;
                    return Err(parse_error(parts, bs)?);
                }

                let resp: MountsResponse = body.to_json().await?;
                for mount in resp.mounts {
                    if mount.is_primary {
                        return Ok(mount.id);
                    }
                }

                Err(Error::new(ErrorKind::Unexpected, "No primary mount found"))
            })
            .await
    }

    pub async fn sign(&self, req: request::Builder) -> Result<request::Builder> {
        let mut signer = self.signer.lock().await;
        if !signer.token.is_empty() {
            return Ok(req.header(
                header::AUTHORIZATION,
                format!("Token token={}", signer.token),
            ));
        }

        let url = format!("{}/token", self.endpoint);

        let body = json!({
            "email": self.email,
            "password": self.password,
        });

        let bs = serde_json::to_vec(&body).map_err(new_json_serialize_error)?;

        let auth_req = Request::post(url)
            .header(header::CONTENT_TYPE, "application/json")
            .body(RequestBody::Bytes(Bytes::from(bs)))
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(auth_req).await?.into_parts();

        if parts.status != StatusCode::OK {
            let bs = body.to_bytes().await?;
            return Err(parse_error(parts, bs).await?);
        }

        let resp: TokenResponse = body.to_json().await?;
        signer.token = resp.token;

        Ok(req.header(
            header::AUTHORIZATION,
            format!("Token token={}", signer.token),
        ))
    }
}

impl KoofrCore {
    pub async fn ensure_dir_exists(&self, path: &str) -> Result<()> {
        let mut dirs = VecDeque::default();

        let mut p = build_abs_path(&self.root, path);

        while p != "/" {
            let parent = get_parent(&p).to_string();

            dirs.push_front(parent.clone());
            p = parent;
        }

        for dir in dirs {
            self.create_dir(&dir).await?;
        }

        Ok(())
    }

    pub async fn create_dir(&self, path: &str) -> Result<()> {
        let resp = self.info(path).await;
        match resp {
            Ok(_) => return Ok(()),
            Err(e) if e.kind() != ErrorKind::NotFound => return Err(e),
            _ => (),
        };

        let name = get_basename(path).trim_end_matches('/');
        let parent = get_parent(path);

        let mount_id = self.get_mount_id().await?;

        let url = format!(
            "{}/api/v2/mounts/{}/files/folder?path={}",
            self.endpoint,
            mount_id,
            percent_encode_path(parent)
        );

        let body = json!({
            "name": name
        });

        let bs = serde_json::to_vec(&body).map_err(new_json_serialize_error)?;

        let req = Request::post(url);

        let req = self.sign(req).await?;

        let req = req
            .header(header::CONTENT_TYPE, "application/json")
            .body(RequestBody::Bytes(Bytes::from(bs)))
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            // When the directory already exists, Koofr returns 400 Bad Request.
            // We should treat it as success.
            StatusCode::OK | StatusCode::CREATED | StatusCode::BAD_REQUEST => {
                body.consume().await?;
                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn info(&self, path: &str) -> Result<Metadata> {
        let mount_id = self.get_mount_id().await?;

        let url = format!(
            "{}/api/v2/mounts/{}/files/info?path={}",
            self.endpoint,
            mount_id,
            percent_encode_path(path)
        );

        let req = Request::get(url);

        let req = self.sign(req).await?;

        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK => {
                let file: File = body.to_json().await?;

                let mode = if file.ty == "dir" {
                    EntryMode::DIR
                } else {
                    EntryMode::FILE
                };

                let mut md = Metadata::new(mode);

                md.set_content_length(file.size)
                    .set_content_type(&file.content_type)
                    .set_last_modified(parse_datetime_from_from_timestamp_millis(file.modified)?);

                Ok(md)
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn get(&self, path: &str, range: BytesRange, buf: oio::WritableBuf) -> Result<usize> {
        let path = build_rooted_abs_path(&self.root, path);

        let mount_id = self.get_mount_id().await?;

        let url = format!(
            "{}/api/v2/mounts/{}/files/get?path={}",
            self.endpoint,
            mount_id,
            percent_encode_path(&path)
        );

        let req = Request::get(url).header(header::RANGE, range.to_header());

        let req = self.sign(req).await?;

        let req = req
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

    pub async fn put(&self, path: &str, bs: Bytes) -> Result<()> {
        let path = build_rooted_abs_path(&self.root, path);

        let filename = get_basename(&path);
        let parent = get_parent(&path);

        let mount_id = self.get_mount_id().await?;

        let url = format!(
            "{}/content/api/v2/mounts/{}/files/put?path={}&filename={}&info=true&overwriteIgnoreNonexisting=&autorename=false&overwrite=true",
            self.endpoint,
            mount_id,
            percent_encode_path(parent),
            percent_encode_path(filename)
        );

        let file_part = FormDataPart::new("file")
            .header(
                header::CONTENT_DISPOSITION,
                format!("form-data; name=\"file\"; filename=\"{filename}\"")
                    .parse()
                    .unwrap(),
            )
            .content(bs);

        let multipart = Multipart::new().part(file_part);

        let req = Request::post(url);

        let req = self.sign(req).await?;

        let req = multipart.apply(req)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK | StatusCode::CREATED => {
                body.consume().await?;
                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn remove(&self, path: &str) -> Result<()> {
        let path = build_rooted_abs_path(&self.root, path);

        let mount_id = self.get_mount_id().await?;

        let url = format!(
            "{}/api/v2/mounts/{}/files/remove?path={}",
            self.endpoint,
            mount_id,
            percent_encode_path(&path)
        );

        let req = Request::delete(url);

        let req = self.sign(req).await?;

        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK |
            // Allow 404 when deleting a non-existing object
            StatusCode::NOT_FOUND  => {
                body.consume().await?;
                Ok(())
            },
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn copy(&self, from: &str, to: &str) -> Result<()> {
        let from = build_rooted_abs_path(&self.root, from);
        let to = build_rooted_abs_path(&self.root, to);

        let mount_id = self.get_mount_id().await?;

        let url = format!(
            "{}/api/v2/mounts/{}/files/copy?path={}",
            self.endpoint,
            mount_id,
            percent_encode_path(&from),
        );

        let body = json!({
            "toMountId": mount_id,
            "toPath": to,
        });

        let bs = serde_json::to_vec(&body).map_err(new_json_serialize_error)?;

        let req = Request::put(url);

        let req = self.sign(req).await?;

        let req = req
            .header(header::CONTENT_TYPE, "application/json")
            .body(RequestBody::Bytes(Bytes::from(bs)))
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

    pub async fn move_object(&self, from: &str, to: &str) -> Result<()> {
        let from = build_rooted_abs_path(&self.root, from);
        let to = build_rooted_abs_path(&self.root, to);

        let mount_id = self.get_mount_id().await?;

        let url = format!(
            "{}/api/v2/mounts/{}/files/move?path={}",
            self.endpoint,
            mount_id,
            percent_encode_path(&from),
        );

        let body = json!({
            "toMountId": mount_id,
            "toPath": to,
        });

        let bs = serde_json::to_vec(&body).map_err(new_json_serialize_error)?;

        let req = Request::put(url);

        let req = self.sign(req).await?;

        let req = req
            .header(header::CONTENT_TYPE, "application/json")
            .body(RequestBody::Bytes(Bytes::from(bs)))
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

    pub async fn list(&self, path: &str) -> Result<Response<oio::Buffer>> {
        let path = build_rooted_abs_path(&self.root, path);

        let mount_id = self.get_mount_id().await?;

        let url = format!(
            "{}/api/v2/mounts/{}/files/list?path={}",
            self.endpoint,
            mount_id,
            percent_encode_path(&path)
        );

        let req = Request::get(url);

        let req = self.sign(req).await?;

        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
    }
}

#[derive(Clone, Default)]
pub struct KoofrSigner {
    pub token: String,
}

#[derive(Debug, Deserialize)]
pub struct TokenResponse {
    pub token: String,
}

#[derive(Debug, Deserialize)]
pub struct MountsResponse {
    pub mounts: Vec<Mount>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Mount {
    pub id: String,
    pub is_primary: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListResponse {
    pub files: Vec<File>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct File {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub size: u64,
    pub modified: i64,
    pub content_type: String,
}
