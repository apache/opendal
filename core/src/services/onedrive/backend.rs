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

use async_trait::async_trait;
use http::header;
use http::Request;
use http::Response;
use http::StatusCode;

use super::error::parse_error;
use super::graph_model::ItemType;
use super::graph_model::OnedriveGetItemBody;
use super::pager::OnedrivePager;
use super::writer::OneDriveWriter;
use crate::{
    ops::{OpCreate, OpDelete, OpList, OpRead, OpStat, OpWrite},
    raw::{
        build_abs_path, build_rooted_abs_path, get_basename, new_json_deserialize_error,
        new_json_serialize_error, new_request_build_error, parse_datetime_from_rfc3339,
        parse_into_metadata, parse_location, percent_encode_path, Accessor, AccessorInfo,
        AsyncBody, HttpClient, IncomingAsyncBody, RpCreate, RpDelete, RpList, RpRead, RpStat,
        RpWrite,
    },
    types::Result,
    Capability, EntryMode, Error, ErrorKind, Metadata,
};

#[derive(Clone)]
pub struct OnedriveBackend {
    root: String,
    access_token: String,
    client: HttpClient,
}

impl OnedriveBackend {
    pub(crate) fn new(root: String, access_token: String, http_client: HttpClient) -> Self {
        Self {
            root,
            access_token,
            client: http_client,
        }
    }
}

impl Debug for OnedriveBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("OneDriveBackend");
        de.field("root", &self.root);
        de.field("access_token", &self.access_token);
        de.finish()
    }
}

#[async_trait]
impl Accessor for OnedriveBackend {
    type Reader = IncomingAsyncBody;
    type BlockingReader = ();
    type Writer = OneDriveWriter;
    type BlockingWriter = ();
    type Pager = OnedrivePager;
    type BlockingPager = ();

    fn info(&self) -> AccessorInfo {
        let mut ma = AccessorInfo::default();
        ma.set_scheme(crate::Scheme::Onedrive)
            .set_root(&self.root)
            .set_capability(Capability {
                read: true,
                write: true,
                stat: true,
                list: true,
                delete: true,
                create_dir: true,
                ..Default::default()
            });

        ma
    }

    async fn read(&self, path: &str, _args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.onedrive_get(path, true).await?;

        let status = resp.status();

        if status.is_redirection() {
            let headers = resp.headers();
            let location = parse_location(headers)?;
            match location {
                None => {
                    return Err(Error::new(
                        ErrorKind::ContentIncomplete,
                        "redirect location not found in response",
                    ));
                }
                Some(location) => {
                    let resp = self.onedrive_get_redirection(location).await?;
                    let meta = parse_into_metadata(path, resp.headers())?;
                    Ok((RpRead::with_metadata(meta), resp.into_body()))
                }
            }
        } else {
            match status {
                StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                    let meta = parse_into_metadata(path, resp.headers())?;
                    Ok((RpRead::with_metadata(meta), resp.into_body()))
                }

                _ => Err(parse_error(resp).await?),
            }
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        if args.content_length().is_none() {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "write without content length is not supported",
            ));
        }

        let path = build_rooted_abs_path(&self.root, path);

        Ok((
            RpWrite::default(),
            OneDriveWriter::new(self.clone(), args, path),
        ))
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        let resp = self.onedrive_get(path, false).await?;
        let status = resp.status();

        if status.is_success() {
            let bytes = resp.into_body().bytes().await?;
            let decoded_response = serde_json::from_slice::<OnedriveGetItemBody>(&bytes)
                .map_err(new_json_deserialize_error)?;

            let entry_mode: EntryMode = match decoded_response.item_type {
                ItemType::Folder { .. } => EntryMode::DIR,
                ItemType::File { .. } => EntryMode::FILE,
            };

            let mut meta = Metadata::new(entry_mode);
            meta.set_etag(&decoded_response.e_tag);

            let last_modified = decoded_response.last_modified_date_time;
            let date_utc_last_modified = parse_datetime_from_rfc3339(&last_modified)?;
            meta.set_last_modified(date_utc_last_modified);

            meta.set_content_length(decoded_response.size);

            Ok(RpStat::new(meta))
        } else {
            match status {
                StatusCode::NOT_FOUND if path.ends_with('/') => {
                    Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
                }
                _ => Err(parse_error(resp).await?),
            }
        }
    }

    /// Delete operation
    /// Documentation: https://learn.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_delete?view=odsp-graph-online
    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let resp = self.onedrive_delete(path).await?;

        let status = resp.status();

        match status {
            StatusCode::NO_CONTENT | StatusCode::NOT_FOUND => Ok(RpDelete::default()),
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn list(&self, path: &str, _op_list: OpList) -> Result<(RpList, Self::Pager)> {
        let pager: OnedrivePager = OnedrivePager::new(
            self.root.clone(),
            path.into(),
            self.access_token.clone(),
            self.client.clone(),
        );

        Ok((RpList::default(), pager))
    }

    async fn create_dir(&self, path: &str, _: OpCreate) -> Result<RpCreate> {
        let path = build_rooted_abs_path(&self.root, path);
        let path_before_last_slash = std::path::Path::new(&path)
            .parent()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "invalid path"))?
            .to_str()
            .unwrap_or(&path);
        let encoded_path = percent_encode_path(path_before_last_slash);

        let uri = format!(
            "https://graph.microsoft.com/v1.0/me/drive/root:{}:/children",
            encoded_path
        );

        let folder_name = get_basename(&path);
        let folder_name = folder_name.strip_suffix('/').unwrap_or(folder_name);

        let body = serde_json::json!({
            "name": folder_name,
            "folder": {},
            "@microsoft.graph.conflictBehavior": "replace"
        });
        let body_bytes = serde_json::to_vec(&body).map_err(new_json_serialize_error)?;
        let async_body = AsyncBody::Bytes(bytes::Bytes::from(body_bytes));

        let response = self
            .onedrive_post(&uri, async_body, Some("application/json"))
            .await?;

        let status = response.status();
        match status {
            StatusCode::CREATED | StatusCode::OK => Ok(RpCreate::default()),
            _ => Err(parse_error(response).await?),
        }
    }
}

impl OnedriveBackend {
    pub(crate) const BASE_URL: &'static str = "https://graph.microsoft.com/v1.0/me";
    async fn onedrive_get(
        &self,
        path: &str,
        append_content_suffix: bool,
    ) -> Result<Response<IncomingAsyncBody>> {
        let path = build_rooted_abs_path(&self.root, path);
        let url: String = format!(
            "https://graph.microsoft.com/v1.0/me/drive/root:{}{}",
            percent_encode_path(&path),
            if append_content_suffix {
                ":/content"
            } else {
                ""
            }
        );

        let mut req = Request::get(&url);

        let auth_header_content = format!("Bearer {}", self.access_token);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    async fn onedrive_get_redirection(&self, url: &str) -> Result<Response<IncomingAsyncBody>> {
        let mut req = Request::get(url);

        let auth_header_content = format!("Bearer {}", self.access_token);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    pub async fn onedrive_put(
        &self,
        path: &str,
        size: Option<usize>,
        content_type: Option<&str>,
        body: AsyncBody,
    ) -> Result<Response<IncomingAsyncBody>> {
        let url = format!(
            "https://graph.microsoft.com/v1.0/me/drive/root:{}:/content",
            percent_encode_path(path)
        );

        let mut req = Request::put(&url);

        let auth_header_content = format!("Bearer {}", self.access_token);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        if let Some(size) = size {
            req = req.header(header::CONTENT_LENGTH, size)
        }

        if let Some(mime) = content_type {
            req = req.header(header::CONTENT_TYPE, mime)
        }

        let req = req.body(body).map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    pub(crate) async fn onedrive_custom_put(
        &self,
        url: &str,
        size: Option<usize>,
        content_type: Option<&str>,
        additional_headers: Option<HashMap<String, String>>,
        body: AsyncBody,
    ) -> Result<Response<IncomingAsyncBody>> {
        let mut req = Request::put(url);

        let auth_header_content = format!("Bearer {}", self.access_token);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        if let Some(headers) = additional_headers {
            for (key, value) in headers {
                req = req.header(key, value);
            }
        }

        if let Some(size) = size {
            req = req.header(header::CONTENT_LENGTH, size)
        }

        if let Some(mime) = content_type {
            req = req.header(header::CONTENT_TYPE, mime)
        }

        let req = req.body(body).map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    pub(crate) async fn onedrive_post(
        &self,
        url: &str,
        body: AsyncBody,
        content_type: Option<&str>,
    ) -> Result<Response<IncomingAsyncBody>> {
        let mut req = Request::post(url);

        let auth_header_content = format!("Bearer {}", self.access_token);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        if let Some(mime) = content_type {
            req = req.header(header::CONTENT_TYPE, mime)
        }

        let req = req.body(body).map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    pub(crate) async fn onedrive_delete(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let path = build_abs_path(&self.root, path);
        let url = format!(
            "https://graph.microsoft.com/v1.0/me/drive/root:/{}",
            percent_encode_path(&path)
        );

        let mut req = Request::delete(&url);

        let auth_header_content = format!("Bearer {}", self.access_token);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }
}
