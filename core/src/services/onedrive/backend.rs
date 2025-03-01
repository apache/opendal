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
use bytes::Bytes;
use http::header;
use http::Request;
use http::Response;
use http::StatusCode;

use super::delete::OnedriveDeleter;
use super::error::parse_error;
use super::graph_model::CreateDirPayload;
use super::graph_model::ItemType;
use super::graph_model::OneDriveUploadSessionCreationRequestBody;
use super::graph_model::OnedriveGetItemBody;
use super::lister::OnedriveLister;
use super::writer::OneDriveWriter;
use crate::raw::*;
use crate::*;

#[derive(Clone)]
pub struct OnedriveBackend {
    info: Arc<AccessorInfo>,
    root: String,
    access_token: String,
    client: HttpClient,
}

impl OnedriveBackend {
    pub(crate) fn new(root: String, access_token: String, http_client: HttpClient) -> Self {
        Self {
            info: {
                let ma = AccessorInfo::default();
                ma.set_scheme(Scheme::Onedrive)
                    .set_root(&root)
                    .set_native_capability(Capability {
                        read: true,
                        write: true,
                        stat: true,
                        stat_has_etag: true,
                        stat_has_last_modified: true,
                        stat_has_content_length: true,
                        delete: true,
                        create_dir: true,
                        list: true,
                        list_has_content_length: true,
                        list_has_etag: true,
                        list_has_last_modified: true,
                        shared: true,
                        ..Default::default()
                    });

                ma.into()
            },
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
        de.field("access_token", &"<redacted>");
        de.finish()
    }
}

impl Access for OnedriveBackend {
    type Reader = HttpBody;
    type Writer = oio::OneShotWriter<OneDriveWriter>;
    type Lister = oio::PageLister<OnedriveLister>;
    type Deleter = oio::OneShotDeleter<OnedriveDeleter>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        self.info.clone()
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        if path == "/" {
            // skip, the root path exists in the personal OneDrive.
            return Ok(RpCreateDir::default());
        }

        let path = build_rooted_abs_path(&self.root, path);
        let path_before_last_slash = get_parent(&path);
        let encoded_path = percent_encode_path(path_before_last_slash);

        let uri = format!(
            "https://graph.microsoft.com/v1.0/me/drive/root:{}:/children",
            encoded_path
        );

        let folder_name = get_basename(&path);
        let folder_name = folder_name.strip_suffix('/').unwrap_or(folder_name);

        let body = CreateDirPayload::new(folder_name.to_string());

        let response = self.onedrive_create_dir(&uri, body).await?;

        let status = response.status();
        match status {
            StatusCode::CREATED | StatusCode::OK => Ok(RpCreateDir::default()),
            _ => Err(parse_error(response)),
        }
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        let resp = self.onedrive_get_stat(path).await?;
        let status = resp.status();

        if status.is_success() {
            let bytes = resp.into_body();
            let decoded_response: OnedriveGetItemBody =
                serde_json::from_reader(bytes.reader()).map_err(new_json_deserialize_error)?;

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
            Err(parse_error(resp))
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.onedrive_get_content(path, args.range()).await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                Ok((RpRead::default(), resp.into_body()))
            }
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                Err(parse_error(Response::from_parts(part, buf)))
            }
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let path = build_rooted_abs_path(&self.root, path);

        Ok((
            RpWrite::default(),
            oio::OneShotWriter::new(OneDriveWriter::new(self.clone(), args, path)),
        ))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(OnedriveDeleter::new(Arc::new(self.clone()))),
        ))
    }

    async fn list(&self, path: &str, _op_list: OpList) -> Result<(RpList, Self::Lister)> {
        let l = OnedriveLister::new(self.root.clone(), path.into(), self.clone());

        Ok((RpList::default(), oio::PageLister::new(l)))
    }
}

impl OnedriveBackend {
    pub(crate) const BASE_URL: &'static str = "https://graph.microsoft.com/v1.0/me";

    async fn onedrive_get_stat(&self, path: &str) -> Result<Response<Buffer>> {
        let path = build_rooted_abs_path(&self.root, path);
        let url: String = format!(
            "https://graph.microsoft.com/v1.0/me/drive/root:{}{}",
            percent_encode_path(&path),
            ""
        );

        let mut req = Request::get(&url);

        let auth_header_content = format!("Bearer {}", self.access_token);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    pub(crate) async fn onedrive_get_next_list_page(&self, url: &str) -> Result<Response<Buffer>> {
        let mut req = Request::get(url);

        let auth_header_content = format!("Bearer {}", self.access_token);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    pub async fn onedrive_get_content(
        &self,
        path: &str,
        range: BytesRange,
    ) -> Result<Response<HttpBody>> {
        let path = build_rooted_abs_path(&self.root, path);
        let url: String = format!(
            "https://graph.microsoft.com/v1.0/me/drive/root:{}{}",
            percent_encode_path(&path),
            ":/content"
        );

        let mut req = Request::get(&url).header(header::RANGE, range.to_header());

        let auth_header_content = format!("Bearer {}", self.access_token);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.client.fetch(req).await
    }

    pub async fn onedrive_upload_simple(
        &self,
        path: &str,
        size: Option<usize>,
        args: &OpWrite,
        body: Buffer,
    ) -> Result<Response<Buffer>> {
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

        if let Some(mime) = args.content_type() {
            req = req.header(header::CONTENT_TYPE, mime)
        }

        let req = req.body(body).map_err(new_request_build_error)?;

        self.client.send(req).await
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
        let mut req = Request::put(url);

        let auth_header_content = format!("Bearer {}", self.access_token);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        let range = format!("bytes {}-{}/{}", offset, chunk_end, total_len);
        req = req.header("Content-Range".to_string(), range);

        let size = chunk_end - offset + 1;
        req = req.header(header::CONTENT_LENGTH, size.to_string());

        if let Some(mime) = args.content_type() {
            req = req.header(header::CONTENT_TYPE, mime)
        }

        let req = req.body(body).map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    pub(crate) async fn onedrive_create_upload_session(
        &self,
        url: &str,
        body: OneDriveUploadSessionCreationRequestBody,
    ) -> Result<Response<Buffer>> {
        let mut req = Request::post(url);

        let auth_header_content = format!("Bearer {}", self.access_token);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        req = req.header(header::CONTENT_TYPE, "application/json");

        let body_bytes = serde_json::to_vec(&body).map_err(new_json_serialize_error)?;
        let asyn_body = Buffer::from(Bytes::from(body_bytes));
        let req = req.body(asyn_body).map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    async fn onedrive_create_dir(
        &self,
        url: &str,
        body: CreateDirPayload,
    ) -> Result<Response<Buffer>> {
        let mut req = Request::post(url);

        let auth_header_content = format!("Bearer {}", self.access_token);
        req = req.header(header::AUTHORIZATION, auth_header_content);
        req = req.header(header::CONTENT_TYPE, "application/json");

        let body_bytes = serde_json::to_vec(&body).map_err(new_json_serialize_error)?;
        let async_body = Buffer::from(bytes::Bytes::from(body_bytes));
        let req = req.body(async_body).map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    pub(crate) async fn onedrive_delete(&self, path: &str) -> Result<Response<Buffer>> {
        let path = build_abs_path(&self.root, path);
        let url = format!(
            "https://graph.microsoft.com/v1.0/me/drive/root:/{}",
            percent_encode_path(&path)
        );

        let mut req = Request::delete(&url);

        let auth_header_content = format!("Bearer {}", self.access_token);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.client.send(req).await
    }
}
