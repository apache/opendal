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

use async_trait::async_trait;
use http::{header, Request, Response, StatusCode};
use serde::Deserialize;
use std::fmt::Debug;

use crate::{
    ops::{OpDelete, OpRead, OpWrite},
    raw::{
        build_rooted_abs_path, new_request_build_error, parse_into_metadata, Accessor,
        AccessorInfo, AsyncBody, HttpClient, IncomingAsyncBody, RpDelete, RpRead, RpWrite,
    },
    types::Result,
    Capability, Error, ErrorKind,
};

use super::{error::parse_error, writer::GdriveWriter};

#[derive(Clone)]
pub struct GdriveBackend {
    root: String,
    access_token: String,
    client: HttpClient,
}

impl GdriveBackend {
    pub(crate) fn new(root: String, access_token: String, http_client: HttpClient) -> Self {
        GdriveBackend {
            root,
            access_token,
            client: http_client,
        }
    }
}

impl Debug for GdriveBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("GoolgeDriveBackend");
        de.field("root", &self.root);
        de.finish()
    }
}

#[async_trait]
impl Accessor for GdriveBackend {
    type Reader = IncomingAsyncBody;
    type BlockingReader = ();
    type Writer = GdriveWriter;
    type BlockingWriter = ();
    type Pager = ();
    type BlockingPager = ();

    fn info(&self) -> AccessorInfo {
        let mut ma = AccessorInfo::default();
        ma.set_scheme(crate::Scheme::Gdrive)
            .set_root(&self.root)
            .set_capability(Capability {
                read: true,
                write: true,
                delete: true,
                ..Default::default()
            });

        ma
    }

    async fn read(&self, path: &str, _args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.gdrive_get(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let meta = parse_into_metadata(path, resp.headers())?;
                Ok((RpRead::with_metadata(meta), resp.into_body()))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        if args.content_length().is_none() {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "write without content length is not supported",
            ));
        }

        Ok((
            RpWrite::default(),
            GdriveWriter::new(self.clone(), args, String::from(path)),
        ))
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let resp = self.gdrive_delete(path).await?;

        let status = resp.status();

        match status {
            StatusCode::NO_CONTENT => Ok(RpDelete::default()),
            _ => Err(parse_error(resp).await?),
        }
    }
}

impl GdriveBackend {
    async fn get_abs_root_id(&self) -> String {
        let mut req = Request::get("https://www.googleapis.com/drive/v3/files/root");
        let auth_header_content = format!("Bearer {}", self.access_token);
        req = req.header(header::AUTHORIZATION, auth_header_content);
        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)
            .unwrap();

        let resp = self.client.send(req).await.unwrap();

        let body_value: GdriveFile =
            serde_json::from_slice(&resp.into_body().bytes().await.unwrap()).unwrap();
        let root_id = String::from(body_value.id.as_str());
        root_id
    }

    async fn get_file_id_by_path(&self, file_path: &str) -> String {
        let path = build_rooted_abs_path(&self.root, file_path);
        let auth_header_content = format!("Bearer {}", self.access_token);

        let mut parent_id = self.get_abs_root_id().await;
        let file_path_items: Vec<&str> = path.split('/').filter(|&x| !x.is_empty()).collect();

        for (i, item) in file_path_items.iter().enumerate() {
            let mut query = format!(
                "name = '{}' and parents = '{}' and trashed = false",
                item, parent_id
            );
            if i != file_path_items.len() - 1 {
                query += "and mimeType = 'application/vnd.google-apps.folder'";
            }
            let query: String = query.chars().filter(|c| !c.is_whitespace()).collect();

            let mut req = Request::get(format!(
                "https://www.googleapis.com/drive/v3/files?q={}",
                query
            ));
            req = req.header(header::AUTHORIZATION, &auth_header_content);
            let req = req
                .body(AsyncBody::default())
                .map_err(new_request_build_error)
                .unwrap();

            let resp = self.client.send(req).await.unwrap();

            let body_value: GdriveFileList =
                serde_json::from_slice(&resp.into_body().bytes().await.unwrap()).unwrap();
            parent_id = String::from(body_value.files[0].id.as_str());
        }

        parent_id
    }

    async fn gdrive_get(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let url: String = format!(
            "https://www.googleapis.com/drive/v3/files/{}?alt=media",
            self.get_file_id_by_path(path).await
        );

        let auth_header_content = format!("Bearer {}", self.access_token);
        let mut req = Request::get(&url);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    pub async fn gdrive_update(
        &self,
        path: &str,
        size: Option<usize>,
        content_type: Option<&str>,
        body: AsyncBody,
    ) -> Result<Response<IncomingAsyncBody>> {
        let url = format!(
            "https://www.googleapis.com/upload/drive/v3/files/{}",
            self.get_file_id_by_path(path).await
        );

        let mut req = Request::patch(&url);

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

    async fn gdrive_delete(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let url = format!(
            "https://www.googleapis.com/drive/v3/files/{}",
            self.get_file_id_by_path(path).await
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

#[derive(Deserialize)]
struct GdriveFile {
    id: String,
}

#[derive(Deserialize)]
struct GdriveFileList {
    files: Vec<GdriveFile>,
}
