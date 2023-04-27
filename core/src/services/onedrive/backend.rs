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
use std::fmt::Debug;

use crate::{
    ops::{OpRead, OpWrite},
    raw::{
        build_rooted_abs_path, new_request_build_error, parse_into_metadata, parse_location,
        percent_encode_path, Accessor, AccessorInfo, AsyncBody, HttpClient, IncomingAsyncBody,
        RpRead, RpWrite,
    },
    types::Result,
    Capability, Error, ErrorKind,
};

use super::{error::parse_error, writer::OneDriveWriter};

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
    type Pager = ();
    type BlockingPager = ();

    fn info(&self) -> AccessorInfo {
        let mut ma = AccessorInfo::default();
        ma.set_scheme(crate::Scheme::Onedrive)
            .set_root(&self.root)
            .set_capability(Capability {
                read: true,
                read_can_next: true,
                write: true,
                list: true,
                copy: true,
                rename: true,
                ..Default::default()
            });

        ma
    }

    async fn read(&self, path: &str, _args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.onedrive_get(path).await?;

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
}

impl OnedriveBackend {
    const ONEDRIVE_ENDPOINT_PREFIX: &'static str =
        "https://graph.microsoft.com/v1.0/me/drive/root:";
    const ONEDRIVE_ENDPOINT_SUFFIX: &'static str = ":/content";

    async fn onedrive_get(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let path = build_rooted_abs_path(&self.root, path);

        let url: String = format!(
            "{}{}{}",
            OnedriveBackend::ONEDRIVE_ENDPOINT_PREFIX,
            percent_encode_path(&path),
            OnedriveBackend::ONEDRIVE_ENDPOINT_SUFFIX
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
            "{}{}{}",
            OnedriveBackend::ONEDRIVE_ENDPOINT_PREFIX,
            percent_encode_path(path),
            OnedriveBackend::ONEDRIVE_ENDPOINT_SUFFIX
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
}
