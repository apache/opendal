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

use bytes::Bytes;
use http::header::CONTENT_LENGTH;
use http::Request;
use http::Response;
use serde_json::json;

use crate::raw::*;
use crate::*;

pub struct SwiftCore {
    pub root: String,
    pub endpoint: String,
    pub account: String,
    pub container: String,
    pub token: String,
    pub client: HttpClient,
}

impl Debug for SwiftCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SwiftCore")
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .field("account", &self.account)
            .field("container", &self.container)
            .field("token", &self.token)
            .finish_non_exhaustive()
    }
}

impl SwiftCore {
    pub async fn swift_create_dir(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        // Swift doesn't support create directory directly.

        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/v1/{}/{}/{}",
            self.endpoint,
            self.account,
            self.container,
            percent_encode_path(&p)
        );

        let mut req = Request::put(&url);

        req = req.header("X-Auth-Token", &self.token);
        req = req.header("Content-Length", "0");

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send(req).await

        // Ok(Response::new(IncomingAsyncBody::empty()))
    }

    pub async fn swift_delete(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/v1/{}/{}/{}",
            self.endpoint,
            self.account,
            self.container,
            percent_encode_path(&p)
        );

        let mut req = Request::delete(&url);

        req = req.header("X-Auth-Token", &self.token);

        let request_body = &json!({
            "path": percent_encode_path(&p),
            "recursive": true,
        });

        let body = AsyncBody::Bytes(Bytes::from(request_body.to_string()));

        let req = req.body(body).map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    pub async fn swift_list(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        // The delimiter is used to disable recursive listing.
        // Swift returns a 200 status code when there is no such pseudo directory in prefix.
        let url = format!(
            "{}/v1/{}/{}/?prefix={}&delimiter=/&format=json",
            self.endpoint,
            self.account,
            self.container,
            percent_encode_path(&p)
        );

        let mut req = Request::get(&url);

        req = req.header("X-Auth-Token", &self.token);

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    pub async fn swift_create_file(
        &self,
        path: &str,
        append: bool,
        offset: u64,
        body: AsyncBody,
    ) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let mut url = format!(
            "{}/v1/{}/{}/{}",
            self.endpoint,
            self.account,
            self.container,
            percent_encode_path(&p)
        );

        // append data to an existing DLO, where the offset is the final size after appending.
        if append && offset > 0 {
            url.push_str(format!("/{}", offset).as_str());
        }

        let mut req = Request::put(&url);

        req = req.header("X-Auth-Token", &self.token);

        // create the manifest file for DLO if not exist.
        if append && offset == 0 {
            req = req.header("X-Object-Manifest", format!("{}/{}", self.container, &p));
            req = req.header(CONTENT_LENGTH, "0");
        }

        let req = req.body(body).map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    pub async fn swift_read(
        &self,
        path: &str,
        range: BytesRange,
    ) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let url = format!(
            "{}/v1/{}/{}/{}",
            self.endpoint,
            self.account,
            self.container,
            percent_encode_path(&p)
        );

        let mut req = Request::get(&url);

        req = req.header("X-Auth-Token", &self.token);

        let range = match (range.offset(), range.size()) {
            (Some(offset), Some(size)) => {
                format!("bytes={}-{}", offset, offset + size - 1)
            }
            (Some(offset), None) => {
                format!("bytes={}-", offset)
            }
            (None, Some(size)) => {
                format!("bytes=-{}", size)
            }
            _ => "".to_string(),
        };

        if !range.is_empty() {
            req = req.header("Range", &range);
        }

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    pub async fn swift_copy(
        &self,
        src_p: &str,
        dst_p: &str,
    ) -> Result<Response<IncomingAsyncBody>> {
        // NOTE: current implementation is limited to same container and root

        let src_p = format!(
            "/{}/{}",
            self.container,
            build_abs_path(&self.root, src_p).trim_end_matches('/')
        );

        let dst_p = build_abs_path(&self.root, dst_p)
            .trim_end_matches('/')
            .to_string();

        let url = format!(
            "{}/v1/{}/{}/{}",
            self.endpoint,
            self.account,
            self.container,
            percent_encode_path(&dst_p)
        );

        // Request method doesn't support for COPY, we use PUT instead.
        // Reference: https://docs.openstack.org/api-ref/object-store/#copy-object
        let mut req = Request::put(&url);

        req = req.header("X-Auth-Token", &self.token);
        req = req.header("X-Copy-From", percent_encode_path(&src_p));

        // if use PUT method, we need to set the content-length to 0.
        req = req.header("Content-Length", "0");

        let body = AsyncBody::Empty;

        let req = req.body(body).map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    pub async fn swift_get_metadata(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/v1/{}/{}/{}",
            &self.endpoint,
            &self.account,
            &self.container,
            percent_encode_path(&p)
        );

        let mut req = Request::head(&url);

        req = req.header("X-Auth-Token", &self.token);

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }
}
