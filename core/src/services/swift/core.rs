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

use http::header;
use http::Request;
use http::Response;
use serde::Deserialize;
use std::fmt::Debug;
use std::sync::Arc;

use crate::raw::*;
use crate::*;

pub struct SwiftCore {
    pub info: Arc<AccessorInfo>,
    pub root: String,
    pub endpoint: String,
    pub container: String,
    pub token: String,
}

impl Debug for SwiftCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SwiftCore")
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .field("container", &self.container)
            .finish_non_exhaustive()
    }
}

impl SwiftCore {
    pub async fn swift_delete(&self, path: &str) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}",
            &self.endpoint,
            &self.container,
            percent_encode_path(&p)
        );

        let mut req = Request::delete(&url);

        req = req.header("X-Auth-Token", &self.token);

        let body = Buffer::new();

        let req = req.body(body).map_err(new_request_build_error)?;

        self.info.http_client().send(req).await
    }

    pub async fn swift_list(
        &self,
        path: &str,
        delimiter: &str,
        limit: Option<usize>,
        marker: &str,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        // The delimiter is used to disable recursive listing.
        // Swift returns a 200 status code when there is no such pseudo directory in prefix.
        let mut url = format!(
            "{}/{}/?prefix={}&delimiter={}&format=json",
            &self.endpoint,
            &self.container,
            percent_encode_path(&p),
            delimiter
        );

        if let Some(limit) = limit {
            url += &format!("&limit={}", limit);
        }
        if !marker.is_empty() {
            url += &format!("&marker={}", marker);
        }

        let mut req = Request::get(&url);

        req = req.header("X-Auth-Token", &self.token);

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.info.http_client().send(req).await
    }

    pub async fn swift_create_object(
        &self,
        path: &str,
        length: u64,
        args: &OpWrite,
        body: Buffer,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);
        let url = format!(
            "{}/{}/{}",
            &self.endpoint,
            &self.container,
            percent_encode_path(&p)
        );

        let mut req = Request::put(&url);

        // Set user metadata headers.
        if let Some(user_metadata) = args.user_metadata() {
            for (k, v) in user_metadata {
                req = req.header(format!("X-Object-Meta-{}", k), v);
            }
        }

        req = req.header("X-Auth-Token", &self.token);
        req = req.header(header::CONTENT_LENGTH, length);

        let req = req.body(body).map_err(new_request_build_error)?;

        self.info.http_client().send(req).await
    }

    pub async fn swift_read(
        &self,
        path: &str,
        range: BytesRange,
        _arg: &OpRead,
    ) -> Result<Response<HttpBody>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let url = format!(
            "{}/{}/{}",
            &self.endpoint,
            &self.container,
            percent_encode_path(&p)
        );

        let mut req = Request::get(&url);

        req = req.header("X-Auth-Token", &self.token);

        if !range.is_full() {
            req = req.header(header::RANGE, range.to_header());
        }

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.info.http_client().fetch(req).await
    }

    pub async fn swift_copy(&self, src_p: &str, dst_p: &str) -> Result<Response<Buffer>> {
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
            "{}/{}/{}",
            &self.endpoint,
            &self.container,
            percent_encode_path(&dst_p)
        );

        // Request method doesn't support for COPY, we use PUT instead.
        // Reference: https://docs.openstack.org/api-ref/object-store/#copy-object
        let mut req = Request::put(&url);

        req = req.header("X-Auth-Token", &self.token);
        req = req.header("X-Copy-From", percent_encode_path(&src_p));

        // if use PUT method, we need to set the content-length to 0.
        req = req.header("Content-Length", "0");

        let body = Buffer::new();

        let req = req.body(body).map_err(new_request_build_error)?;

        self.info.http_client().send(req).await
    }

    pub async fn swift_get_metadata(&self, path: &str) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}",
            &self.endpoint,
            &self.container,
            percent_encode_path(&p)
        );

        let mut req = Request::head(&url);

        req = req.header("X-Auth-Token", &self.token);

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.info.http_client().send(req).await
    }
}

#[derive(Debug, Eq, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum ListOpResponse {
    Subdir {
        subdir: String,
    },
    FileInfo {
        bytes: u64,
        hash: String,
        name: String,
        last_modified: String,
        content_type: Option<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_list_response_test() -> Result<()> {
        let resp = bytes::Bytes::from(
            r#"
            [
                {
                    "subdir": "animals/"
                },
                {
                    "subdir": "fruit/"
                },
                {
                    "bytes": 147,
                    "hash": "5e6b5b70b0426b1cc1968003e1afa5ad",
                    "name": "test.txt",
                    "content_type": "text/plain",
                    "last_modified": "2023-11-01T03:00:23.147480"
                }
            ]
            "#,
        );

        let mut out = serde_json::from_slice::<Vec<ListOpResponse>>(&resp)
            .map_err(new_json_deserialize_error)?;

        assert_eq!(out.len(), 3);
        assert_eq!(
            out.pop().unwrap(),
            ListOpResponse::FileInfo {
                bytes: 147,
                hash: "5e6b5b70b0426b1cc1968003e1afa5ad".to_string(),
                name: "test.txt".to_string(),
                last_modified: "2023-11-01T03:00:23.147480".to_string(),
                content_type: Some("text/plain".to_string()),
            }
        );

        assert_eq!(
            out.pop().unwrap(),
            ListOpResponse::Subdir {
                subdir: "fruit/".to_string()
            }
        );

        assert_eq!(
            out.pop().unwrap(),
            ListOpResponse::Subdir {
                subdir: "animals/".to_string()
            }
        );

        Ok(())
    }
}
