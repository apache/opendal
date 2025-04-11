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
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::Request;
use http::Response;
use http::StatusCode;
use serde::Deserialize;
use tokio::sync::OnceCell;

use super::error::parse_error;
use crate::raw::*;
use crate::*;

pub struct WebhdfsCore {
    pub info: Arc<AccessorInfo>,
    pub root: String,
    pub endpoint: String,
    pub user_name: Option<String>,
    pub auth: Option<String>,
    pub root_checker: OnceCell<()>,

    pub atomic_write_dir: Option<String>,
    pub disable_list_batch: bool,
}

impl Debug for WebhdfsCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebhdfsCore")
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

impl WebhdfsCore {
    pub fn webhdfs_create_dir_request(&self, path: &str) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let mut url = format!(
            "{}/webhdfs/v1/{}?op=MKDIRS&overwrite=true&noredirect=true",
            self.endpoint,
            percent_encode_path(&p),
        );
        if let Some(user) = &self.user_name {
            url += format!("&user.name={user}").as_str();
        }
        if let Some(auth) = &self.auth {
            url += format!("&{auth}").as_str();
        }

        let req = Request::put(&url);

        req.body(Buffer::new()).map_err(new_request_build_error)
    }

    /// create object
    pub async fn webhdfs_create_object_request(
        &self,
        path: &str,
        size: Option<u64>,
        args: &OpWrite,
        body: Buffer,
    ) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let mut url = format!(
            "{}/webhdfs/v1/{}?op=CREATE&overwrite=true&noredirect=true",
            self.endpoint,
            percent_encode_path(&p),
        );
        if let Some(user) = &self.user_name {
            url += format!("&user.name={user}").as_str();
        }
        if let Some(auth) = &self.auth {
            url += format!("&{auth}").as_str();
        }

        let req = Request::put(&url);

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        let resp = self.info.http_client().send(req).await?;

        let status = resp.status();

        if status != StatusCode::CREATED && status != StatusCode::OK {
            return Err(parse_error(resp));
        }

        let bs = resp.into_body();

        let resp: LocationResponse =
            serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

        let mut req = Request::put(&resp.location);

        if let Some(size) = size {
            req = req.header(CONTENT_LENGTH, size);
        };

        if let Some(content_type) = args.content_type() {
            req = req.header(CONTENT_TYPE, content_type);
        };
        let req = req.body(body).map_err(new_request_build_error)?;

        Ok(req)
    }

    pub async fn webhdfs_init_append_request(&self, path: &str) -> Result<String> {
        let p = build_abs_path(&self.root, path);
        let mut url = format!(
            "{}/webhdfs/v1/{}?op=APPEND&noredirect=true",
            self.endpoint,
            percent_encode_path(&p),
        );
        if let Some(user) = &self.user_name {
            url += format!("&user.name={user}").as_str();
        }
        if let Some(auth) = &self.auth {
            url += &format!("&{auth}");
        }

        let req = Request::post(url)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        let resp = self.info.http_client().send(req).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let bs = resp.into_body();
                let resp: LocationResponse =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

                Ok(resp.location)
            }
            _ => Err(parse_error(resp)),
        }
    }

    pub async fn webhdfs_rename_object(&self, from: &str, to: &str) -> Result<Response<Buffer>> {
        let from = build_abs_path(&self.root, from);
        let to = build_rooted_abs_path(&self.root, to);

        let mut url = format!(
            "{}/webhdfs/v1/{}?op=RENAME&destination={}",
            self.endpoint,
            percent_encode_path(&from),
            percent_encode_path(&to)
        );
        if let Some(user) = &self.user_name {
            url += format!("&user.name={user}").as_str();
        }
        if let Some(auth) = &self.auth {
            url += &format!("&{auth}");
        }

        let req = Request::put(&url)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.info.http_client().send(req).await
    }

    pub fn webhdfs_append_request(
        &self,
        location: &str,
        size: u64,
        body: Buffer,
    ) -> Result<Request<Buffer>> {
        let mut url = location.to_string();
        if let Some(user) = &self.user_name {
            url += format!("&user.name={user}").as_str();
        }
        if let Some(auth) = &self.auth {
            url += &format!("&{auth}");
        }

        let mut req = Request::post(&url);

        req = req.header(CONTENT_LENGTH, size.to_string());

        req.body(body).map_err(new_request_build_error)
    }

    /// CONCAT will concat sources to the path
    pub fn webhdfs_concat_request(
        &self,
        path: &str,
        sources: Vec<String>,
    ) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let sources = sources
            .iter()
            .map(|p| build_rooted_abs_path(&self.root, p))
            .collect::<Vec<String>>()
            .join(",");

        let mut url = format!(
            "{}/webhdfs/v1/{}?op=CONCAT&sources={}",
            self.endpoint,
            percent_encode_path(&p),
            percent_encode_path(&sources),
        );
        if let Some(user) = &self.user_name {
            url += format!("&user.name={user}").as_str();
        }
        if let Some(auth) = &self.auth {
            url += &format!("&{auth}");
        }

        let req = Request::post(url);

        req.body(Buffer::new()).map_err(new_request_build_error)
    }

    fn webhdfs_open_request(&self, path: &str, range: &BytesRange) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);
        let mut url = format!(
            "{}/webhdfs/v1/{}?op=OPEN",
            self.endpoint,
            percent_encode_path(&p),
        );
        if let Some(user) = &self.user_name {
            url += format!("&user.name={user}").as_str();
        }
        if let Some(auth) = &self.auth {
            url += &format!("&{auth}");
        }

        if !range.is_full() {
            url += &format!("&offset={}", range.offset());
            if let Some(size) = range.size() {
                url += &format!("&length={size}")
            }
        }

        let req = Request::get(&url)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        Ok(req)
    }

    pub async fn webhdfs_list_status_request(&self, path: &str) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);
        let mut url = format!(
            "{}/webhdfs/v1/{}?op=LISTSTATUS",
            self.endpoint,
            percent_encode_path(&p),
        );
        if let Some(user) = &self.user_name {
            url += format!("&user.name={user}").as_str();
        }
        if let Some(auth) = &self.auth {
            url += format!("&{auth}").as_str();
        }

        let req = Request::get(&url)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;
        self.info.http_client().send(req).await
    }

    pub async fn webhdfs_list_status_batch_request(
        &self,
        path: &str,
        start_after: &str,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let mut url = format!(
            "{}/webhdfs/v1/{}?op=LISTSTATUS_BATCH",
            self.endpoint,
            percent_encode_path(&p),
        );
        if !start_after.is_empty() {
            url += format!("&startAfter={}", start_after).as_str();
        }
        if let Some(user) = &self.user_name {
            url += format!("&user.name={user}").as_str();
        }
        if let Some(auth) = &self.auth {
            url += format!("&{auth}").as_str();
        }

        let req = Request::get(&url)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;
        self.info.http_client().send(req).await
    }

    pub async fn webhdfs_read_file(
        &self,
        path: &str,
        range: BytesRange,
    ) -> Result<Response<HttpBody>> {
        let req = self.webhdfs_open_request(path, &range)?;
        self.info.http_client().fetch(req).await
    }

    pub(super) async fn webhdfs_get_file_status(&self, path: &str) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);
        let mut url = format!(
            "{}/webhdfs/v1/{}?op=GETFILESTATUS",
            self.endpoint,
            percent_encode_path(&p),
        );
        if let Some(user) = &self.user_name {
            url += format!("&user.name={user}").as_str();
        }
        if let Some(auth) = &self.auth {
            url += format!("&{auth}").as_str();
        }

        let req = Request::get(&url)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.info.http_client().send(req).await
    }

    pub async fn webhdfs_delete(&self, path: &str) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);
        let mut url = format!(
            "{}/webhdfs/v1/{}?op=DELETE&recursive=false",
            self.endpoint,
            percent_encode_path(&p),
        );
        if let Some(user) = &self.user_name {
            url += format!("&user.name={user}").as_str();
        }
        if let Some(auth) = &self.auth {
            url += format!("&{auth}").as_str();
        }

        let req = Request::delete(&url)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.info.http_client().send(req).await
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(super) struct LocationResponse {
    pub location: String,
}
