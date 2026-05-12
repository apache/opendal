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

use http::Request;
use http::Response;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::header::HOST;
use reqsign_core::Signer;
use reqsign_volcengine_tos::Credential;
use reqsign_volcengine_tos::{percent_encode_path, percent_encode_query};
use serde::Deserialize;
use serde::Serialize;

use opendal_core::raw::*;
use opendal_core::*;

pub mod constants {
    pub const X_TOS_STORAGE_CLASS: &str = "x-tos-storage-class";

    pub const X_TOS_META_PREFIX: &str = "x-tos-meta-";

    pub const X_TOS_VERSION_ID: &str = "x-tos-version-id";
    pub const X_TOS_OBJECT_SIZE: &str = "x-tos-object-size";

    pub const X_TOS_DIRECTORY: &str = "x-tos-directory";

    pub const RESPONSE_CONTENT_DISPOSITION: &str = "response-content-disposition";
    pub const RESPONSE_CONTENT_TYPE: &str = "response-content-type";
    pub const RESPONSE_CACHE_CONTROL: &str = "response-cache-control";

    pub const TOS_QUERY_VERSION_ID: &str = "versionId";
}

pub struct TosCore {
    pub info: Arc<AccessorInfo>,

    pub bucket: String,
    pub endpoint: String, // full endpoint with scheme, e.g. https://tos-cn-beijing.volces.com
    pub endpoint_domain: String, // endpoint domain without scheme, e.g. tos-cn-beijing.volces.com
    pub root: String,
    pub default_storage_class: Option<String>,
    pub allow_anonymous: bool,

    pub signer: Signer<Credential>,
}

impl Debug for TosCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TosCore")
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl TosCore {
    pub async fn send(&self, req: Request<Buffer>) -> Result<Response<Buffer>> {
        if self.allow_anonymous {
            return self.info.http_client().send(req).await;
        }

        let (mut parts, body) = req.into_parts();
        self.signer
            .sign(&mut parts, None)
            .await
            .map_err(|e| new_request_sign_error(e.into()))?;

        let resp = self
            .info
            .http_client()
            .send(Request::from_parts(parts, body))
            .await?;

        Ok(resp)
    }

    pub async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
        if self.allow_anonymous {
            return self.info.http_client().fetch(req).await;
        }

        let (mut parts, body) = req.into_parts();

        self.signer
            .sign(&mut parts, None)
            .await
            .map_err(|e| new_request_sign_error(e.into()))?;

        parts.headers.remove(HOST);

        self.info
            .http_client()
            .fetch(Request::from_parts(parts, body))
            .await
    }

    pub fn insert_metadata_headers(
        &self,
        mut req: http::request::Builder,
        size: Option<u64>,
        args: &OpWrite,
    ) -> http::request::Builder {
        if let Some(size) = size {
            req = req.header(CONTENT_LENGTH, size.to_string());
        }

        if let Some(mime) = args.content_type() {
            req = req.header(CONTENT_TYPE, mime);
        }

        if let Some(cache_control) = args.cache_control() {
            req = req.header(http::header::CACHE_CONTROL, cache_control);
        }

        if let Some(content_encoding) = args.content_encoding() {
            req = req.header(http::header::CONTENT_ENCODING, content_encoding);
        }

        if let Some(content_disposition) = args.content_disposition() {
            req = req.header(http::header::CONTENT_DISPOSITION, content_disposition);
        }

        if let Some(if_match) = args.if_match() {
            req = req.header(http::header::IF_MATCH, if_match);
        }

        if args.if_not_exists() {
            req = req.header(http::header::IF_NONE_MATCH, "*");
        }

        if let Some(v) = &self.default_storage_class {
            req = req.header(
                http::HeaderName::from_static(constants::X_TOS_STORAGE_CLASS),
                v,
            );
        }

        if let Some(user_metadata) = args.user_metadata() {
            for (key, value) in user_metadata {
                req = req.header(format!("{}{}", constants::X_TOS_META_PREFIX, key), value);
            }
        }

        req
    }

    pub fn tos_get_object_request(
        &self,
        path: &str,
        range: BytesRange,
        args: &OpRead,
    ) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let mut url = format!(
            "https://{}.{}/{}",
            self.bucket,
            self.endpoint_domain,
            percent_encode_path(&p)
        );

        let mut query_args = Vec::new();
        if let Some(override_content_disposition) = args.override_content_disposition() {
            query_args.push(format!(
                "{}={}",
                constants::RESPONSE_CONTENT_DISPOSITION,
                percent_encode_query(override_content_disposition)
            ));
        }
        if let Some(override_content_type) = args.override_content_type() {
            query_args.push(format!(
                "{}={}",
                constants::RESPONSE_CONTENT_TYPE,
                percent_encode_query(override_content_type)
            ));
        }
        if let Some(override_cache_control) = args.override_cache_control() {
            query_args.push(format!(
                "{}={}",
                constants::RESPONSE_CACHE_CONTROL,
                percent_encode_query(override_cache_control)
            ));
        }
        if let Some(version) = args.version() {
            query_args.push(format!(
                "{}={}",
                constants::TOS_QUERY_VERSION_ID,
                percent_decode_path(version)
            ));
        }
        if !query_args.is_empty() {
            url.push_str(&format!("?{}", query_args.join("&")));
        }

        let mut req = Request::get(&url);

        if !range.is_full() {
            req = req.header(http::header::RANGE, range.to_header());
        }

        if let Some(if_none_match) = args.if_none_match() {
            req = req.header(http::header::IF_NONE_MATCH, if_none_match);
        }

        if let Some(if_match) = args.if_match() {
            req = req.header(http::header::IF_MATCH, if_match);
        }

        if let Some(if_modified_since) = args.if_modified_since() {
            req = req.header(
                http::header::IF_MODIFIED_SINCE,
                if_modified_since.format_http_date(),
            );
        }

        if let Some(if_unmodified_since) = args.if_unmodified_since() {
            req = req.header(
                http::header::IF_UNMODIFIED_SINCE,
                if_unmodified_since.format_http_date(),
            );
        }

        req = req.extension(Operation::Read);

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        Ok(req)
    }

    pub async fn tos_get_object(
        &self,
        path: &str,
        range: BytesRange,
        args: &OpRead,
    ) -> Result<Response<HttpBody>> {
        let req = self.tos_get_object_request(path, range, args)?;
        self.fetch(req).await
    }

    pub fn tos_put_object_request(
        &self,
        path: &str,
        size: Option<u64>,
        args: &OpWrite,
        body: Buffer,
    ) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "https://{}.{}/{}",
            self.bucket,
            self.endpoint_domain,
            percent_encode_path(&p)
        );

        let mut req = Request::put(&url);

        req = self.insert_metadata_headers(req, size, args);

        req = req.extension(Operation::Write);

        let req = req.body(body).map_err(new_request_build_error)?;

        Ok(req)
    }

    pub fn tos_head_object_request(&self, path: &str, args: OpStat) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let mut url = format!(
            "https://{}.{}/{}",
            self.bucket,
            self.endpoint_domain,
            percent_encode_path(&p)
        );

        let mut query_args = Vec::new();
        if let Some(override_content_disposition) = args.override_content_disposition() {
            query_args.push(format!(
                "{}={}",
                constants::RESPONSE_CONTENT_DISPOSITION,
                percent_encode_query(override_content_disposition)
            ));
        }
        if let Some(override_content_type) = args.override_content_type() {
            query_args.push(format!(
                "{}={}",
                constants::RESPONSE_CONTENT_TYPE,
                percent_encode_query(override_content_type)
            ));
        }
        if let Some(override_cache_control) = args.override_cache_control() {
            query_args.push(format!(
                "{}={}",
                constants::RESPONSE_CACHE_CONTROL,
                percent_encode_query(override_cache_control)
            ));
        }
        if let Some(version) = args.version() {
            query_args.push(format!(
                "{}={}",
                constants::TOS_QUERY_VERSION_ID,
                percent_decode_path(version)
            ));
        }
        if !query_args.is_empty() {
            url.push_str(&format!("?{}", query_args.join("&")));
        }

        let mut req = Request::head(&url);

        if let Some(if_none_match) = args.if_none_match() {
            req = req.header(http::header::IF_NONE_MATCH, if_none_match);
        }
        if let Some(if_match) = args.if_match() {
            req = req.header(http::header::IF_MATCH, if_match);
        }

        if let Some(if_modified_since) = args.if_modified_since() {
            req = req.header(
                http::header::IF_MODIFIED_SINCE,
                if_modified_since.format_http_date(),
            );
        }
        if let Some(if_unmodified_since) = args.if_unmodified_since() {
            req = req.header(
                http::header::IF_UNMODIFIED_SINCE,
                if_unmodified_since.format_http_date(),
            );
        }

        req = req.extension(Operation::Stat);

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        Ok(req)
    }

    pub async fn tos_head_object(&self, path: &str, args: OpStat) -> Result<Response<Buffer>> {
        let req = self.tos_head_object_request(path, args)?;
        self.send(req).await
    }

    pub async fn tos_delete_object(&self, path: &str, args: &OpDelete) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let mut url = format!(
            "https://{}.{}/{}",
            self.bucket,
            self.endpoint_domain,
            percent_encode_path(&p)
        );

        let mut query_args = Vec::new();

        if let Some(version) = args.version() {
            query_args.push(format!(
                "{}={}",
                constants::TOS_QUERY_VERSION_ID,
                percent_encode_query(version)
            ));
        }

        if !query_args.is_empty() {
            url.push_str(&format!("?{}", query_args.join("&")));
        }

        let req = Request::delete(&url);

        let req = req
            .extension(Operation::Delete)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn tos_delete_objects(
        &self,
        paths: Vec<(String, OpDelete)>,
    ) -> Result<Response<Buffer>> {
        let url = format!("https://{}.{}?delete", self.bucket, self.endpoint_domain);

        let mut req = Request::post(&url);

        let content = serde_json::to_string(&DeleteObjectsRequest {
            objects: paths
                .into_iter()
                .map(|(path, op)| DeleteObjectsRequestObject {
                    key: build_abs_path(&self.root, &path),
                    version_id: op.version().map(|v| v.to_owned()),
                })
                .collect(),
        })
        .map_err(new_json_serialize_error)?;

        req = req.header(CONTENT_LENGTH, content.len());
        req = req.header(CONTENT_TYPE, "application/json");
        req = req.header("CONTENT-MD5", format_content_md5(content.as_bytes()));

        req = req.extension(Operation::Delete);

        let req = req
            .body(Buffer::from(content))
            .map_err(new_request_build_error)?;

        self.send(req).await
    }
}

#[derive(Default, Debug, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct DeleteObjectsRequest {
    pub objects: Vec<DeleteObjectsRequestObject>,
}

#[derive(Default, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteObjectsRequestObject {
    pub key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct DeleteObjectsResult {
    pub deleted: Vec<DeleteObjectsResultDeleted>,
    pub errors: Vec<DeleteObjectsResultError>,
}

#[derive(Default, Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteObjectsResultDeleted {
    pub key: String,
    pub version_id: Option<String>,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct DeleteObjectsResultError {
    pub code: String,
    pub key: String,
    pub message: String,
    pub version_id: Option<String>,
}
