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

use base64::Engine;
use hmac::Hmac;
use hmac::Mac;
use http::header;
use http::HeaderMap;
use http::Request;
use http::Response;
use md5::Digest;
use serde::Deserialize;
use sha1::Sha1;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use self::constants::*;
use crate::raw::*;
use crate::*;

pub(super) mod constants {
    pub const X_UPYUN_FILE_TYPE: &str = "x-upyun-file-type";
    pub const X_UPYUN_FILE_SIZE: &str = "x-upyun-file-size";
    pub const X_UPYUN_CACHE_CONTROL: &str = "x-upyun-meta-cache-control";
    pub const X_UPYUN_CONTENT_DISPOSITION: &str = "x-upyun-meta-content-disposition";
    pub const X_UPYUN_MULTI_STAGE: &str = "X-Upyun-Multi-Stage";
    pub const X_UPYUN_MULTI_TYPE: &str = "X-Upyun-Multi-Type";
    pub const X_UPYUN_MULTI_DISORDER: &str = "X-Upyun-Multi-Disorder";
    pub const X_UPYUN_MULTI_UUID: &str = "X-Upyun-Multi-Uuid";
    pub const X_UPYUN_PART_ID: &str = "X-Upyun-Part-Id";
    pub const X_UPYUN_FOLDER: &str = "x-upyun-folder";
    pub const X_UPYUN_MOVE_SOURCE: &str = "X-Upyun-Move-Source";
    pub const X_UPYUN_COPY_SOURCE: &str = "X-Upyun-Copy-Source";
    pub const X_UPYUN_METADATA_DIRECTIVE: &str = "X-Upyun-Metadata-Directive";
    pub const X_UPYUN_LIST_ITER: &str = "x-list-iter";
    pub const X_UPYUN_LIST_LIMIT: &str = "X-List-Limit";
    pub const X_UPYUN_LIST_MAX_LIMIT: usize = 4096;
    pub const X_UPYUN_LIST_DEFAULT_LIMIT: usize = 256;
}

#[derive(Clone)]
pub struct UpyunCore {
    pub info: Arc<AccessorInfo>,
    /// The root of this core.
    pub root: String,
    /// The endpoint of this backend.
    pub operator: String,
    /// The bucket of this backend.
    pub bucket: String,

    /// signer of this backend.
    pub signer: UpyunSigner,
}

impl Debug for UpyunCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend")
            .field("root", &self.root)
            .field("bucket", &self.bucket)
            .field("operator", &self.operator)
            .finish_non_exhaustive()
    }
}

impl UpyunCore {
    #[inline]
    pub async fn send(&self, req: Request<Buffer>) -> Result<Response<Buffer>> {
        self.info.http_client().send(req).await
    }

    pub fn sign(&self, req: &mut Request<Buffer>) -> Result<()> {
        // get rfc1123 date
        let date = chrono::Utc::now()
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string();
        let authorization =
            self.signer
                .authorization(&date, req.method().as_str(), req.uri().path());

        req.headers_mut()
            .insert("Authorization", authorization.parse().unwrap());
        req.headers_mut().insert("Date", date.parse().unwrap());

        Ok(())
    }
}

impl UpyunCore {
    pub async fn download_file(&self, path: &str, range: BytesRange) -> Result<Response<HttpBody>> {
        let path = build_abs_path(&self.root, path);

        let url = format!(
            "https://v0.api.upyun.com/{}/{}",
            self.bucket,
            percent_encode_path(&path)
        );

        let req = Request::get(url);

        let mut req = req
            .header(header::RANGE, range.to_header())
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(&mut req)?;

        self.info.http_client().fetch(req).await
    }

    pub async fn info(&self, path: &str) -> Result<Response<Buffer>> {
        let path = build_abs_path(&self.root, path);

        let url = format!(
            "https://v0.api.upyun.com/{}/{}",
            self.bucket,
            percent_encode_path(&path)
        );

        let req = Request::head(url);

        let mut req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.sign(&mut req)?;

        self.send(req).await
    }

    pub fn upload(
        &self,
        path: &str,
        size: Option<u64>,
        args: &OpWrite,
        body: Buffer,
    ) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "https://v0.api.upyun.com/{}/{}",
            self.bucket,
            percent_encode_path(&p)
        );

        let mut req = Request::put(&url);

        if let Some(size) = size {
            req = req.header(header::CONTENT_LENGTH, size.to_string())
        }

        if let Some(mime) = args.content_type() {
            req = req.header(header::CONTENT_TYPE, mime)
        }

        if let Some(pos) = args.content_disposition() {
            req = req.header(X_UPYUN_CONTENT_DISPOSITION, pos)
        }

        if let Some(cache_control) = args.cache_control() {
            req = req.header(X_UPYUN_CACHE_CONTROL, cache_control)
        }

        // Set body
        let mut req = req.body(body).map_err(new_request_build_error)?;

        self.sign(&mut req)?;

        Ok(req)
    }

    pub async fn delete(&self, path: &str) -> Result<Response<Buffer>> {
        let path = build_abs_path(&self.root, path);

        let url = format!(
            "https://v0.api.upyun.com/{}/{}",
            self.bucket,
            percent_encode_path(&path)
        );

        let req = Request::delete(url);

        let mut req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.sign(&mut req)?;

        self.send(req).await
    }

    pub async fn copy(&self, from: &str, to: &str) -> Result<Response<Buffer>> {
        let from = format!("/{}/{}", self.bucket, build_abs_path(&self.root, from));
        let to = build_abs_path(&self.root, to);

        let url = format!(
            "https://v0.api.upyun.com/{}/{}",
            self.bucket,
            percent_encode_path(&to)
        );

        let mut req = Request::put(url);

        req = req.header(header::CONTENT_LENGTH, "0");

        req = req.header(X_UPYUN_COPY_SOURCE, from);

        req = req.header(X_UPYUN_METADATA_DIRECTIVE, "copy");

        // Set body
        let mut req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.sign(&mut req)?;

        self.send(req).await
    }

    pub async fn move_object(&self, from: &str, to: &str) -> Result<Response<Buffer>> {
        let from = format!("/{}/{}", self.bucket, build_abs_path(&self.root, from));
        let to = build_abs_path(&self.root, to);

        let url = format!(
            "https://v0.api.upyun.com/{}/{}",
            self.bucket,
            percent_encode_path(&to)
        );

        let mut req = Request::put(url);

        req = req.header(header::CONTENT_LENGTH, "0");

        req = req.header(X_UPYUN_MOVE_SOURCE, from);

        req = req.header(X_UPYUN_METADATA_DIRECTIVE, "copy");

        // Set body
        let mut req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.sign(&mut req)?;

        self.send(req).await
    }

    pub async fn create_dir(&self, path: &str) -> Result<Response<Buffer>> {
        let path = build_abs_path(&self.root, path);
        let path = path[..path.len() - 1].to_string();

        let url = format!(
            "https://v0.api.upyun.com/{}/{}",
            self.bucket,
            percent_encode_path(&path)
        );

        let mut req = Request::post(url);

        req = req.header("folder", "true");

        req = req.header(X_UPYUN_FOLDER, "true");

        let mut req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.sign(&mut req)?;

        self.send(req).await
    }

    pub async fn initiate_multipart_upload(
        &self,
        path: &str,
        args: &OpWrite,
    ) -> Result<Response<Buffer>> {
        let path = build_abs_path(&self.root, path);

        let url = format!(
            "https://v0.api.upyun.com/{}/{}",
            self.bucket,
            percent_encode_path(&path)
        );

        let mut req = Request::put(url);

        req = req.header(X_UPYUN_MULTI_STAGE, "initiate");

        req = req.header(X_UPYUN_MULTI_DISORDER, "true");

        if let Some(content_type) = args.content_type() {
            req = req.header(X_UPYUN_MULTI_TYPE, content_type);
        }

        if let Some(content_disposition) = args.content_disposition() {
            req = req.header(X_UPYUN_CONTENT_DISPOSITION, content_disposition)
        }

        if let Some(cache_control) = args.cache_control() {
            req = req.header(X_UPYUN_CACHE_CONTROL, cache_control)
        }

        let mut req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.sign(&mut req)?;

        self.send(req).await
    }

    pub fn upload_part(
        &self,
        path: &str,
        upload_id: &str,
        part_number: usize,
        size: u64,
        body: Buffer,
    ) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "https://v0.api.upyun.com/{}/{}",
            self.bucket,
            percent_encode_path(&p),
        );

        let mut req = Request::put(&url);

        req = req.header(header::CONTENT_LENGTH, size);

        req = req.header(X_UPYUN_MULTI_STAGE, "upload");

        req = req.header(X_UPYUN_MULTI_UUID, upload_id);

        req = req.header(X_UPYUN_PART_ID, part_number);

        // Set body
        let mut req = req.body(body).map_err(new_request_build_error)?;

        self.sign(&mut req)?;

        Ok(req)
    }

    pub async fn complete_multipart_upload(
        &self,
        path: &str,
        upload_id: &str,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "https://v0.api.upyun.com/{}/{}",
            self.bucket,
            percent_encode_path(&p),
        );

        let mut req = Request::put(url);

        req = req.header(X_UPYUN_MULTI_STAGE, "complete");

        req = req.header(X_UPYUN_MULTI_UUID, upload_id);

        let mut req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.sign(&mut req)?;

        self.send(req).await
    }

    pub async fn list_objects(
        &self,
        path: &str,
        iter: &str,
        limit: Option<usize>,
    ) -> Result<Response<Buffer>> {
        let path = build_abs_path(&self.root, path);

        let url = format!(
            "https://v0.api.upyun.com/{}/{}",
            self.bucket,
            percent_encode_path(&path),
        );

        let mut req = Request::get(url.clone());

        req = req.header(header::ACCEPT, "application/json");

        if !iter.is_empty() {
            req = req.header(X_UPYUN_LIST_ITER, iter);
        }

        if let Some(mut limit) = limit {
            if limit > X_UPYUN_LIST_MAX_LIMIT {
                limit = X_UPYUN_LIST_DEFAULT_LIMIT;
            }
            req = req.header(X_UPYUN_LIST_LIMIT, limit);
        }

        // Set body
        let mut req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.sign(&mut req)?;

        self.send(req).await
    }
}

#[derive(Clone, Default)]
pub struct UpyunSigner {
    pub operator: String,
    pub password: String,
}

type HmacSha1 = Hmac<Sha1>;

impl UpyunSigner {
    pub fn authorization(&self, date: &str, method: &str, uri: &str) -> String {
        let sign = vec![method, uri, date];

        let sign = sign
            .into_iter()
            .filter(|s| !s.is_empty())
            .collect::<Vec<&str>>()
            .join("&");

        let mut mac = HmacSha1::new_from_slice(format_md5(self.password.as_bytes()).as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(sign.as_bytes());
        let sign_str = mac.finalize().into_bytes();

        let sign = base64::engine::general_purpose::STANDARD.encode(sign_str.as_slice());
        format!("UPYUN {}:{}", self.operator, sign)
    }
}

pub(super) fn parse_info(headers: &HeaderMap) -> Result<Metadata> {
    let mode = if parse_header_to_str(headers, X_UPYUN_FILE_TYPE)? == Some("file") {
        EntryMode::FILE
    } else {
        EntryMode::DIR
    };

    let mut m = Metadata::new(mode);

    if let Some(v) = parse_header_to_str(headers, X_UPYUN_FILE_SIZE)? {
        let size = v.parse::<u64>().map_err(|e| {
            Error::new(ErrorKind::Unexpected, "header value is not valid integer")
                .with_operation("parse_info")
                .set_source(e)
        })?;
        m.set_content_length(size);
    }

    if let Some(v) = parse_content_type(headers)? {
        m.set_content_type(v);
    }

    if let Some(v) = parse_content_md5(headers)? {
        m.set_content_md5(v);
    }

    if let Some(v) = parse_header_to_str(headers, X_UPYUN_CACHE_CONTROL)? {
        m.set_cache_control(v);
    }

    if let Some(v) = parse_header_to_str(headers, X_UPYUN_CONTENT_DISPOSITION)? {
        m.set_content_disposition(v);
    }

    Ok(m)
}

pub fn format_md5(bs: &[u8]) -> String {
    let mut hasher = md5::Md5::new();
    hasher.update(bs);

    format!("{:x}", hasher.finalize())
}

#[derive(Debug, Deserialize)]
pub(super) struct File {
    #[serde(rename = "type")]
    pub type_field: String,
    pub name: String,
    pub length: u64,
    pub last_modified: i64,
}

#[derive(Debug, Deserialize)]
pub(super) struct ListObjectsResponse {
    pub iter: String,
    pub files: Vec<File>,
}
