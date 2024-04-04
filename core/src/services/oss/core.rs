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
use std::fmt::Write;
use std::time::Duration;

use bytes::Bytes;
use http::header::CACHE_CONTROL;
use http::header::CONTENT_DISPOSITION;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::header::IF_MATCH;
use http::header::IF_NONE_MATCH;
use http::header::RANGE;
use http::HeaderValue;
use http::Request;
use http::Response;
use http::{HeaderName, StatusCode};
use reqsign::AliyunCredential;
use reqsign::AliyunLoader;
use reqsign::AliyunOssSigner;
use serde::Deserialize;
use serde::Serialize;

use crate::raw::*;
use crate::services::oss::error::parse_error;
use crate::*;

mod constants {
    pub const X_OSS_SERVER_SIDE_ENCRYPTION: &str = "x-oss-server-side-encryption";

    pub const X_OSS_SERVER_SIDE_ENCRYPTION_KEY_ID: &str = "x-oss-server-side-encryption-key-id";

    pub const RESPONSE_CONTENT_DISPOSITION: &str = "response-content-disposition";
}

pub struct OssCore {
    pub root: String,
    pub bucket: String,
    /// buffered host string
    ///
    /// format: <bucket-name>.<endpoint-domain-name>
    pub host: String,
    pub endpoint: String,
    pub presign_endpoint: String,
    pub allow_anonymous: bool,

    pub server_side_encryption: Option<HeaderValue>,
    pub server_side_encryption_key_id: Option<HeaderValue>,

    pub client: HttpClient,
    pub loader: AliyunLoader,
    pub signer: AliyunOssSigner,
    pub batch_max_operations: usize,
}

impl Debug for OssCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend")
            .field("root", &self.root)
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("host", &self.host)
            .finish_non_exhaustive()
    }
}

impl OssCore {
    async fn load_credential(&self) -> Result<Option<AliyunCredential>> {
        let cred = self
            .loader
            .load()
            .await
            .map_err(new_request_credential_error)?;

        if let Some(cred) = cred {
            Ok(Some(cred))
        } else if self.allow_anonymous {
            // If allow_anonymous has been set, we will not sign the request.
            Ok(None)
        } else {
            // Mark this error as temporary since it could be caused by Aliyun STS.
            Err(Error::new(
                ErrorKind::PermissionDenied,
                "no valid credential found, please check configuration or try again",
            )
            .set_temporary())
        }
    }

    pub async fn sign<T>(&self, req: &mut Request<T>) -> Result<()> {
        let cred = if let Some(cred) = self.load_credential().await? {
            cred
        } else {
            return Ok(());
        };

        self.signer.sign(req, &cred).map_err(new_request_sign_error)
    }

    pub async fn sign_query<T>(&self, req: &mut Request<T>, duration: Duration) -> Result<()> {
        let cred = if let Some(cred) = self.load_credential().await? {
            cred
        } else {
            return Ok(());
        };

        self.signer
            .sign_query(req, duration, &cred)
            .map_err(new_request_sign_error)
    }

    /// Set sse headers
    /// # Note
    /// According to the OSS documentation, only PutObject, CopyObject, and InitiateMultipartUpload may require to be set.
    pub fn insert_sse_headers(&self, mut req: http::request::Builder) -> http::request::Builder {
        if let Some(v) = &self.server_side_encryption {
            let mut v = v.clone();
            v.set_sensitive(true);

            req = req.header(
                HeaderName::from_static(constants::X_OSS_SERVER_SIDE_ENCRYPTION),
                v,
            )
        }
        if let Some(v) = &self.server_side_encryption_key_id {
            let mut v = v.clone();
            v.set_sensitive(true);

            req = req.header(
                HeaderName::from_static(constants::X_OSS_SERVER_SIDE_ENCRYPTION_KEY_ID),
                v,
            )
        }
        req
    }
}

impl OssCore {
    #[allow(clippy::too_many_arguments)]
    pub fn oss_put_object_request(
        &self,
        path: &str,
        size: Option<u64>,
        args: &OpWrite,
        body: RequestBody,
        is_presign: bool,
    ) -> Result<Request<RequestBody>> {
        let p = build_abs_path(&self.root, path);
        let endpoint = self.get_endpoint(is_presign);
        let url = format!("{}/{}", endpoint, percent_encode_path(&p));

        let mut req = Request::put(&url);

        req = req.header(CONTENT_LENGTH, size.unwrap_or_default());

        if let Some(mime) = args.content_type() {
            req = req.header(CONTENT_TYPE, mime);
        }

        if let Some(pos) = args.content_disposition() {
            req = req.header(CONTENT_DISPOSITION, pos);
        }

        if let Some(cache_control) = args.cache_control() {
            req = req.header(CACHE_CONTROL, cache_control)
        }

        // set sse headers
        req = self.insert_sse_headers(req);

        let req = req.body(body).map_err(new_request_build_error)?;
        Ok(req)
    }

    pub fn oss_append_object_request(
        &self,
        path: &str,
        position: u64,
        size: u64,
        args: &OpWrite,
        body: RequestBody,
    ) -> Result<Request<RequestBody>> {
        let p = build_abs_path(&self.root, path);
        let endpoint = self.get_endpoint(false);
        let url = format!(
            "{}/{}?append&position={}",
            endpoint,
            percent_encode_path(&p),
            position
        );

        let mut req = Request::post(&url);

        req = req.header(CONTENT_LENGTH, size);

        if let Some(mime) = args.content_type() {
            req = req.header(CONTENT_TYPE, mime);
        }

        if let Some(pos) = args.content_disposition() {
            req = req.header(CONTENT_DISPOSITION, pos);
        }

        if let Some(cache_control) = args.cache_control() {
            req = req.header(CACHE_CONTROL, cache_control)
        }

        // set sse headers
        req = self.insert_sse_headers(req);

        let req = req.body(body).map_err(new_request_build_error)?;
        Ok(req)
    }

    pub fn oss_get_object_request(
        &self,
        path: &str,
        range: BytesRange,
        is_presign: bool,
        args: &OpRead,
    ) -> Result<Request<RequestBody>> {
        let p = build_abs_path(&self.root, path);
        let endpoint = self.get_endpoint(is_presign);
        let mut url = format!("{}/{}", endpoint, percent_encode_path(&p));

        // Add query arguments to the URL based on response overrides
        let mut query_args = Vec::new();
        if let Some(override_content_disposition) = args.override_content_disposition() {
            query_args.push(format!(
                "{}={}",
                constants::RESPONSE_CONTENT_DISPOSITION,
                percent_encode_path(override_content_disposition)
            ))
        }

        if !query_args.is_empty() {
            url.push_str(&format!("?{}", query_args.join("&")));
        }

        let mut req = Request::get(&url);
        req = req.header(CONTENT_TYPE, "application/octet-stream");

        if !range.is_full() {
            req = req.header(RANGE, range.to_header());
            // Adding `x-oss-range-behavior` header to use standard behavior.
            // ref: https://help.aliyun.com/document_detail/39571.html
            req = req.header("x-oss-range-behavior", "standard");
        }

        if let Some(if_match) = args.if_match() {
            req = req.header(IF_MATCH, if_match)
        }
        if let Some(if_none_match) = args.if_none_match() {
            req = req.header(IF_NONE_MATCH, if_none_match);
        }

        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        Ok(req)
    }

    fn oss_delete_object_request(&self, path: &str) -> Result<Request<RequestBody>> {
        let p = build_abs_path(&self.root, path);
        let endpoint = self.get_endpoint(false);
        let url = format!("{}/{}", endpoint, percent_encode_path(&p));
        let req = Request::delete(&url);

        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        Ok(req)
    }

    pub fn oss_head_object_request(
        &self,
        path: &str,
        is_presign: bool,
        if_match: Option<&str>,
        if_none_match: Option<&str>,
    ) -> Result<Request<RequestBody>> {
        let p = build_abs_path(&self.root, path);
        let endpoint = self.get_endpoint(is_presign);
        let url = format!("{}/{}", endpoint, percent_encode_path(&p));

        let mut req = Request::head(&url);
        if let Some(if_match) = if_match {
            req = req.header(IF_MATCH, if_match)
        }
        if let Some(if_none_match) = if_none_match {
            req = req.header(IF_NONE_MATCH, if_none_match);
        }
        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        Ok(req)
    }

    pub fn oss_list_object_request(
        &self,
        path: &str,
        token: &str,
        delimiter: &str,
        limit: Option<usize>,
        start_after: Option<String>,
    ) -> Result<Request<RequestBody>> {
        let p = build_abs_path(&self.root, path);

        let endpoint = self.get_endpoint(false);
        let mut url = format!("{}/?list-type=2", endpoint);

        write!(url, "&delimiter={delimiter}").expect("write into string must succeed");
        // prefix
        if !p.is_empty() {
            write!(url, "&prefix={}", percent_encode_path(&p))
                .expect("write into string must succeed");
        }

        // max-key
        if let Some(limit) = limit {
            write!(url, "&max-keys={limit}").expect("write into string must succeed");
        }

        // continuation_token
        if !token.is_empty() {
            write!(url, "&continuation-token={}", percent_encode_path(token))
                .expect("write into string must succeed");
        }

        // start-after
        if let Some(start_after) = start_after {
            let start_after = build_abs_path(&self.root, &start_after);
            write!(url, "&start-after={}", percent_encode_path(&start_after))
                .expect("write into string must succeed");
        }

        let req = Request::get(&url)
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;
        Ok(req)
    }

    pub async fn oss_get_object(
        &self,
        path: &str,
        range: BytesRange,
        args: &OpRead,
        buf: oio::WritableBuf,
    ) -> Result<usize> {
        let mut req = self.oss_get_object_request(path, range, false, args)?;
        self.sign(&mut req).await?;

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

    pub async fn oss_head_object(
        &self,
        path: &str,
        if_match: Option<&str>,
        if_none_match: Option<&str>,
    ) -> Result<Metadata> {
        let mut req = self.oss_head_object_request(path, false, if_match, if_none_match)?;

        self.sign(&mut req).await?;
        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK => {
                body.consume().await?;
                parse_into_metadata(path, parts.headers())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn oss_put_object(
        &self,
        path: &str,
        size: Option<u64>,
        args: &OpWrite,
        body: RequestBody,
    ) -> Result<()> {
        let mut req = self.oss_put_object_request(path, size, args, body, false)?;

        self.sign(&mut req).await?;
        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::CREATED | StatusCode::OK => {
                body.consume().await?;
                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn oss_copy_object(&self, from: &str, to: &str) -> Result<()> {
        let source = build_abs_path(&self.root, from);
        let target = build_abs_path(&self.root, to);

        let url = format!(
            "{}/{}",
            self.get_endpoint(false),
            percent_encode_path(&target)
        );
        let source = format!("/{}/{}", self.bucket, percent_encode_path(&source));

        let mut req = Request::put(&url);

        req = self.insert_sse_headers(req);

        req = req.header("x-oss-copy-source", source);

        let mut req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
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

    pub async fn oss_list_object(
        &self,
        path: &str,
        token: &str,
        delimiter: &str,
        limit: Option<usize>,
        start_after: Option<String>,
    ) -> Result<ListObjectsOutput> {
        let mut req = self.oss_list_object_request(path, token, delimiter, limit, start_after)?;

        self.sign(&mut req).await?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK => {
                let output: ListObjectsOutput = body.to_xml().await?;
                Ok(output)
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn oss_delete_object(&self, path: &str) -> Result<()> {
        let mut req = self.oss_delete_object_request(path)?;
        self.sign(&mut req).await?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::NO_CONTENT | StatusCode::NOT_FOUND => {
                body.consume().await?;
                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn oss_delete_objects(&self, paths: Vec<String>) -> Result<DeleteObjectsResult> {
        let url = format!("{}/?delete", self.endpoint);

        let req = Request::post(&url);

        let content = quick_xml::se::to_string(&DeleteObjectsRequest {
            object: paths
                .into_iter()
                .map(|path| DeleteObjectsRequestObject {
                    key: build_abs_path(&self.root, &path),
                })
                .collect(),
        })
        .map_err(new_xml_deserialize_error)?;

        // Make sure content length has been set to avoid post with chunked encoding.
        let req = req.header(CONTENT_LENGTH, content.len());
        // Set content-type to `application/xml` to avoid mixed with form post.
        let req = req.header(CONTENT_TYPE, "application/xml");
        // Set content-md5 as required by API.
        let req = req.header("CONTENT-MD5", format_content_md5(content.as_bytes()));

        let mut req = req
            .body(RequestBody::Bytes(Bytes::from(content)))
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK => {
                let result = body.to_xml().await?;
                Ok(result)
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    fn get_endpoint(&self, is_presign: bool) -> &str {
        if is_presign {
            &self.presign_endpoint
        } else {
            &self.endpoint
        }
    }

    pub async fn oss_initiate_upload(
        &self,
        path: &str,
        content_type: Option<&str>,
        content_disposition: Option<&str>,
        cache_control: Option<&str>,
        is_presign: bool,
    ) -> Result<String> {
        let path = build_abs_path(&self.root, path);
        let endpoint = self.get_endpoint(is_presign);
        let url = format!("{}/{}?uploads", endpoint, percent_encode_path(&path));
        let mut req = Request::post(&url);
        if let Some(mime) = content_type {
            req = req.header(CONTENT_TYPE, mime);
        }
        if let Some(disposition) = content_disposition {
            req = req.header(CONTENT_DISPOSITION, disposition);
        }
        if let Some(cache_control) = cache_control {
            req = req.header(CACHE_CONTROL, cache_control);
        }
        req = self.insert_sse_headers(req);
        let mut req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;
        self.sign(&mut req).await?;
        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK => {
                let result: InitiateMultipartUploadResult = body.to_xml().await?;
                Ok(result.upload_id)
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    /// Creates a request to upload a part
    pub async fn oss_upload_part_request(
        &self,
        path: &str,
        upload_id: &str,
        part_number: usize,
        is_presign: bool,
        size: u64,
        body: RequestBody,
    ) -> Result<String> {
        let p = build_abs_path(&self.root, path);
        let endpoint = self.get_endpoint(is_presign);

        let url = format!(
            "{}/{}?partNumber={}&uploadId={}",
            endpoint,
            percent_encode_path(&p),
            part_number,
            percent_encode_path(upload_id)
        );

        let mut req = Request::put(&url);
        req = req.header(CONTENT_LENGTH, size);
        let mut req = req.body(body).map_err(new_request_build_error)?;
        self.sign(&mut req).await?;
        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK => {
                body.consume().await?;
                let etag = parse_etag(parts.headers())?
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::Unexpected,
                            "ETag not present in returning response",
                        )
                    })?
                    .to_string();

                Ok(etag)
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn oss_complete_multipart_upload_request(
        &self,
        path: &str,
        upload_id: &str,
        is_presign: bool,
        parts: Vec<MultipartUploadPart>,
    ) -> Result<()> {
        let p = build_abs_path(&self.root, path);
        let endpoint = self.get_endpoint(is_presign);
        let url = format!(
            "{}/{}?uploadId={}",
            endpoint,
            percent_encode_path(&p),
            percent_encode_path(upload_id)
        );

        let req = Request::post(&url);

        let content = quick_xml::se::to_string(&CompleteMultipartUploadRequest {
            part: parts.to_vec(),
        })
        .map_err(new_xml_deserialize_error)?;
        // Make sure content length has been set to avoid post with chunked encoding.
        let req = req.header(CONTENT_LENGTH, content.len());
        // Set content-type to `application/xml` to avoid mixed with form post.
        let req = req.header(CONTENT_TYPE, "application/xml");

        let mut req = req
            .body(RequestBody::Bytes(Bytes::from(content)))
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
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

    /// Abort an on-going multipart upload.
    /// reference docs https://www.alibabacloud.com/help/zh/oss/developer-reference/abortmultipartupload
    pub async fn oss_abort_multipart_upload(&self, path: &str, upload_id: &str) -> Result<()> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}?uploadId={}",
            self.endpoint,
            percent_encode_path(&p),
            percent_encode_path(upload_id)
        );

        let mut req = Request::delete(&url)
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;
        self.sign(&mut req).await?;
        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            // OSS returns code 204 if abort succeeds.
            StatusCode::NO_CONTENT => {
                body.consume().await?;
                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }
}

/// Request of DeleteObjects.
#[derive(Default, Debug, Serialize)]
#[serde(default, rename = "Delete", rename_all = "PascalCase")]
pub struct DeleteObjectsRequest {
    pub object: Vec<DeleteObjectsRequestObject>,
}

#[derive(Default, Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteObjectsRequestObject {
    pub key: String,
}

/// Result of DeleteObjects.
#[derive(Default, Debug, Deserialize)]
#[serde(default, rename = "DeleteResult", rename_all = "PascalCase")]
pub struct DeleteObjectsResult {
    pub deleted: Vec<DeleteObjectsResultDeleted>,
}

#[derive(Default, Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteObjectsResultDeleted {
    pub key: String,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct DeleteObjectsResultError {
    pub code: String,
    pub key: String,
    pub message: String,
}

#[derive(Default, Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct InitiateMultipartUploadResult {
    #[cfg(test)]
    pub bucket: String,
    #[cfg(test)]
    pub key: String,
    pub upload_id: String,
}

#[derive(Clone, Default, Debug, Serialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct MultipartUploadPart {
    #[serde(rename = "PartNumber")]
    pub part_number: usize,
    #[serde(rename = "ETag")]
    pub etag: String,
}

#[derive(Default, Debug, Serialize)]
#[serde(default, rename = "CompleteMultipartUpload", rename_all = "PascalCase")]
pub struct CompleteMultipartUploadRequest {
    pub part: Vec<MultipartUploadPart>,
}

#[derive(Default, Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct CompleteMultipartUploadResult {
    pub location: String,
    pub bucket: String,
    pub key: String,
    #[serde(rename = "ETag")]
    pub etag: String,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct ListObjectsOutput {
    pub prefix: String,
    pub max_keys: u64,
    pub encoding_type: String,
    pub is_truncated: bool,
    pub common_prefixes: Vec<CommonPrefix>,
    pub contents: Vec<ListObjectsOutputContent>,
    pub key_count: u64,

    pub next_continuation_token: Option<String>,
}

#[derive(Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "PascalCase")]
pub struct ListObjectsOutputContent {
    pub key: String,
    pub last_modified: String,
    #[serde(rename = "ETag")]
    pub etag: String,
    pub size: u64,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct CommonPrefix {
    pub prefix: String,
}

#[cfg(test)]
mod tests {
    use bytes::Buf;
    use bytes::Bytes;

    use super::*;

    /// This example is from https://www.alibabacloud.com/help/zh/object-storage-service/latest/deletemultipleobjects
    #[test]
    fn test_serialize_delete_objects_request() {
        let req = DeleteObjectsRequest {
            object: vec![
                DeleteObjectsRequestObject {
                    key: "multipart.data".to_string(),
                },
                DeleteObjectsRequestObject {
                    key: "test.jpg".to_string(),
                },
                DeleteObjectsRequestObject {
                    key: "demo.jpg".to_string(),
                },
            ],
        };

        let actual = quick_xml::se::to_string(&req).expect("must succeed");

        pretty_assertions::assert_eq!(
            actual,
            r#"<Delete>
  <Object>
    <Key>multipart.data</Key>
  </Object>
  <Object>
    <Key>test.jpg</Key>
  </Object>
  <Object>
    <Key>demo.jpg</Key>
  </Object>
</Delete>"#
                // Cleanup space and new line
                .replace([' ', '\n'], "")
        )
    }

    /// This example is from https://www.alibabacloud.com/help/zh/object-storage-service/latest/deletemultipleobjects
    #[test]
    fn test_deserialize_delete_objects_result() {
        let bs = Bytes::from(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="http://doc.oss-cn-hangzhou.aliyuncs.com">
    <Deleted>
       <Key>multipart.data</Key>
    </Deleted>
    <Deleted>
       <Key>test.jpg</Key>
    </Deleted>
    <Deleted>
       <Key>demo.jpg</Key>
    </Deleted>
</DeleteResult>"#,
        );

        let out: DeleteObjectsResult =
            quick_xml::de::from_reader(bs.reader()).expect("must success");

        assert_eq!(out.deleted.len(), 3);
        assert_eq!(out.deleted[0].key, "multipart.data");
        assert_eq!(out.deleted[1].key, "test.jpg");
        assert_eq!(out.deleted[2].key, "demo.jpg");
    }

    #[test]
    fn test_deserialize_initiate_multipart_upload_response() {
        let bs = Bytes::from(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://doc.oss-cn-hangzhou.aliyuncs.com">
    <Bucket>oss-example</Bucket>
    <Key>multipart.data</Key>
    <UploadId>0004B9894A22E5B1888A1E29F823****</UploadId>
</InitiateMultipartUploadResult>"#,
        );
        let out: InitiateMultipartUploadResult =
            quick_xml::de::from_reader(bs.reader()).expect("must success");

        assert_eq!("0004B9894A22E5B1888A1E29F823****", out.upload_id);
        assert_eq!("multipart.data", out.key);
        assert_eq!("oss-example", out.bucket);
    }

    #[test]
    fn test_serialize_complete_multipart_upload_request() {
        let req = CompleteMultipartUploadRequest {
            part: vec![
                MultipartUploadPart {
                    part_number: 1,
                    etag: "\"3349DC700140D7F86A0784842780****\"".to_string(),
                },
                MultipartUploadPart {
                    part_number: 5,
                    etag: "\"8EFDA8BE206636A695359836FE0A****\"".to_string(),
                },
                MultipartUploadPart {
                    part_number: 8,
                    etag: "\"8C315065167132444177411FDA14****\"".to_string(),
                },
            ],
        };

        // quick_xml::se::to_string()
        let mut serialized = String::new();
        let mut serializer = quick_xml::se::Serializer::new(&mut serialized);
        serializer.indent(' ', 4);
        req.serialize(serializer).unwrap();
        pretty_assertions::assert_eq!(
            serialized,
            r#"<CompleteMultipartUpload>
    <Part>
        <PartNumber>1</PartNumber>
        <ETag>"3349DC700140D7F86A0784842780****"</ETag>
    </Part>
    <Part>
        <PartNumber>5</PartNumber>
        <ETag>"8EFDA8BE206636A695359836FE0A****"</ETag>
    </Part>
    <Part>
        <PartNumber>8</PartNumber>
        <ETag>"8C315065167132444177411FDA14****"</ETag>
    </Part>
</CompleteMultipartUpload>"#
                .replace('"', "&quot;") /* Escape `"` by hand to address <https://github.com/tafia/quick-xml/issues/362> */
        )
    }

    #[test]
    fn test_deserialize_complete_oss_multipart_result() {
        let bytes = Bytes::from(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://doc.oss-cn-hangzhou.aliyuncs.com">
    <EncodingType>url</EncodingType>
    <Location>http://oss-example.oss-cn-hangzhou.aliyuncs.com /multipart.data</Location>
    <Bucket>oss-example</Bucket>
    <Key>multipart.data</Key>
    <ETag>"B864DB6A936D376F9F8D3ED3BBE540****"</ETag>
</CompleteMultipartUploadResult>"#,
        );

        let result: CompleteMultipartUploadResult =
            quick_xml::de::from_reader(bytes.reader()).unwrap();
        assert_eq!("\"B864DB6A936D376F9F8D3ED3BBE540****\"", result.etag);
        assert_eq!(
            "http://oss-example.oss-cn-hangzhou.aliyuncs.com /multipart.data",
            result.location
        );
        assert_eq!("oss-example", result.bucket);
        assert_eq!("multipart.data", result.key);
    }

    #[test]
    fn test_parse_list_output() {
        let bs = bytes::Bytes::from(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="https://doc.oss-cn-hangzhou.aliyuncs.com">
    <Name>examplebucket</Name>
    <Prefix></Prefix>
    <StartAfter>b</StartAfter>
    <MaxKeys>3</MaxKeys>
    <EncodingType>url</EncodingType>
    <IsTruncated>true</IsTruncated>
    <NextContinuationToken>CgJiYw--</NextContinuationToken>
    <Contents>
        <Key>b/c</Key>
        <LastModified>2020-05-18T05:45:54.000Z</LastModified>
        <ETag>"35A27C2B9EAEEB6F48FD7FB5861D****"</ETag>
        <Size>25</Size>
        <StorageClass>STANDARD</StorageClass>
        <Owner>
            <ID>1686240967192623</ID>
            <DisplayName>1686240967192623</DisplayName>
        </Owner>
    </Contents>
    <Contents>
        <Key>ba</Key>
        <LastModified>2020-05-18T11:17:58.000Z</LastModified>
        <ETag>"35A27C2B9EAEEB6F48FD7FB5861D****"</ETag>
        <Size>25</Size>
        <StorageClass>STANDARD</StorageClass>
        <Owner>
            <ID>1686240967192623</ID>
            <DisplayName>1686240967192623</DisplayName>
        </Owner>
    </Contents>
    <Contents>
        <Key>bc</Key>
        <LastModified>2020-05-18T05:45:59.000Z</LastModified>
        <ETag>"35A27C2B9EAEEB6F48FD7FB5861D****"</ETag>
        <Size>25</Size>
        <StorageClass>STANDARD</StorageClass>
        <Owner>
            <ID>1686240967192623</ID>
            <DisplayName>1686240967192623</DisplayName>
        </Owner>
    </Contents>
    <KeyCount>3</KeyCount>
</ListBucketResult>"#,
        );

        let out: ListObjectsOutput = quick_xml::de::from_reader(bs.reader()).expect("must_success");

        assert!(out.is_truncated);
        assert_eq!(out.next_continuation_token, Some("CgJiYw--".to_string()));
        assert!(out.common_prefixes.is_empty());

        assert_eq!(
            out.contents,
            vec![
                ListObjectsOutputContent {
                    key: "b/c".to_string(),
                    last_modified: "2020-05-18T05:45:54.000Z".to_string(),
                    etag: "\"35A27C2B9EAEEB6F48FD7FB5861D****\"".to_string(),
                    size: 25,
                },
                ListObjectsOutputContent {
                    key: "ba".to_string(),
                    last_modified: "2020-05-18T11:17:58.000Z".to_string(),
                    etag: "\"35A27C2B9EAEEB6F48FD7FB5861D****\"".to_string(),
                    size: 25,
                },
                ListObjectsOutputContent {
                    key: "bc".to_string(),
                    last_modified: "2020-05-18T05:45:59.000Z".to_string(),
                    etag: "\"35A27C2B9EAEEB6F48FD7FB5861D****\"".to_string(),
                    size: 25,
                }
            ]
        )
    }
}
