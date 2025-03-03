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
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use constants::X_OSS_META_PREFIX;
use http::header::CACHE_CONTROL;
use http::header::CONTENT_DISPOSITION;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::header::IF_MATCH;
use http::header::IF_MODIFIED_SINCE;
use http::header::IF_NONE_MATCH;
use http::header::IF_UNMODIFIED_SINCE;
use http::header::RANGE;
use http::HeaderMap;
use http::HeaderName;
use http::HeaderValue;
use http::Request;
use http::Response;
use reqsign::AliyunCredential;
use reqsign::AliyunLoader;
use reqsign::AliyunOssSigner;
use serde::Deserialize;
use serde::Serialize;

use crate::raw::*;
use crate::services::oss::core::constants::X_OSS_FORBID_OVERWRITE;
use crate::*;

pub mod constants {
    pub const X_OSS_SERVER_SIDE_ENCRYPTION: &str = "x-oss-server-side-encryption";

    pub const X_OSS_SERVER_SIDE_ENCRYPTION_KEY_ID: &str = "x-oss-server-side-encryption-key-id";

    pub const X_OSS_FORBID_OVERWRITE: &str = "x-oss-forbid-overwrite";

    pub const X_OSS_VERSION_ID: &str = "x-oss-version-id";

    pub const RESPONSE_CONTENT_DISPOSITION: &str = "response-content-disposition";

    pub const OSS_QUERY_VERSION_ID: &str = "versionId";

    pub const X_OSS_META_PREFIX: &str = "x-oss-meta-";
}

pub struct OssCore {
    pub info: Arc<AccessorInfo>,

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

    pub loader: AliyunLoader,
    pub signer: AliyunOssSigner,
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

    #[inline]
    pub async fn send(&self, req: Request<Buffer>) -> Result<Response<Buffer>> {
        self.info.http_client().send(req).await
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

    fn insert_metadata_headers(
        &self,
        mut req: http::request::Builder,
        size: Option<u64>,
        args: &OpWrite,
    ) -> Result<http::request::Builder> {
        req = req.header(CONTENT_LENGTH, size.unwrap_or_default());

        if let Some(mime) = args.content_type() {
            req = req.header(CONTENT_TYPE, mime);
        }

        if let Some(pos) = args.content_disposition() {
            req = req.header(CONTENT_DISPOSITION, pos);
        }

        if let Some(cache_control) = args.cache_control() {
            req = req.header(CACHE_CONTROL, cache_control);
        }

        // TODO: disable if not exists while version has been enabled.
        //
        // Specifies whether the object that is uploaded by calling the PutObject operation
        // overwrites the existing object that has the same name. When versioning is enabled
        // or suspended for the bucket to which you want to upload the object, the
        // x-oss-forbid-overwrite header does not take effect. In this case, the object that
        // is uploaded by calling the PutObject operation overwrites the existing object that
        // has the same name.
        //
        // ref: https://www.alibabacloud.com/help/en/oss/developer-reference/putobject?spm=a2c63.p38356.0.0.39ef75e93o0Xtz
        if args.if_not_exists() {
            req = req.header(X_OSS_FORBID_OVERWRITE, "true");
        }

        if let Some(user_metadata) = args.user_metadata() {
            for (key, value) in user_metadata {
                // before insert user defined metadata header, add prefix to the header name
                if !self.check_user_metadata_key(key) {
                    return Err(Error::new(
                        ErrorKind::Unsupported,
                        "the format of the user metadata key is invalid, please refer the document",
                    ));
                }
                req = req.header(format!("{X_OSS_META_PREFIX}{key}"), value)
            }
        }

        Ok(req)
    }

    // According to https://help.aliyun.com/zh/oss/developer-reference/putobject
    // there are some limits in user defined metadata key
    fn check_user_metadata_key(&self, key: &str) -> bool {
        key.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
    }

    /// parse_metadata will parse http headers(including standards http headers
    /// and user defined metadata header) into Metadata.
    ///
    /// # Arguments
    ///
    /// * `user_metadata_prefix` is the prefix of user defined metadata key
    ///
    /// # Notes
    ///
    /// before return the user defined metadata, we'll strip the user_metadata_prefix from the key
    pub fn parse_metadata(&self, path: &str, headers: &HeaderMap) -> Result<Metadata> {
        let mut m = parse_into_metadata(path, headers)?;
        let user_meta = parse_prefixed_headers(headers, X_OSS_META_PREFIX);
        if !user_meta.is_empty() {
            m.with_user_metadata(user_meta);
        }

        Ok(m)
    }
}

impl OssCore {
    #[allow(clippy::too_many_arguments)]
    pub fn oss_put_object_request(
        &self,
        path: &str,
        size: Option<u64>,
        args: &OpWrite,
        body: Buffer,
        is_presign: bool,
    ) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);
        let endpoint = self.get_endpoint(is_presign);
        let url = format!("{}/{}", endpoint, percent_encode_path(&p));

        let mut req = Request::put(&url);

        req = self.insert_metadata_headers(req, size, args)?;

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
        body: Buffer,
    ) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);
        let endpoint = self.get_endpoint(false);
        let url = format!(
            "{}/{}?append&position={}",
            endpoint,
            percent_encode_path(&p),
            position
        );

        let mut req = Request::post(&url);

        req = self.insert_metadata_headers(req, Some(size), args)?;

        // set sse headers
        req = self.insert_sse_headers(req);

        let req = req.body(body).map_err(new_request_build_error)?;
        Ok(req)
    }

    pub fn oss_get_object_request(
        &self,
        path: &str,
        is_presign: bool,
        args: &OpRead,
    ) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);
        let endpoint = self.get_endpoint(is_presign);
        let range = args.range();
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
        if let Some(version) = args.version() {
            query_args.push(format!(
                "{}={}",
                constants::OSS_QUERY_VERSION_ID,
                percent_encode_path(version)
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

        if let Some(if_modified_since) = args.if_modified_since() {
            req = req.header(
                IF_MODIFIED_SINCE,
                format_datetime_into_http_date(if_modified_since),
            );
        }

        if let Some(if_unmodified_since) = args.if_unmodified_since() {
            req = req.header(
                IF_UNMODIFIED_SINCE,
                format_datetime_into_http_date(if_unmodified_since),
            );
        }

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        Ok(req)
    }

    fn oss_delete_object_request(&self, path: &str, args: &OpDelete) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);
        let endpoint = self.get_endpoint(false);
        let mut url = format!("{}/{}", endpoint, percent_encode_path(&p));

        let mut query_args = Vec::new();

        if let Some(version) = args.version() {
            query_args.push(format!(
                "{}={}",
                constants::OSS_QUERY_VERSION_ID,
                percent_encode_path(version)
            ))
        }

        if !query_args.is_empty() {
            url.push_str(&format!("?{}", query_args.join("&")));
        }

        let req = Request::delete(&url);

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        Ok(req)
    }

    pub fn oss_head_object_request(
        &self,
        path: &str,
        is_presign: bool,
        args: &OpStat,
    ) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);
        let endpoint = self.get_endpoint(is_presign);
        let mut url = format!("{}/{}", endpoint, percent_encode_path(&p));

        let mut query_args = Vec::new();

        if let Some(version) = args.version() {
            query_args.push(format!(
                "{}={}",
                constants::OSS_QUERY_VERSION_ID,
                percent_encode_path(version)
            ))
        }

        if !query_args.is_empty() {
            url.push_str(&format!("?{}", query_args.join("&")));
        }

        let mut req = Request::head(&url);
        if let Some(if_match) = args.if_match() {
            req = req.header(IF_MATCH, if_match)
        }
        if let Some(if_none_match) = args.if_none_match() {
            req = req.header(IF_NONE_MATCH, if_none_match);
        }
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        Ok(req)
    }

    pub fn oss_list_object_request(
        &self,
        path: &str,
        token: &str,
        delimiter: &str,
        limit: Option<usize>,
        start_after: Option<String>,
    ) -> Result<Request<Buffer>> {
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
            .body(Buffer::new())
            .map_err(new_request_build_error)?;
        Ok(req)
    }

    pub async fn oss_get_object(&self, path: &str, args: &OpRead) -> Result<Response<HttpBody>> {
        let mut req = self.oss_get_object_request(path, false, args)?;
        self.sign(&mut req).await?;
        self.info.http_client().fetch(req).await
    }

    pub async fn oss_head_object(&self, path: &str, args: &OpStat) -> Result<Response<Buffer>> {
        let mut req = self.oss_head_object_request(path, false, args)?;

        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn oss_copy_object(&self, from: &str, to: &str) -> Result<Response<Buffer>> {
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

        let mut req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn oss_list_object(
        &self,
        path: &str,
        token: &str,
        delimiter: &str,
        limit: Option<usize>,
        start_after: Option<String>,
    ) -> Result<Response<Buffer>> {
        let mut req = self.oss_list_object_request(path, token, delimiter, limit, start_after)?;

        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn oss_list_object_versions(
        &self,
        prefix: &str,
        delimiter: &str,
        limit: Option<usize>,
        key_marker: &str,
        version_id_marker: &str,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, prefix);

        let mut url = format!("{}?versions", self.endpoint);
        if !p.is_empty() {
            write!(url, "&prefix={}", percent_encode_path(p.as_str()))
                .expect("write into string must succeed");
        }
        if !delimiter.is_empty() {
            write!(url, "&delimiter={}", delimiter).expect("write into string must succeed");
        }

        if let Some(limit) = limit {
            write!(url, "&max-keys={}", limit).expect("write into string must succeed");
        }
        if !key_marker.is_empty() {
            write!(url, "&key-marker={}", percent_encode_path(key_marker))
                .expect("write into string must succeed");
        }
        if !version_id_marker.is_empty() {
            write!(
                url,
                "&version-id-marker={}",
                percent_encode_path(version_id_marker)
            )
            .expect("write into string must succeed");
        }

        let mut req = Request::get(&url)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;

        self.send(req).await
    }

    pub async fn oss_delete_object(&self, path: &str, args: &OpDelete) -> Result<Response<Buffer>> {
        let mut req = self.oss_delete_object_request(path, args)?;
        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn oss_delete_objects(
        &self,
        paths: Vec<(String, OpDelete)>,
    ) -> Result<Response<Buffer>> {
        let url = format!("{}/?delete", self.endpoint);

        let req = Request::post(&url);

        let content = quick_xml::se::to_string(&DeleteObjectsRequest {
            object: paths
                .into_iter()
                .map(|(path, op)| DeleteObjectsRequestObject {
                    key: build_abs_path(&self.root, &path),
                    version_id: op.version().map(|v| v.to_owned()),
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
            .body(Buffer::from(Bytes::from(content)))
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;

        self.send(req).await
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
    ) -> Result<Response<Buffer>> {
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
        let mut req = req.body(Buffer::new()).map_err(new_request_build_error)?;
        self.sign(&mut req).await?;
        self.send(req).await
    }

    /// Creates a request to upload a part
    pub async fn oss_upload_part_request(
        &self,
        path: &str,
        upload_id: &str,
        part_number: usize,
        is_presign: bool,
        size: u64,
        body: Buffer,
    ) -> Result<Response<Buffer>> {
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
        self.send(req).await
    }

    pub async fn oss_complete_multipart_upload_request(
        &self,
        path: &str,
        upload_id: &str,
        is_presign: bool,
        parts: Vec<MultipartUploadPart>,
    ) -> Result<Response<Buffer>> {
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
            .body(Buffer::from(Bytes::from(content)))
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
        self.send(req).await
    }

    /// Abort an ongoing multipart upload.
    /// reference docs https://www.alibabacloud.com/help/zh/oss/developer-reference/abortmultipartupload
    pub async fn oss_abort_multipart_upload(
        &self,
        path: &str,
        upload_id: &str,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}?uploadId={}",
            self.endpoint,
            percent_encode_path(&p),
            percent_encode_path(upload_id)
        );

        let mut req = Request::delete(&url)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;
        self.sign(&mut req).await?;
        self.send(req).await
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
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
    pub version_id: Option<String>,
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

#[derive(Default, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct OutputCommonPrefix {
    pub prefix: String,
}

/// Output of ListObjectVersions
#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct ListObjectVersionsOutput {
    pub is_truncated: Option<bool>,
    pub next_key_marker: Option<String>,
    pub next_version_id_marker: Option<String>,
    pub common_prefixes: Vec<OutputCommonPrefix>,
    pub version: Vec<ListObjectVersionsOutputVersion>,
    pub delete_marker: Vec<ListObjectVersionsOutputDeleteMarker>,
}

#[derive(Default, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListObjectVersionsOutputVersion {
    pub key: String,
    pub version_id: String,
    pub is_latest: bool,
    pub size: u64,
    pub last_modified: String,
    #[serde(rename = "ETag")]
    pub etag: Option<String>,
}

#[derive(Default, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListObjectVersionsOutputDeleteMarker {
    pub key: String,
    pub version_id: String,
    pub is_latest: bool,
    pub last_modified: String,
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
                    version_id: None,
                },
                DeleteObjectsRequestObject {
                    key: "test.jpg".to_string(),
                    version_id: None,
                },
                DeleteObjectsRequestObject {
                    key: "demo.jpg".to_string(),
                    version_id: None,
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
        )
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
