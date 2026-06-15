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

use http::Request;
use http::Response;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::header::HOST;
use http::header::HeaderValue;
use http::header::USER_AGENT;
use reqsign_core::Context;
use reqsign_core::OsEnv;
use reqsign_core::Signer;
use reqsign_volcengine_tos::Credential;
use reqsign_volcengine_tos::{percent_encode_path, percent_encode_query};
use serde::Deserialize;
use serde::Serialize;

use opendal_core::raw::*;
use opendal_core::*;

pub mod constants {
    pub const X_TOS_ACL: &str = "x-tos-acl";
    pub const X_TOS_COPY_SOURCE: &str = "x-tos-copy-source";
    pub const X_TOS_COPY_SOURCE_RANGE: &str = "x-tos-copy-source-range";
    pub const X_TOS_FORBID_OVERWRITE: &str = "x-tos-forbid-overwrite";

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
    pub info: ServiceInfo,
    pub capability: Capability,

    pub bucket: String,
    pub endpoint: String, // full endpoint with scheme, e.g. https://tos-cn-beijing.volces.com
    pub endpoint_domain: String, // endpoint domain without scheme, e.g. tos-cn-beijing.volces.com
    pub root: String,
    pub default_storage_class: Option<String>,
    pub skip_signature: bool,

    pub signer: Signer<Credential>,
}

pub(crate) struct TosUploadPartCopyRequest<'a> {
    pub from: &'a str,
    pub to: &'a str,
    pub upload_id: &'a str,
    pub part_number: usize,
    pub range: BytesRange,
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
    fn signer(&self, ctx: &OperationContext) -> Signer<Credential> {
        self.signer.clone().with_context(
            Context::new()
                .with_file_read(reqsign_file_read_tokio::TokioFileRead)
                .with_http_send(HttpClientHttpSend::new(ctx.http_client().clone()))
                .with_env(OsEnv),
        )
    }

    pub async fn send(
        &self,
        ctx: &OperationContext,
        req: Request<Buffer>,
    ) -> Result<Response<Buffer>> {
        let (mut parts, body) = req.into_parts();

        if !self.skip_signature {
            self.signer(ctx)
                .sign(&mut parts, None)
                .await
                .map_err(|e| new_request_sign_error(e.into()))?;
        }

        parts.headers.insert(
            USER_AGENT,
            HeaderValue::from_str(&format!("opendal/{VERSION}"))
                .expect("user agent must be valid header value"),
        );

        let resp = ctx
            .http_client()
            .send(Request::from_parts(parts, body))
            .await?;

        Ok(resp)
    }

    pub async fn fetch(
        &self,
        ctx: &OperationContext,
        req: Request<Buffer>,
    ) -> Result<Response<HttpBody>> {
        let (mut parts, body) = req.into_parts();

        if !self.skip_signature {
            self.signer(ctx)
                .sign(&mut parts, None)
                .await
                .map_err(|e| new_request_sign_error(e.into()))?;

            parts.headers.remove(HOST);
        }

        parts.headers.insert(
            USER_AGENT,
            HeaderValue::from_str(&format!("opendal/{VERSION}"))
                .expect("user agent must be valid header value"),
        );

        ctx.http_client()
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
        ctx: &OperationContext,
        path: &str,
        range: BytesRange,
        args: &OpRead,
    ) -> Result<Response<HttpBody>> {
        let req = self.tos_get_object_request(path, range, args)?;
        self.fetch(ctx, req).await
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

    pub async fn tos_head_object(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpStat,
    ) -> Result<Response<Buffer>> {
        let req = self.tos_head_object_request(path, args)?;
        self.send(ctx, req).await
    }

    pub async fn tos_delete_object(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: &OpDelete,
    ) -> Result<Response<Buffer>> {
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

        self.send(ctx, req).await
    }

    pub async fn tos_delete_objects(
        &self,
        ctx: &OperationContext,
        paths: &[(String, OpDelete)],
    ) -> Result<Response<Buffer>> {
        let url = format!("https://{}.{}?delete", self.bucket, self.endpoint_domain);

        let mut req = Request::post(&url);

        let content = serde_json::to_string(&DeleteObjectsRequest {
            objects: paths
                .iter()
                .map(|(path, op)| DeleteObjectsRequestObject {
                    key: build_abs_path(&self.root, path),
                    version_id: op.version().map(|v| v.to_owned()),
                })
                .collect(),
            quiet: false,
        })
        .map_err(new_json_serialize_error)?;

        req = req.header(CONTENT_LENGTH, content.len());
        req = req.header(CONTENT_TYPE, "application/json");
        req = req.header("CONTENT-MD5", format_content_md5(content.as_bytes()));

        req = req.extension(Operation::Delete);

        let req = req
            .body(Buffer::from(content))
            .map_err(new_request_build_error)?;

        self.send(ctx, req).await
    }

    pub async fn tos_copy_object(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: &OpCopy,
    ) -> Result<Response<Buffer>> {
        let source = build_abs_path(&self.root, from);
        let target = build_abs_path(&self.root, to);

        let url = format!(
            "https://{}.{}/{}",
            self.bucket,
            self.endpoint_domain,
            percent_encode_path(&target)
        );
        let source = format!("/{}/{}", self.bucket, percent_encode_path(&source));

        let mut req = Request::put(&url)
            .header(constants::X_TOS_COPY_SOURCE, source)
            .header(constants::X_TOS_ACL, "default");

        if args.if_not_exists() {
            req = req.header(constants::X_TOS_FORBID_OVERWRITE, "true");
        }

        let req = req
            .extension(Operation::Copy)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.send(ctx, req).await
    }

    pub async fn tos_initiate_multipart_copy(
        &self,
        ctx: &OperationContext,
        path: &str,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "https://{}.{}/{}?uploads",
            self.bucket,
            self.endpoint_domain,
            percent_encode_path(&p)
        );

        let mut req = Request::post(&url).header(constants::X_TOS_ACL, "default");

        if let Some(v) = &self.default_storage_class {
            req = req.header(
                http::HeaderName::from_static(constants::X_TOS_STORAGE_CLASS),
                v,
            );
        }

        let req = req
            .extension(Operation::Copy)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.send(ctx, req).await
    }

    pub fn tos_upload_part_copy_request(
        &self,
        input: TosUploadPartCopyRequest<'_>,
    ) -> Result<Request<Buffer>> {
        let source = build_abs_path(&self.root, input.from);
        let target = build_abs_path(&self.root, input.to);

        let url = format!(
            "https://{}.{}/{}?partNumber={}&uploadId={}",
            self.bucket,
            self.endpoint_domain,
            percent_encode_path(&target),
            input.part_number,
            percent_encode_query(input.upload_id)
        );
        let source = format!("/{}/{}", self.bucket, percent_encode_path(&source));

        let req = Request::put(&url)
            .extension(Operation::Copy)
            .header(constants::X_TOS_COPY_SOURCE, source)
            .header(constants::X_TOS_COPY_SOURCE_RANGE, input.range.to_header());

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        Ok(req)
    }

    pub async fn tos_complete_multipart_copy(
        &self,
        ctx: &OperationContext,
        path: &str,
        upload_id: &str,
        parts: Vec<CompleteMultipartUploadRequestPart>,
        args: &OpCopy,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "https://{}.{}/{}?uploadId={}",
            self.bucket,
            self.endpoint_domain,
            percent_encode_path(&p),
            percent_encode_query(upload_id)
        );

        let content = serde_json::to_string(&CompleteMultipartUploadRequest { parts })
            .map_err(new_json_serialize_error)?;

        let mut req = Request::post(&url)
            .header(CONTENT_LENGTH, content.len())
            .header(CONTENT_TYPE, "application/json");

        if args.if_not_exists() {
            req = req.header(constants::X_TOS_FORBID_OVERWRITE, "true");
        }

        let req = req
            .extension(Operation::Copy)
            .body(Buffer::from(content))
            .map_err(new_request_build_error)?;

        self.send(ctx, req).await
    }

    pub async fn tos_abort_multipart_copy(
        &self,
        ctx: &OperationContext,
        path: &str,
        upload_id: &str,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "https://{}.{}/{}?uploadId={}",
            self.bucket,
            self.endpoint_domain,
            percent_encode_path(&p),
            percent_encode_query(upload_id)
        );

        let req = Request::delete(&url)
            .extension(Operation::Copy)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.send(ctx, req).await
    }

    pub async fn tos_list_objects_v2(
        &self,
        ctx: &OperationContext,
        path: &str,
        continuation_token: &str,
        delimiter: &str,
        limit: Option<usize>,
        start_after: Option<String>,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let mut url =
            QueryPairsWriter::new(&format!("https://{}.{}", self.bucket, self.endpoint_domain));
        url = url.push("list-type", "2");

        if !p.is_empty() {
            url = url.push("prefix", &percent_encode_query(&p));
        }
        if !delimiter.is_empty() {
            url = url.push("delimiter", &percent_encode_query(delimiter));
        }
        if let Some(limit) = limit {
            url = url.push("max-keys", &limit.to_string());
        }
        if let Some(start_after) = start_after {
            if path.is_empty() || path == "/" || start_after.starts_with(path) {
                let start_after = build_abs_path(&self.root, &start_after);
                url = url.push("start-after", &percent_encode_query(&start_after));
            }
        }
        if !continuation_token.is_empty() {
            url = url.push(
                "continuation-token",
                &percent_encode_query(continuation_token),
            );
        }

        let req = Request::get(url.finish())
            .extension(Operation::List)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.send(ctx, req).await
    }

    pub async fn tos_list_object_versions(
        &self,
        ctx: &OperationContext,
        prefix: &str,
        delimiter: &str,
        limit: Option<usize>,
        key_marker: &str,
        version_id_marker: &str,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, prefix);

        let mut url =
            QueryPairsWriter::new(&format!("https://{}.{}", self.bucket, self.endpoint_domain));
        url = url.push("versions", "");

        if !p.is_empty() {
            url = url.push("prefix", &percent_encode_query(&p));
        }
        if !delimiter.is_empty() {
            url = url.push("delimiter", &percent_encode_query(delimiter));
        }
        if let Some(limit) = limit {
            url = url.push("max-keys", &limit.to_string());
        }
        if !key_marker.is_empty() {
            url = url.push("key-marker", &percent_encode_query(key_marker));
        }
        if !version_id_marker.is_empty() {
            url = url.push(
                "version-id-marker",
                &percent_encode_query(version_id_marker),
            );
        }

        let req = Request::get(url.finish())
            .extension(Operation::List)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.send(ctx, req).await
    }

    pub async fn tos_initiate_multipart_upload(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: &OpWrite,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "https://{}.{}/{}?uploads",
            self.bucket,
            self.endpoint_domain,
            percent_encode_path(&p)
        );

        let mut req = Request::post(&url);

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

        req = req.extension(Operation::Write);

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.send(ctx, req).await
    }

    pub fn tos_upload_part_request(
        &self,
        path: &str,
        upload_id: &str,
        part_number: usize,
        size: u64,
        body: Buffer,
    ) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "https://{}.{}/{}?partNumber={}&uploadId={}",
            self.bucket,
            self.endpoint_domain,
            percent_encode_path(&p),
            part_number,
            percent_encode_query(upload_id)
        );

        let req = Request::put(&url)
            .header(CONTENT_LENGTH, size)
            .extension(Operation::Write)
            .body(body)
            .map_err(new_request_build_error)?;

        Ok(req)
    }

    pub async fn tos_complete_multipart_upload(
        &self,
        ctx: &OperationContext,
        path: &str,
        upload_id: &str,
        parts: Vec<CompleteMultipartUploadRequestPart>,
        args: &OpWrite,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "https://{}.{}/{}?uploadId={}",
            self.bucket,
            self.endpoint_domain,
            percent_encode_path(&p),
            percent_encode_query(upload_id)
        );

        let content = serde_json::to_string(&CompleteMultipartUploadRequest { parts })
            .map_err(new_json_serialize_error)?;

        let mut req = Request::post(&url)
            .header(CONTENT_LENGTH, content.len())
            .header(CONTENT_TYPE, "application/json");

        if let Some(if_match) = args.if_match() {
            req = req.header(http::header::IF_MATCH, if_match);
        }
        if args.if_not_exists() {
            req = req.header(http::header::IF_NONE_MATCH, "*");
        }

        let req = req
            .extension(Operation::Write)
            .body(Buffer::from(content))
            .map_err(new_request_build_error)?;

        self.send(ctx, req).await
    }

    pub async fn tos_abort_multipart_upload(
        &self,
        ctx: &OperationContext,
        path: &str,
        upload_id: &str,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "https://{}.{}/{}?uploadId={}",
            self.bucket,
            self.endpoint_domain,
            percent_encode_path(&p),
            percent_encode_query(upload_id)
        );

        let req = Request::delete(&url)
            .extension(Operation::Write)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.send(ctx, req).await
    }
}

#[derive(Default, Debug, Serialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct CompleteMultipartUploadRequest {
    pub parts: Vec<CompleteMultipartUploadRequestPart>,
}

#[derive(Clone, Default, Debug, Serialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct CompleteMultipartUploadRequestPart {
    pub part_number: usize,
    #[serde(rename = "ETag")]
    pub etag: String,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct InitiateMultipartUploadResult {
    #[serde(alias = "UploadID")]
    pub upload_id: String,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct CompleteMultipartUploadResult {
    pub bucket: String,
    pub key: String,
    #[serde(rename = "ETag")]
    pub etag: String,
    pub location: String,
    pub version_id: String,
    pub code: String,
    pub message: String,
    pub request_id: String,
}

#[derive(Default, Debug, Serialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct DeleteObjectsRequest {
    pub objects: Vec<DeleteObjectsRequestObject>,
    pub quiet: bool,
}

#[derive(Default, Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteObjectsRequestObject {
    pub key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct DeleteObjectsResult {
    pub deleted: Vec<DeleteObjectsResultDeleted>,
    #[serde(alias = "Error")]
    pub errors: Vec<DeleteObjectsResultError>,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct DeleteObjectsResultDeleted {
    pub key: String,
    #[serde(alias = "VersionID")]
    pub version_id: Option<String>,
    pub delete_marker: bool,
    #[serde(alias = "DeleteMarkerVersionID")]
    pub delete_marker_version_id: Option<String>,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct DeleteObjectsResultError {
    pub code: String,
    pub key: String,
    pub message: String,
    #[serde(alias = "VersionID")]
    pub version_id: Option<String>,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct ListObjectsOutputV2 {
    pub name: String,
    pub prefix: String,
    pub key_count: usize,
    pub max_keys: usize,
    pub is_truncated: bool,
    pub delimiter: String,
    pub next_continuation_token: Option<String>,
    pub common_prefixes: Vec<OutputCommonPrefix>,
    pub contents: Vec<ListObjectsOutputContent>,
}

#[derive(Default, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListObjectsOutputContent {
    pub key: String,
    pub size: u64,
    pub last_modified: String,
    #[serde(rename = "ETag")]
    pub etag: Option<String>,
}

#[derive(Default, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct OutputCommonPrefix {
    pub prefix: String,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct ListObjectVersionsOutput {
    pub name: String,
    pub prefix: String,
    pub key_marker: String,
    #[serde(alias = "VersionIDMarker")]
    pub version_id_marker: String,
    pub max_keys: usize,
    pub delimiter: String,
    pub is_truncated: Option<bool>,
    pub next_key_marker: Option<String>,
    #[serde(alias = "NextVersionIDMarker")]
    pub next_version_id_marker: Option<String>,
    pub common_prefixes: Vec<OutputCommonPrefix>,
    pub versions: Vec<ListObjectVersionsOutputVersion>,
    pub delete_markers: Vec<ListObjectVersionsOutputDeleteMarker>,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct ListObjectVersionsOutputVersion {
    pub key: String,
    #[serde(alias = "VersionID")]
    pub version_id: String,
    pub is_latest: bool,
    pub last_modified: String,
    #[serde(rename = "ETag")]
    pub etag: Option<String>,
    pub size: u64,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct ListObjectVersionsOutputDeleteMarker {
    pub key: String,
    #[serde(alias = "VersionID")]
    pub version_id: String,
    pub is_latest: bool,
    pub last_modified: String,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct UploadPartCopyOutput {
    #[serde(rename = "ETag")]
    pub etag: String,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct CopyObjectOutput {
    #[serde(rename = "ETag")]
    pub etag: String,
}
