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

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Write;

use bytes::Buf;
use bytes::Bytes;
use constants::*;
use http::Request;
use http::Response;
use http::header::CACHE_CONTROL;
use http::header::CONTENT_DISPOSITION;
use http::header::CONTENT_ENCODING;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::header::HOST;
use http::header::IF_MATCH;
use http::header::IF_MODIFIED_SINCE;
use http::header::IF_NONE_MATCH;
use http::header::IF_UNMODIFIED_SINCE;
use reqsign_core::{Context, Signer};
use reqsign_google::Credential;
use serde::Deserialize;
use serde::Serialize;

use opendal_core::raw::*;
use opendal_core::*;

pub mod constants {
    pub const GCS_REWRITE_MIN_CHUNK_SIZE: usize = 1024 * 1024;
    #[cfg(target_pointer_width = "64")]
    pub const GCS_REWRITE_MAX_CHUNK_SIZE: usize =
        i64::MAX as usize / GCS_REWRITE_MIN_CHUNK_SIZE * GCS_REWRITE_MIN_CHUNK_SIZE;
    #[cfg(not(target_pointer_width = "64"))]
    pub const GCS_REWRITE_MAX_CHUNK_SIZE: usize =
        usize::MAX / GCS_REWRITE_MIN_CHUNK_SIZE * GCS_REWRITE_MIN_CHUNK_SIZE;

    pub const X_GOOG_ACL: &str = "x-goog-acl";
    pub const X_GOOG_STORAGE_CLASS: &str = "x-goog-storage-class";
    pub const X_GOOG_META_PREFIX: &str = "x-goog-meta-";
}

pub struct GcsCore {
    pub info: ServiceInfo,
    pub capability: Capability,
    pub endpoint: String,
    pub bucket: String,
    pub root: String,

    pub signer: Signer<Credential>,
    pub sign_ctx: Context,

    pub predefined_acl: Option<String>,
    pub default_storage_class: Option<String>,

    pub skip_signature: bool,
}

impl Debug for GcsCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcsCore")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl GcsCore {
    fn signer(&self, ctx: &OperationContext) -> Signer<Credential> {
        self.signer.clone().with_context(
            self.sign_ctx
                .clone()
                .with_http_send(ctx.http_transport().clone()),
        )
    }

    pub async fn sign<T>(&self, ctx: &OperationContext, req: Request<T>) -> Result<Request<T>> {
        if self.skip_signature {
            return Ok(req);
        }

        let (mut parts, body) = req.into_parts();

        self.signer(ctx)
            .sign(&mut parts, None)
            .await
            .map_err(|err| new_request_sign_error(err.into()))?;

        // Always remove host header, let users' client to set it based on
        // HTTP version.
        //
        // As discussed in <https://github.com/seanmonstar/reqwest/issues/1809>,
        // google server could send RST_STREAM of PROTOCOL_ERROR if our
        // request contains host header.
        parts.headers.remove(HOST);

        Ok(Request::from_parts(parts, body))
    }

    pub async fn sign_query<T>(
        &self,
        ctx: &OperationContext,
        req: Request<T>,
        duration: Duration,
    ) -> Result<Request<T>> {
        if self.skip_signature {
            return Ok(req);
        }

        let (mut parts, body) = req.into_parts();

        self.signer(ctx)
            .sign(&mut parts, Some(duration))
            .await
            .map_err(|err| new_request_sign_error(err.into()))?;

        // Always remove host header, let users' client to set it based on
        // HTTP version.
        //
        // As discussed in <https://github.com/seanmonstar/reqwest/issues/1809>,
        // google server could send RST_STREAM of PROTOCOL_ERROR if our
        // request contains host header.
        parts.headers.remove(HOST);

        Ok(Request::from_parts(parts, body))
    }

    #[inline]
    pub async fn send(
        &self,
        ctx: &OperationContext,
        req: Request<Buffer>,
    ) -> Result<Response<Buffer>> {
        ctx.http_transport().send(req).await
    }
}

impl GcsCore {
    pub fn gcs_get_object_request(
        &self,
        path: &str,
        range: BytesRange,
        args: &OpRead,
    ) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/storage/v1/b/{}/o/{}?alt=media",
            self.endpoint,
            self.bucket,
            gcs_percent_encode_path(&p)
        );

        let mut req = Request::get(&url);

        if let Some(if_match) = args.if_match() {
            req = req.header(IF_MATCH, if_match);
        }
        if let Some(if_none_match) = args.if_none_match() {
            req = req.header(IF_NONE_MATCH, if_none_match);
        }
        if !range.is_full() {
            req = req.header(http::header::RANGE, range.to_header());
        }

        let req = req
            .extension(Operation::Read)
            .extension(ServiceOperation("GetObject"));

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        Ok(req)
    }

    // It's for presign operation. Gcs only supports query sign over XML API.
    pub fn gcs_get_object_xml_request(
        &self,
        path: &str,
        range: BytesRange,
        args: &OpRead,
    ) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!("{}/{}/{}", self.endpoint, self.bucket, p);

        let mut req = Request::get(&url);

        if let Some(if_match) = args.if_match() {
            req = req.header(IF_MATCH, if_match);
        }
        if let Some(if_none_match) = args.if_none_match() {
            req = req.header(IF_NONE_MATCH, if_none_match);
        }

        if let Some(if_modified_since) = args.if_modified_since() {
            req = req.header(IF_MODIFIED_SINCE, if_modified_since.format_http_date());
        }

        if let Some(if_unmodified_since) = args.if_unmodified_since() {
            req = req.header(IF_UNMODIFIED_SINCE, if_unmodified_since.format_http_date());
        }
        if !range.is_full() {
            req = req.header(http::header::RANGE, range.to_header());
        }

        let req = req
            .extension(Operation::Read)
            .extension(ServiceOperation("GetObject"));

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        Ok(req)
    }

    pub async fn gcs_get_object(
        &self,
        ctx: &OperationContext,
        path: &str,
        range: BytesRange,
        args: &OpRead,
    ) -> Result<Response<HttpBody>> {
        let req = self.gcs_get_object_request(path, range, args)?;

        let req = self.sign(ctx, req).await?;
        ctx.http_transport().fetch(req).await
    }

    pub fn gcs_insert_object_request(
        &self,
        path: &str,
        size: Option<u64>,
        op: &OpWrite,
        body: Buffer,
    ) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let request_metadata = InsertRequestMetadata {
            storage_class: self.default_storage_class.as_deref(),
            cache_control: op.cache_control(),
            content_type: op.content_type(),
            content_encoding: op.content_encoding(),
            metadata: op.user_metadata(),
        };

        let mut url = format!(
            "{}/upload/storage/v1/b/{}/o?uploadType={}&name={}",
            self.endpoint,
            self.bucket,
            if request_metadata.is_empty() {
                "media"
            } else {
                "multipart"
            },
            gcs_percent_encode_path(&p)
        );

        if let Some(acl) = &self.predefined_acl {
            write!(&mut url, "&predefinedAcl={acl}").unwrap();
        }

        // Makes the operation conditional on whether the object's current generation
        // matches the given value. Setting to 0 makes the operation succeed only if
        // there are no live versions of the object.
        if op.if_not_exists() {
            write!(&mut url, "&ifGenerationMatch=0").unwrap();
        }

        let mut req = Request::post(&url);

        req = req.header(CONTENT_LENGTH, size.unwrap_or_default());

        if request_metadata.is_empty() {
            let req = req
                .extension(Operation::Write)
                .extension(ServiceOperation("InsertObject"));
            // If the metadata is empty, we do not set any `Content-Type` header,
            // since if we had it in the `op.content_type()`, it would be already set in the
            // `multipart` metadata body and this branch won't be executed.
            let req = req.body(body).map_err(new_request_build_error)?;
            Ok(req)
        } else {
            let mut multipart = Multipart::new();
            let metadata_part = RelatedPart::new()
                .header(
                    CONTENT_TYPE,
                    "application/json; charset=UTF-8".parse().unwrap(),
                )
                .content(
                    serde_json::to_vec(&request_metadata)
                        .expect("metadata serialization should succeed"),
                );
            multipart = multipart.part(metadata_part);

            // Content-Type must be set, even if it is set in the metadata part
            let content_type = op
                .content_type()
                .unwrap_or("application/octet-stream")
                .parse()
                .expect("Failed to parse content-type");
            let media_part = RelatedPart::new()
                .header(CONTENT_TYPE, content_type)
                .content(body);
            multipart = multipart.part(media_part);

            let req = multipart.apply(
                Request::post(url)
                    .extension(Operation::Write)
                    .extension(ServiceOperation("InsertObject")),
            )?;

            Ok(req)
        }
    }

    // It's for presign operation. Gcs only supports query sign over XML API.
    pub fn gcs_insert_object_xml_request(
        &self,
        path: &str,
        args: &OpWrite,
        body: Buffer,
    ) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!("{}/{}/{}", self.endpoint, self.bucket, p);

        let mut req = Request::put(&url);

        if let Some(user_metadata) = args.user_metadata() {
            for (key, value) in user_metadata {
                req = req.header(format!("{X_GOOG_META_PREFIX}{key}"), value)
            }
        }

        if let Some(content_type) = args.content_type() {
            req = req.header(CONTENT_TYPE, content_type);
        }

        if let Some(content_encoding) = args.content_encoding() {
            req = req.header(CONTENT_ENCODING, content_encoding);
        }

        if let Some(acl) = &self.predefined_acl {
            if let Some(predefined_acl_in_xml_spec) = predefined_acl_to_xml_header(acl) {
                req = req.header(X_GOOG_ACL, predefined_acl_in_xml_spec);
            } else {
                log::warn!("Unrecognized predefined_acl. Ignoring");
            }
        }

        if let Some(storage_class) = &self.default_storage_class {
            req = req.header(X_GOOG_STORAGE_CLASS, storage_class);
        }

        let req = req
            .extension(Operation::Write)
            .extension(ServiceOperation("InsertObject"));

        let req = req.body(body).map_err(new_request_build_error)?;

        Ok(req)
    }

    pub fn gcs_head_object_request(&self, path: &str, args: &OpStat) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/storage/v1/b/{}/o/{}",
            self.endpoint,
            self.bucket,
            gcs_percent_encode_path(&p)
        );

        let mut req = Request::get(&url);

        if let Some(if_none_match) = args.if_none_match() {
            req = req.header(IF_NONE_MATCH, if_none_match);
        }

        if let Some(if_match) = args.if_match() {
            req = req.header(IF_MATCH, if_match);
        }

        let req = req
            .extension(Operation::Stat)
            .extension(ServiceOperation("GetObject"));

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        Ok(req)
    }

    // It's for presign operation. Gcs only supports query sign over XML API.
    pub fn gcs_head_object_xml_request(
        &self,
        path: &str,
        args: &OpStat,
    ) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!("{}/{}/{}", self.endpoint, self.bucket, p);

        let mut req = Request::head(&url);

        if let Some(if_none_match) = args.if_none_match() {
            req = req.header(IF_NONE_MATCH, if_none_match);
        }

        if let Some(if_match) = args.if_match() {
            req = req.header(IF_MATCH, if_match);
        }

        let req = req
            .extension(Operation::Stat)
            .extension(ServiceOperation("GetObject"));

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        Ok(req)
    }

    pub async fn gcs_get_object_metadata(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: &OpStat,
    ) -> Result<Response<Buffer>> {
        let req = self.gcs_head_object_request(path, args)?;

        let req = self.sign(ctx, req).await?;

        self.send(ctx, req).await
    }

    pub async fn gcs_delete_object(
        &self,
        ctx: &OperationContext,
        path: &str,
    ) -> Result<Response<Buffer>> {
        let req = self.gcs_delete_object_request(path)?;

        let req = self.sign(ctx, req).await?;
        self.send(ctx, req).await
    }

    pub fn gcs_delete_object_request(&self, path: &str) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/storage/v1/b/{}/o/{}",
            self.endpoint,
            self.bucket,
            gcs_percent_encode_path(&p)
        );

        Request::delete(&url)
            .extension(Operation::Delete)
            .extension(ServiceOperation("DeleteObject"))
            .body(Buffer::new())
            .map_err(new_request_build_error)
    }

    pub async fn gcs_delete_objects(
        &self,
        ctx: &OperationContext,
        paths: Vec<String>,
    ) -> Result<Response<Buffer>> {
        let uri = format!("{}/batch/storage/v1", self.endpoint);

        let mut multipart = Multipart::new();

        for (idx, path) in paths.iter().enumerate() {
            let req = self.gcs_delete_object_request(path)?;

            multipart = multipart.part(
                MixedPart::from_request(req).part_header("content-id".parse().unwrap(), idx.into()),
            );
        }

        let req = Request::post(uri)
            .extension(Operation::Delete)
            .extension(ServiceOperation("BatchDeleteObjects"));
        let req = multipart.apply(req)?;

        let req = self.sign(ctx, req).await?;
        self.send(ctx, req).await
    }

    pub async fn gcs_rewrite_object(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: &OpCopy,
        max_bytes_rewritten_per_call: Option<usize>,
        rewrite_token: Option<&str>,
    ) -> Result<Response<Buffer>> {
        let source = build_abs_path(&self.root, from);
        let dest = build_abs_path(&self.root, to);

        let url = format!(
            "{}/storage/v1/b/{}/o/{}/rewriteTo/b/{}/o/{}",
            self.endpoint,
            self.bucket,
            gcs_percent_encode_path(&source),
            self.bucket,
            gcs_percent_encode_path(&dest)
        );

        let mut url = QueryPairsWriter::new(&url);

        if let Some(version) = args.source_version() {
            url = url.push("sourceGeneration", &gcs_percent_encode_path(version));
        }
        if args.if_not_exists() {
            url = url.push("ifGenerationMatch", "0");
        }
        if let Some(max_bytes) = max_bytes_rewritten_per_call {
            url = url.push("maxBytesRewrittenPerCall", &max_bytes.to_string());
        }
        if let Some(token) = rewrite_token {
            url = url.push("rewriteToken", &gcs_percent_encode_path(token));
        }

        let req = Request::post(url.finish())
            .header(CONTENT_LENGTH, 0)
            .extension(Operation::Copy)
            .extension(ServiceOperation("RewriteObject"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        let req = self.sign(ctx, req).await?;
        self.send(ctx, req).await
    }

    pub async fn gcs_list_objects(
        &self,
        ctx: &OperationContext,
        path: &str,
        page_token: &str,
        delimiter: &str,
        limit: Option<usize>,
        start_after: Option<String>,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!("{}/storage/v1/b/{}/o", self.endpoint, self.bucket,);

        let mut url = QueryPairsWriter::new(&url);
        url = url.push("prefix", &gcs_percent_encode_path(&p));

        if !delimiter.is_empty() {
            url = url.push("delimiter", delimiter);
        }
        if let Some(limit) = limit {
            url = url.push("maxResults", &limit.to_string());
        }
        if let Some(start_after) = start_after {
            let start_after = build_abs_path(&self.root, &start_after);
            url = url.push("startOffset", &gcs_percent_encode_path(&start_after));
        }

        if !page_token.is_empty() {
            // NOTE:
            //
            // GCS uses pageToken in request and nextPageToken in response
            //
            // Don't know how will those tokens be like so this part are copied
            // directly from AWS S3 service.
            url = url.push("pageToken", &gcs_percent_encode_path(page_token));
        }

        let req = Request::get(url.finish())
            .extension(Operation::List)
            .extension(ServiceOperation("ListObjects"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        let req = self.sign(ctx, req).await?;

        self.send(ctx, req).await
    }

    pub fn gcs_initiate_multipart_upload_request(
        &self,
        path: &str,
        op: &OpWrite,
    ) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}?uploads",
            self.endpoint,
            self.bucket,
            gcs_percent_encode_path(&p)
        );

        let mut builder = Request::post(&url)
            .header(CONTENT_LENGTH, 0)
            .extension(Operation::Write)
            .extension(ServiceOperation("CreateMultipartUpload"));

        if let Some(header_val) = op.content_disposition() {
            builder = builder.header(CONTENT_DISPOSITION, header_val);
        }

        if let Some(header_val) = op.content_encoding() {
            builder = builder.header(CONTENT_ENCODING, header_val);
        }

        if let Some(header_val) = op.content_type() {
            builder = builder.header(CONTENT_TYPE, header_val);
        }

        if let Some(header_val) = op.cache_control() {
            builder = builder.header(CACHE_CONTROL, header_val);
        }

        if let Some(metadata) = op.user_metadata() {
            for (k, v) in metadata {
                builder = builder.header(&format!("x-goog-meta-{k}"), v);
            }
        }

        if let Some(acl) = self.predefined_acl.as_ref() {
            if let Some(predefined_acl_in_xml_spec) = predefined_acl_to_xml_header(acl) {
                builder = builder.header(X_GOOG_ACL, predefined_acl_in_xml_spec);
            } else {
                log::warn!("Unrecognized predefined_acl. Ignoring");
            }
        }

        builder.body(Buffer::new()).map_err(new_request_build_error)
    }

    pub async fn gcs_initiate_multipart_upload(
        &self,
        ctx: &OperationContext,
        path: &str,
        op: &OpWrite,
    ) -> Result<Response<Buffer>> {
        let req = self.gcs_initiate_multipart_upload_request(path, op)?;
        let req = self.sign(ctx, req).await?;
        self.send(ctx, req).await
    }

    pub async fn gcs_upload_part(
        &self,
        ctx: &OperationContext,
        path: &str,
        upload_id: &str,
        part_number: usize,
        size: u64,
        body: Buffer,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}?partNumber={}&uploadId={}",
            self.endpoint,
            self.bucket,
            gcs_percent_encode_path(&p),
            part_number,
            gcs_percent_encode_path(upload_id)
        );

        let mut req = Request::put(&url);

        req = req.header(CONTENT_LENGTH, size);

        let req = req
            .extension(Operation::Write)
            .extension(ServiceOperation("UploadPart"));

        let req = req.body(body).map_err(new_request_build_error)?;

        let req = self.sign(ctx, req).await?;
        self.send(ctx, req).await
    }

    pub async fn gcs_complete_multipart_upload(
        &self,
        ctx: &OperationContext,
        path: &str,
        upload_id: &str,
        parts: Vec<CompleteMultipartUploadRequestPart>,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}?uploadId={}",
            self.endpoint,
            self.bucket,
            gcs_percent_encode_path(&p),
            gcs_percent_encode_path(upload_id)
        );

        let req = Request::post(&url);

        let content = quick_xml::se::to_string(&CompleteMultipartUploadRequest { part: parts })
            .map_err(new_xml_serialize_error)?;
        // Make sure content length has been set to avoid post with chunked encoding.
        let req = req.header(CONTENT_LENGTH, content.len());
        // Set content-type to `application/xml` to avoid mixed with form post.
        let req = req.header(CONTENT_TYPE, "application/xml");

        let req = req
            .extension(Operation::Write)
            .extension(ServiceOperation("CompleteMultipartUpload"));

        let req = req
            .body(Buffer::from(Bytes::from(content)))
            .map_err(new_request_build_error)?;

        let req = self.sign(ctx, req).await?;
        self.send(ctx, req).await
    }

    pub async fn gcs_abort_multipart_upload(
        &self,
        ctx: &OperationContext,
        path: &str,
        upload_id: &str,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}?uploadId={}",
            self.endpoint,
            self.bucket,
            gcs_percent_encode_path(&p),
            gcs_percent_encode_path(upload_id)
        );

        let req = Request::delete(&url)
            .extension(Operation::Write)
            .extension(ServiceOperation("AbortMultipartUpload"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;
        let req = self.sign(ctx, req).await?;
        self.send(ctx, req).await
    }

    pub fn build_metadata_from_object_response(path: &str, data: Buffer) -> Result<Metadata> {
        let meta: GetObjectJsonResponse =
            serde_json::from_reader(data.reader()).map_err(new_json_deserialize_error)?;

        meta.into_metadata(path)
    }
}

impl GetObjectJsonResponse {
    fn into_metadata(self, path: &str) -> Result<Metadata> {
        let mut m = Metadata::new(EntryMode::from_path(path));

        m.set_etag(&self.etag);
        m.set_content_md5(&self.md5_hash);

        let size = self
            .size
            .parse::<u64>()
            .map_err(|e| Error::new(ErrorKind::Unexpected, "parse u64").set_source(e))?;
        m.set_content_length(size);
        if !self.content_type.is_empty() {
            m.set_content_type(&self.content_type);
        }

        if !self.content_encoding.is_empty() {
            m.set_content_encoding(&self.content_encoding);
        }

        if !self.cache_control.is_empty() {
            m.set_cache_control(&self.cache_control);
        }

        if !self.content_disposition.is_empty() {
            m.set_content_disposition(&self.content_disposition);
        }

        if !self.generation.is_empty() {
            m.set_version(&self.generation);
        }

        if !self.updated.is_empty() {
            m.set_last_modified(self.updated.parse::<Timestamp>()?);
        }

        if !self.metadata.is_empty() {
            m = m.with_user_metadata(self.metadata);
        }

        Ok(m)
    }
}

// https://cloud.google.com/storage/docs/xml-api/reference-headers#xgoogacl
fn predefined_acl_to_xml_header(predefined_acl: &str) -> Option<&'static str> {
    match predefined_acl {
        "projectPrivate" => Some("project-private"),
        "private" => Some("private"),
        "bucketOwnerRead" => Some("bucket-owner-read"),
        "bucketOwnerFullControl" => Some("bucket-owner-full-control"),
        "publicRead" => Some("public-read"),
        "authenticatedRead" => Some("authenticated-read"),
        _ => None,
    }
}

#[derive(Debug, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct InsertRequestMetadata<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    content_type: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_encoding: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    storage_class: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_control: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<&'a HashMap<String, String>>,
}

impl InsertRequestMetadata<'_> {
    pub fn is_empty(&self) -> bool {
        self.content_type.is_none()
            && self.content_encoding.is_none()
            && self.storage_class.is_none()
            && self.cache_control.is_none()
            // We could also put content-encoding in the url parameters
            && self.content_encoding.is_none()
            && self.metadata.is_none()
    }
}
/// Response JSON from GCS list objects API.
///
/// refer to https://cloud.google.com/storage/docs/json_api/v1/objects/list for details
#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ListResponse {
    /// The continuation token.
    ///
    /// If this is the last page of results, then no continuation token is returned.
    pub next_page_token: Option<String>,
    /// Object name prefixes for objects that matched the listing request
    /// but were excluded from [items] because of a delimiter.
    pub prefixes: Vec<String>,
    /// The list of objects, ordered lexicographically by name.
    pub items: Vec<ListResponseItem>,
}

#[derive(Default, Debug, Eq, PartialEq, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ListResponseItem {
    pub name: String,
    pub size: String,
    // metadata
    pub etag: String,
    pub md5_hash: String,
    pub updated: String,
    pub content_type: String,
}

/// Result of CreateMultipartUpload
#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct InitiateMultipartUploadResult {
    pub upload_id: String,
}

/// Request of CompleteMultipartUploadRequest
#[derive(Default, Debug, Serialize)]
#[serde(default, rename = "CompleteMultipartUpload", rename_all = "PascalCase")]
pub struct CompleteMultipartUploadRequest {
    pub part: Vec<CompleteMultipartUploadRequestPart>,
}

#[derive(Clone, Default, Debug, Serialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct CompleteMultipartUploadRequestPart {
    #[serde(rename = "PartNumber")]
    pub part_number: usize,
    #[serde(rename = "ETag")]
    pub etag: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct RewriteResponse {
    pub total_bytes_rewritten: String,
    pub done: bool,
    pub rewrite_token: Option<String>,
    resource: Option<GetObjectJsonResponse>,
}

impl RewriteResponse {
    pub fn into_metadata(self, path: &str) -> Result<Metadata> {
        match self.resource {
            Some(resource) => resource.into_metadata(path),
            None => Ok(Metadata::default()),
        }
    }
}

/// The raw json response returned by [`get`](https://cloud.google.com/storage/docs/json_api/v1/objects/get)
#[derive(Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct GetObjectJsonResponse {
    /// GCS will return size in string.
    ///
    /// For example: `"size": "56535"`
    size: String,
    /// etag is not quoted.
    ///
    /// For example: `"etag": "CKWasoTgyPkCEAE="`
    etag: String,
    /// RFC3339 styled datetime string.
    ///
    /// For example: `"updated": "2022-08-15T11:33:34.866Z"`
    updated: String,
    /// Content md5 hash
    ///
    /// For example: `"md5Hash": "fHcEH1vPwA6eTPqxuasXcg=="`
    md5_hash: String,
    /// Content type of this object.
    ///
    /// For example: `"contentType": "image/png",`
    content_type: String,
    /// Content encoding of this object
    ///
    /// For example: "contentEncoding": "br"
    content_encoding: String,
    /// Content disposition of this object
    content_disposition: String,
    /// Cache-Control directive for the object data.
    cache_control: String,
    /// Content generation of this object. Used for object versioning and soft delete.
    generation: String,
    /// Custom metadata of this object.
    ///
    /// For example: `"metadata" : { "my-key": "my-value" }`
    metadata: HashMap<String, String>,
}

#[cfg(test)]
pub(crate) fn build_test_core(root: &str) -> GcsCore {
    use reqsign_core::Context;
    use reqsign_core::ProvideCredentialChain;
    use reqsign_core::Signer;
    use reqsign_core::StaticEnv;
    use reqsign_file_read_tokio::TokioFileRead;
    use reqsign_google::RequestSigner;

    let ctx = Context::new()
        .with_file_read(TokioFileRead)
        .with_env(StaticEnv {
            home_dir: None,
            envs: HashMap::new(),
        });
    let signer = Signer::new(
        ctx.clone(),
        ProvideCredentialChain::new(),
        RequestSigner::new("storage"),
    );

    GcsCore {
        info: ServiceInfo::new(super::GCS_SCHEME, root, "test-bucket"),
        capability: Capability::default(),
        endpoint: "https://storage.googleapis.com".to_string(),
        bucket: "test-bucket".to_string(),
        root: root.to_string(),
        signer,
        sign_ctx: ctx,
        predefined_acl: None,
        default_storage_class: None,
        skip_signature: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression test for the GCS writer double-encoding bug: the write request
    /// must percent-encode the object path exactly once, matching the read path.
    #[test]
    fn test_write_request_single_encodes_path() {
        let core = build_test_core("/");
        let path = "odfs/w/x.parquet";

        let write_req = core
            .gcs_insert_object_request(path, Some(0), &OpWrite::default(), Buffer::new())
            .expect("build write request");
        let write_uri = write_req.uri().to_string();
        assert!(
            write_uri.contains("name=odfs%2Fw%2Fx.parquet"),
            "write request must single-encode path, got: {write_uri}"
        );
        assert!(
            !write_uri.contains("%252F"),
            "write request must not double-encode path, got: {write_uri}"
        );

        let read_req = core
            .gcs_get_object_request(path, BytesRange::default(), &OpRead::default())
            .expect("build read request");
        let read_uri = read_req.uri().to_string();
        assert!(
            read_uri.contains("odfs%2Fw%2Fx.parquet") && !read_uri.contains("%252F"),
            "read request must single-encode path, got: {read_uri}"
        );
    }

    /// Multipart initiation must percent-encode the object path exactly once,
    /// including characters such as `/`, spaces and `#`.
    #[test]
    fn test_initiate_multipart_upload_single_encodes_path() {
        let core = build_test_core("/");

        let uri = core
            .gcs_initiate_multipart_upload_request("odfs/w x#1.parquet", &OpWrite::default())
            .expect("build initiate request")
            .uri()
            .to_string();

        assert!(
            uri.contains("/odfs%2Fw%20x%231.parquet?uploads"),
            "initiate request must single-encode path, got: {uri}"
        );
        assert!(
            !uri.contains("%252F"),
            "initiate request must not double-encode path, got: {uri}"
        );
    }

    #[test]
    fn test_deserialize_get_object_json_response() {
        let content = r#"{
    "kind": "storage#object",
    "id": "example/1.png/1660563214863653",
    "selfLink": "https://www.googleapis.com/storage/v1/b/example/o/1.png",
    "mediaLink": "https://content-storage.googleapis.com/download/storage/v1/b/example/o/1.png?generation=1660563214863653&alt=media",
    "name": "1.png",
    "bucket": "example",
    "generation": "1660563214863653",
    "metageneration": "1",
    "contentType": "image/png",
    "contentEncoding": "br",
    "contentDisposition": "attachment",
    "cacheControl": "public, max-age=3600",
    "storageClass": "STANDARD",
    "size": "56535",
    "md5Hash": "fHcEH1vPwA6eTPqxuasXcg==",
    "crc32c": "j/un9g==",
    "etag": "CKWasoTgyPkCEAE=",
    "timeCreated": "2022-08-15T11:33:34.866Z",
    "updated": "2022-08-15T11:33:34.866Z",
    "timeStorageClassUpdated": "2022-08-15T11:33:34.866Z",
    "metadata" : {
        "location" : "everywhere"
  }
}"#;

        let meta = GcsCore::build_metadata_from_object_response("1.png", content.into())
            .expect("parse metadata should not fail");

        assert_eq!(meta.content_length(), 56535);
        assert_eq!(
            meta.last_modified(),
            Some(
                "2022-08-15T11:33:34.866Z"
                    .parse::<Timestamp>()
                    .expect("parse date should not fail")
            )
        );
        assert_eq!(meta.content_md5(), Some("fHcEH1vPwA6eTPqxuasXcg=="));
        assert_eq!(meta.etag(), Some("CKWasoTgyPkCEAE="));
        assert_eq!(meta.content_type(), Some("image/png"));
        assert_eq!(meta.content_encoding(), Some("br"));
        assert_eq!(meta.content_disposition(), Some("attachment"));
        assert_eq!(meta.cache_control(), Some("public, max-age=3600"));
        assert_eq!(meta.version(), Some("1660563214863653"));

        let metadata = HashMap::from_iter([("location".to_string(), "everywhere".to_string())]);
        assert_eq!(meta.user_metadata(), Some(&metadata));
    }

    #[test]
    fn test_deserialize_list_response() {
        let content = r#"
    {
  "kind": "storage#objects",
  "prefixes": [
    "dir/",
    "test/"
  ],
  "items": [
    {
      "kind": "storage#object",
      "id": "example/1.png/1660563214863653",
      "selfLink": "https://www.googleapis.com/storage/v1/b/example/o/1.png",
      "mediaLink": "https://content-storage.googleapis.com/download/storage/v1/b/example/o/1.png?generation=1660563214863653&alt=media",
      "name": "1.png",
      "bucket": "example",
      "generation": "1660563214863653",
      "metageneration": "1",
      "contentType": "image/png",
      "storageClass": "STANDARD",
      "size": "56535",
      "md5Hash": "fHcEH1vPwA6eTPqxuasXcg==",
      "crc32c": "j/un9g==",
      "etag": "CKWasoTgyPkCEAE=",
      "timeCreated": "2022-08-15T11:33:34.866Z",
      "updated": "2022-08-15T11:33:34.866Z",
      "timeStorageClassUpdated": "2022-08-15T11:33:34.866Z"
    },
    {
      "kind": "storage#object",
      "id": "example/2.png/1660563214883337",
      "selfLink": "https://www.googleapis.com/storage/v1/b/example/o/2.png",
      "mediaLink": "https://content-storage.googleapis.com/download/storage/v1/b/example/o/2.png?generation=1660563214883337&alt=media",
      "name": "2.png",
      "bucket": "example",
      "generation": "1660563214883337",
      "metageneration": "1",
      "contentType": "image/png",
      "storageClass": "STANDARD",
      "size": "45506",
      "md5Hash": "e6LsGusU7pFJZk+114NV1g==",
      "crc32c": "L00QAg==",
      "etag": "CIm0s4TgyPkCEAE=",
      "timeCreated": "2022-08-15T11:33:34.886Z",
      "updated": "2022-08-15T11:33:34.886Z",
      "timeStorageClassUpdated": "2022-08-15T11:33:34.886Z"
    }
  ]
}
    "#;

        let output: ListResponse =
            serde_json::from_str(content).expect("JSON deserialize must succeed");
        assert!(output.next_page_token.is_none());
        assert_eq!(output.items.len(), 2);
        assert_eq!(output.items[0].name, "1.png");
        assert_eq!(output.items[0].size, "56535");
        assert_eq!(output.items[0].md5_hash, "fHcEH1vPwA6eTPqxuasXcg==");
        assert_eq!(output.items[0].etag, "CKWasoTgyPkCEAE=");
        assert_eq!(output.items[0].updated, "2022-08-15T11:33:34.866Z");
        assert_eq!(output.items[1].name, "2.png");
        assert_eq!(output.items[1].size, "45506");
        assert_eq!(output.items[1].md5_hash, "e6LsGusU7pFJZk+114NV1g==");
        assert_eq!(output.items[1].etag, "CIm0s4TgyPkCEAE=");
        assert_eq!(output.items[1].updated, "2022-08-15T11:33:34.886Z");
        assert_eq!(output.items[1].content_type, "image/png");
        assert_eq!(output.prefixes, vec!["dir/", "test/"])
    }

    #[test]
    fn test_deserialize_list_response_with_next_page_token() {
        let content = r#"
    {
  "kind": "storage#objects",
  "prefixes": [
    "dir/",
    "test/"
  ],
  "nextPageToken": "CgYxMC5wbmc=",
  "items": [
    {
      "kind": "storage#object",
      "id": "example/1.png/1660563214863653",
      "selfLink": "https://www.googleapis.com/storage/v1/b/example/o/1.png",
      "mediaLink": "https://content-storage.googleapis.com/download/storage/v1/b/example/o/1.png?generation=1660563214863653&alt=media",
      "name": "1.png",
      "bucket": "example",
      "generation": "1660563214863653",
      "metageneration": "1",
      "contentType": "image/png",
      "storageClass": "STANDARD",
      "size": "56535",
      "md5Hash": "fHcEH1vPwA6eTPqxuasXcg==",
      "crc32c": "j/un9g==",
      "etag": "CKWasoTgyPkCEAE=",
      "timeCreated": "2022-08-15T11:33:34.866Z",
      "updated": "2022-08-15T11:33:34.866Z",
      "timeStorageClassUpdated": "2022-08-15T11:33:34.866Z"
    },
    {
      "kind": "storage#object",
      "id": "example/2.png/1660563214883337",
      "selfLink": "https://www.googleapis.com/storage/v1/b/example/o/2.png",
      "mediaLink": "https://content-storage.googleapis.com/download/storage/v1/b/example/o/2.png?generation=1660563214883337&alt=media",
      "name": "2.png",
      "bucket": "example",
      "generation": "1660563214883337",
      "metageneration": "1",
      "contentType": "image/png",
      "storageClass": "STANDARD",
      "size": "45506",
      "md5Hash": "e6LsGusU7pFJZk+114NV1g==",
      "crc32c": "L00QAg==",
      "etag": "CIm0s4TgyPkCEAE=",
      "timeCreated": "2022-08-15T11:33:34.886Z",
      "updated": "2022-08-15T11:33:34.886Z",
      "timeStorageClassUpdated": "2022-08-15T11:33:34.886Z"
    }
  ]
}
    "#;

        let output: ListResponse =
            serde_json::from_str(content).expect("JSON deserialize must succeed");
        assert_eq!(output.next_page_token, Some("CgYxMC5wbmc=".to_string()));
        assert_eq!(output.items.len(), 2);
        assert_eq!(output.items[0].name, "1.png");
        assert_eq!(output.items[0].size, "56535");
        assert_eq!(output.items[0].md5_hash, "fHcEH1vPwA6eTPqxuasXcg==");
        assert_eq!(output.items[0].etag, "CKWasoTgyPkCEAE=");
        assert_eq!(output.items[0].updated, "2022-08-15T11:33:34.866Z");
        assert_eq!(output.items[1].name, "2.png");
        assert_eq!(output.items[1].size, "45506");
        assert_eq!(output.items[1].md5_hash, "e6LsGusU7pFJZk+114NV1g==");
        assert_eq!(output.items[1].etag, "CIm0s4TgyPkCEAE=");
        assert_eq!(output.items[1].updated, "2022-08-15T11:33:34.886Z");
        assert_eq!(output.prefixes, vec!["dir/", "test/"])
    }
}

mod error {
    use http::Response;
    use http::StatusCode;
    use serde::Deserialize;
    use serde_json::de;

    use opendal_core::raw::*;
    use opendal_core::*;

    #[derive(Default, Debug, Deserialize)]
    #[serde(default, rename_all = "camelCase")]
    struct GcsErrorResponse {
        error: GcsError,
    }

    #[derive(Default, Debug, Deserialize)]
    #[serde(default, rename_all = "camelCase")]
    struct GcsError {
        code: usize,
        message: String,
        errors: Vec<GcsErrorDetail>,
    }

    #[derive(Default, Debug, Deserialize)]
    #[serde(default, rename_all = "camelCase")]
    struct GcsErrorDetail {
        domain: String,
        location: String,
        location_type: String,
        message: String,
        reason: String,
    }

    /// Parse error response into Error.
    pub(crate) fn parse_error(resp: Response<Buffer>) -> Error {
        let (parts, body) = resp.into_parts();
        let bs = body.to_bytes();

        let (kind, retryable) = match parts.status {
            StatusCode::NOT_FOUND => (ErrorKind::NotFound, false),
            StatusCode::FORBIDDEN => (ErrorKind::PermissionDenied, false),
            StatusCode::PRECONDITION_FAILED | StatusCode::NOT_MODIFIED => {
                (ErrorKind::ConditionNotMatch, false)
            }
            StatusCode::TOO_MANY_REQUESTS => (ErrorKind::RateLimited, true),
            StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT => (ErrorKind::Unexpected, true),
            _ => (ErrorKind::Unexpected, false),
        };

        let message = match de::from_slice::<GcsErrorResponse>(&bs) {
            Ok(gcs_err) => format!("{gcs_err:?}"),
            Err(_) => String::from_utf8_lossy(&bs).into_owned(),
        };

        let mut err = Error::new(kind, message);

        err = with_error_response_context(err, parts);

        if retryable {
            err = err.set_temporary();
        }

        err
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_parse_error() {
            let bs = bytes::Bytes::from(
                r#"
{
"error": {
 "errors": [
  {
   "domain": "global",
   "reason": "required",
   "message": "Login Required",
   "locationType": "header",
   "location": "Authorization"
  }
 ],
 "code": 401,
 "message": "Login Required"
 }
}
"#,
            );

            let out: GcsErrorResponse = de::from_slice(&bs).expect("must success");
            println!("{out:?}");

            assert_eq!(out.error.code, 401);
            assert_eq!(out.error.message, "Login Required");
            assert_eq!(out.error.errors[0].domain, "global");
            assert_eq!(out.error.errors[0].reason, "required");
            assert_eq!(out.error.errors[0].message, "Login Required");
            assert_eq!(out.error.errors[0].location_type, "header");
            assert_eq!(out.error.errors[0].location, "Authorization");
        }
    }
}

pub(super) use error::*;

mod uri {
    use percent_encoding::AsciiSet;
    use percent_encoding::NON_ALPHANUMERIC;
    use percent_encoding::utf8_percent_encode;

    /// PATH_ENCODE_SET is the encode set for http url path.
    ///
    /// This set follows [encodeURIComponent](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/encodeURIComponent) which will encode all non-ASCII characters except `A-Z a-z 0-9 - _ . ! ~ * ' ( )`
    ///
    /// Following characters is allowed in GCS, check "https://cloud.google.com/storage/docs/request-endpoints#encoding" for details
    static GCS_PATH_ENCODE_SET: AsciiSet = NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'_')
        .remove(b'.')
        .remove(b'*');

    /// gcs_percent_encode_path will do percent encoding for http encode path.
    ///
    /// Follows [encodeURIComponent](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/encodeURIComponent)
    /// which will encode all non-ASCII characters except `A-Z a-z 0-9 - _ . *`
    ///
    /// GCS does not allow '/'s in paths, this should also be dealt with
    pub(crate) fn gcs_percent_encode_path(path: &str) -> String {
        utf8_percent_encode(path, &GCS_PATH_ENCODE_SET).to_string()
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_percent_encode_path() {
            let cases = vec![
                (
                    "Reserved Characters",
                    ";,/?:@&=+$",
                    "%3B%2C%2F%3F%3A%40%26%3D%2B%24",
                ),
                ("Unescaped Characters", "-_.*", "-_.*"),
                ("Number Sign", "#", "%23"),
                (
                    "Alphanumeric Characters + Space",
                    "ABC abc 123",
                    "ABC%20abc%20123",
                ),
                (
                    "Unicode",
                    "你好，世界！❤",
                    "%E4%BD%A0%E5%A5%BD%EF%BC%8C%E4%B8%96%E7%95%8C%EF%BC%81%E2%9D%A4",
                ),
            ];

            for (name, input, expected) in cases {
                let actual = gcs_percent_encode_path(input);

                assert_eq!(actual, expected, "{name}");
            }
        }
    }
}

pub(super) use uri::*;
