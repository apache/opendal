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
use http::header::CACHE_CONTROL;
use http::header::CONTENT_DISPOSITION;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::header::IF_MATCH;
use http::header::IF_NONE_MATCH;
use http::Request;
use http::Response;
use reqsign::TencentCosCredential;
use reqsign::TencentCosCredentialLoader;
use reqsign::TencentCosSigner;
use serde::Deserialize;
use serde::Serialize;

use crate::raw::*;
use crate::*;

pub mod constants {
    pub const COS_QUERY_VERSION_ID: &str = "versionId";

    pub const X_COS_VERSION_ID: &str = "x-cos-version-id";
}

pub struct CosCore {
    pub info: Arc<AccessorInfo>,
    pub bucket: String,
    pub root: String,
    pub endpoint: String,

    pub signer: TencentCosSigner,
    pub loader: TencentCosCredentialLoader,
}

impl Debug for CosCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend")
            .field("root", &self.root)
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl CosCore {
    async fn load_credential(&self) -> Result<Option<TencentCosCredential>> {
        let cred = self
            .loader
            .load()
            .await
            .map_err(new_request_credential_error)?;

        if let Some(cred) = cred {
            return Ok(Some(cred));
        }

        Err(Error::new(
            ErrorKind::PermissionDenied,
            "no valid credential found and anonymous access is not allowed",
        ))
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
}

impl CosCore {
    pub async fn cos_get_object(
        &self,
        path: &str,
        range: BytesRange,
        args: &OpRead,
    ) -> Result<Response<HttpBody>> {
        let mut req = self.cos_get_object_request(path, range, args)?;

        self.sign(&mut req).await?;

        self.info.http_client().fetch(req).await
    }

    pub fn cos_get_object_request(
        &self,
        path: &str,
        range: BytesRange,
        args: &OpRead,
    ) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let mut url = format!("{}/{}", self.endpoint, percent_encode_path(&p));

        let mut query_args = Vec::new();
        if let Some(version) = args.version() {
            query_args.push(format!(
                "{}={}",
                constants::COS_QUERY_VERSION_ID,
                percent_decode_path(version)
            ))
        }
        if !query_args.is_empty() {
            url.push_str(&format!("?{}", query_args.join("&")));
        }

        let mut req = Request::get(&url);

        if let Some(if_match) = args.if_match() {
            req = req.header(IF_MATCH, if_match);
        }

        if !range.is_full() {
            req = req.header(http::header::RANGE, range.to_header())
        }

        if let Some(if_none_match) = args.if_none_match() {
            req = req.header(IF_NONE_MATCH, if_none_match);
        }

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        Ok(req)
    }

    pub fn cos_put_object_request(
        &self,
        path: &str,
        size: Option<u64>,
        args: &OpWrite,
        body: Buffer,
    ) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!("{}/{}", self.endpoint, percent_encode_path(&p));

        let mut req = Request::put(&url);

        if let Some(size) = size {
            req = req.header(CONTENT_LENGTH, size)
        }
        if let Some(cache_control) = args.cache_control() {
            req = req.header(CACHE_CONTROL, cache_control)
        }
        if let Some(pos) = args.content_disposition() {
            req = req.header(CONTENT_DISPOSITION, pos)
        }
        if let Some(mime) = args.content_type() {
            req = req.header(CONTENT_TYPE, mime)
        }

        // For a bucket which has never enabled versioning, you may use it to
        // specify whether to prohibit overwriting the object with the same name
        // when uploading the object:
        //
        // When the x-cos-forbid-overwrite is specified as true, overwriting the object
        // with the same name will be prohibited.
        //
        // ref: https://www.tencentcloud.com/document/product/436/7749
        if args.if_not_exists() {
            req = req.header("x-cos-forbid-overwrite", "true")
        }

        // Set user metadata headers.
        if let Some(user_metadata) = args.user_metadata() {
            for (key, value) in user_metadata {
                req = req.header(format!("x-cos-meta-{key}"), value)
            }
        }

        let req = req.body(body).map_err(new_request_build_error)?;

        Ok(req)
    }

    pub async fn cos_head_object(&self, path: &str, args: &OpStat) -> Result<Response<Buffer>> {
        let mut req = self.cos_head_object_request(path, args)?;

        self.sign(&mut req).await?;

        self.send(req).await
    }

    pub fn cos_head_object_request(&self, path: &str, args: &OpStat) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let mut url = format!("{}/{}", self.endpoint, percent_encode_path(&p));

        let mut query_args = Vec::new();
        if let Some(version) = args.version() {
            query_args.push(format!(
                "{}={}",
                constants::COS_QUERY_VERSION_ID,
                percent_decode_path(version)
            ))
        }
        if !query_args.is_empty() {
            url.push_str(&format!("?{}", query_args.join("&")));
        }

        let mut req = Request::head(&url);

        if let Some(if_match) = args.if_match() {
            req = req.header(IF_MATCH, if_match);
        }

        if let Some(if_none_match) = args.if_none_match() {
            req = req.header(IF_NONE_MATCH, if_none_match);
        }

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        Ok(req)
    }

    pub async fn cos_delete_object(&self, path: &str, args: &OpDelete) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let mut url = format!("{}/{}", self.endpoint, percent_encode_path(&p));

        let mut query_args = Vec::new();
        if let Some(version) = args.version() {
            query_args.push(format!(
                "{}={}",
                constants::COS_QUERY_VERSION_ID,
                percent_decode_path(version)
            ))
        }
        if !query_args.is_empty() {
            url.push_str(&format!("?{}", query_args.join("&")));
        }

        let req = Request::delete(&url);

        let mut req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.sign(&mut req).await?;

        self.send(req).await
    }

    pub fn cos_append_object_request(
        &self,
        path: &str,
        position: u64,
        size: u64,
        args: &OpWrite,
        body: Buffer,
    ) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);
        let url = format!(
            "{}/{}?append&position={}",
            self.endpoint,
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

        let req = req.body(body).map_err(new_request_build_error)?;
        Ok(req)
    }

    pub async fn cos_copy_object(&self, from: &str, to: &str) -> Result<Response<Buffer>> {
        let source = build_abs_path(&self.root, from);
        let target = build_abs_path(&self.root, to);

        let source = format!("/{}/{}", self.bucket, percent_encode_path(&source));
        let url = format!("{}/{}", self.endpoint, percent_encode_path(&target));

        let mut req = Request::put(&url)
            .header("x-cos-copy-source", &source)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;

        self.send(req).await
    }

    pub async fn cos_list_objects(
        &self,
        path: &str,
        next_marker: &str,
        delimiter: &str,
        limit: Option<usize>,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let mut queries = vec![];
        if !p.is_empty() {
            queries.push(format!("prefix={}", percent_encode_path(&p)));
        }
        if !delimiter.is_empty() {
            queries.push(format!("delimiter={delimiter}"));
        }
        if let Some(limit) = limit {
            queries.push(format!("max-keys={limit}"));
        }
        if !next_marker.is_empty() {
            queries.push(format!("marker={next_marker}"));
        }

        let url = if queries.is_empty() {
            self.endpoint.to_string()
        } else {
            format!("{}?{}", self.endpoint, queries.join("&"))
        };

        let mut req = Request::get(&url)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;

        self.send(req).await
    }

    pub async fn cos_initiate_multipart_upload(
        &self,
        path: &str,
        args: &OpWrite,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!("{}/{}?uploads", self.endpoint, percent_encode_path(&p));

        let mut req = Request::post(&url);

        if let Some(mime) = args.content_type() {
            req = req.header(CONTENT_TYPE, mime)
        }

        if let Some(content_disposition) = args.content_disposition() {
            req = req.header(CONTENT_DISPOSITION, content_disposition)
        }

        if let Some(cache_control) = args.cache_control() {
            req = req.header(CACHE_CONTROL, cache_control)
        }

        // Set user metadata headers.
        if let Some(user_metadata) = args.user_metadata() {
            for (key, value) in user_metadata {
                req = req.header(format!("x-cos-meta-{key}"), value)
            }
        }

        let mut req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.sign(&mut req).await?;

        self.send(req).await
    }

    pub async fn cos_upload_part_request(
        &self,
        path: &str,
        upload_id: &str,
        part_number: usize,
        size: u64,
        body: Buffer,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}?partNumber={}&uploadId={}",
            self.endpoint,
            percent_encode_path(&p),
            part_number,
            percent_encode_path(upload_id)
        );

        let mut req = Request::put(&url);
        req = req.header(CONTENT_LENGTH, size);
        // Set body
        let mut req = req.body(body).map_err(new_request_build_error)?;

        self.sign(&mut req).await?;

        self.send(req).await
    }

    pub async fn cos_complete_multipart_upload(
        &self,
        path: &str,
        upload_id: &str,
        parts: Vec<CompleteMultipartUploadRequestPart>,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}?uploadId={}",
            self.endpoint,
            percent_encode_path(&p),
            percent_encode_path(upload_id)
        );

        let req = Request::post(&url);

        let content = quick_xml::se::to_string(&CompleteMultipartUploadRequest { part: parts })
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

    /// Abort an on-going multipart upload.
    pub async fn cos_abort_multipart_upload(
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

    pub async fn cos_list_object_versions(
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
    /// # TODO
    ///
    /// quick-xml will do escape on `"` which leads to our serialized output is
    /// not the same as aws s3's example.
    ///
    /// Ideally, we could use `serialize_with` to address this (buf failed)
    ///
    /// ```ignore
    /// #[derive(Default, Debug, Serialize)]
    /// #[serde(default, rename_all = "PascalCase")]
    /// struct CompleteMultipartUploadRequestPart {
    ///     #[serde(rename = "PartNumber")]
    ///     part_number: usize,
    ///     #[serde(rename = "ETag", serialize_with = "partial_escape")]
    ///     etag: String,
    /// }
    ///
    /// fn partial_escape<S>(s: &str, ser: S) -> Result<S::Ok, S::Error>
    /// where
    ///     S: serde::Serializer,
    /// {
    ///     ser.serialize_str(&String::from_utf8_lossy(
    ///         &quick_xml::escape::partial_escape(s.as_bytes()),
    ///     ))
    /// }
    /// ```
    ///
    /// ref: <https://github.com/tafia/quick-xml/issues/362>
    #[serde(rename = "ETag")]
    pub etag: String,
}

/// Output of `CompleteMultipartUpload` operation
#[derive(Debug, Default, Deserialize)]
#[serde[default, rename_all = "PascalCase"]]
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
    pub name: String,
    pub prefix: String,
    pub contents: Vec<ListObjectsOutputContent>,
    pub common_prefixes: Vec<CommonPrefix>,
    pub marker: String,
    pub next_marker: Option<String>,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct CommonPrefix {
    pub prefix: String,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct ListObjectsOutputContent {
    pub key: String,
    pub size: u64,
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

    use super::*;

    #[test]
    fn test_parse_xml() {
        let bs = bytes::Bytes::from(
            r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ListBucketResult>
    <Name>examplebucket</Name>
    <Prefix>obj</Prefix>
    <Marker>obj002</Marker>
    <NextMarker>obj004</NextMarker>
    <MaxKeys>1000</MaxKeys>
    <IsTruncated>false</IsTruncated>
    <Contents>
        <Key>obj002</Key>
        <LastModified>2015-07-01T02:11:19.775Z</LastModified>
        <ETag>"a72e382246ac83e86bd203389849e71d"</ETag>
        <Size>9</Size>
        <Owner>
            <ID>b4bf1b36d9ca43d984fbcb9491b6fce9</ID>
        </Owner>
        <StorageClass>STANDARD</StorageClass>
    </Contents>
    <Contents>
        <Key>obj003</Key>
        <LastModified>2015-07-01T02:11:19.775Z</LastModified>
        <ETag>"a72e382246ac83e86bd203389849e71d"</ETag>
        <Size>10</Size>
        <Owner>
            <ID>b4bf1b36d9ca43d984fbcb9491b6fce9</ID>
        </Owner>
        <StorageClass>STANDARD</StorageClass>
    </Contents>
    <CommonPrefixes>
        <Prefix>hello</Prefix>
    </CommonPrefixes>
    <CommonPrefixes>
        <Prefix>world</Prefix>
    </CommonPrefixes>
</ListBucketResult>"#,
        );
        let out: ListObjectsOutput = quick_xml::de::from_reader(bs.reader()).expect("must success");

        assert_eq!(out.name, "examplebucket".to_string());
        assert_eq!(out.prefix, "obj".to_string());
        assert_eq!(out.marker, "obj002".to_string());
        assert_eq!(out.next_marker, Some("obj004".to_string()),);
        assert_eq!(
            out.contents
                .iter()
                .map(|v| v.key.clone())
                .collect::<Vec<String>>(),
            ["obj002", "obj003"],
        );
        assert_eq!(
            out.contents.iter().map(|v| v.size).collect::<Vec<u64>>(),
            [9, 10],
        );
        assert_eq!(
            out.common_prefixes
                .iter()
                .map(|v| v.prefix.clone())
                .collect::<Vec<String>>(),
            ["hello", "world"],
        )
    }
}
