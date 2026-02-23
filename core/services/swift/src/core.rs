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
use http::header;
use http::header::IF_MATCH;
use http::header::IF_MODIFIED_SINCE;
use http::header::IF_NONE_MATCH;
use http::header::IF_UNMODIFIED_SINCE;
use serde::Deserialize;
use serde::Serialize;

use opendal_core::raw::*;
use opendal_core::*;

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

        let req = req
            .extension(Operation::Delete)
            .body(body)
            .map_err(new_request_build_error)?;

        self.info.http_client().send(req).await
    }

    /// Bulk delete multiple objects in a single request.
    ///
    /// Reference: <https://docs.openstack.org/api-ref/object-store/#bulk-delete>
    pub async fn swift_bulk_delete(
        &self,
        paths: &[(String, OpDelete)],
    ) -> Result<Response<Buffer>> {
        // The bulk-delete endpoint is on the account URL (the endpoint itself).
        let url = format!("{}?bulk-delete", &self.endpoint);

        let mut req = Request::post(&url);

        req = req.header("X-Auth-Token", &self.token);
        req = req.header(header::CONTENT_TYPE, "text/plain");
        req = req.header(header::ACCEPT, "application/json");

        // Body is newline-separated list of URL-encoded paths:
        // /{container}/{object_path}
        let body_str: String = paths
            .iter()
            .map(|(path, _)| {
                let abs = build_abs_path(&self.root, path);
                format!("{}/{}", &self.container, percent_encode_path(&abs))
            })
            .collect::<Vec<_>>()
            .join("\n");

        req = req.header(header::CONTENT_LENGTH, body_str.len());

        let req = req
            .extension(Operation::Delete)
            .body(Buffer::from(bytes::Bytes::from(body_str)))
            .map_err(new_request_build_error)?;

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
        let mut url = QueryPairsWriter::new(&format!("{}/{}/", &self.endpoint, &self.container,))
            .push("prefix", &percent_encode_path(&p))
            .push("delimiter", delimiter)
            .push("format", "json");

        if let Some(limit) = limit {
            url = url.push("limit", &limit.to_string());
        }
        if !marker.is_empty() {
            url = url.push("marker", marker);
        }

        let mut req = Request::get(url.finish());

        req = req.header("X-Auth-Token", &self.token);

        let req = req
            .extension(Operation::List)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

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

        if let Some(content_type) = args.content_type() {
            req = req.header(header::CONTENT_TYPE, content_type);
        }
        if let Some(content_disposition) = args.content_disposition() {
            req = req.header(header::CONTENT_DISPOSITION, content_disposition);
        }
        if let Some(content_encoding) = args.content_encoding() {
            req = req.header(header::CONTENT_ENCODING, content_encoding);
        }
        if let Some(cache_control) = args.cache_control() {
            req = req.header(header::CACHE_CONTROL, cache_control);
        }

        // Set user metadata headers.
        if let Some(user_metadata) = args.user_metadata() {
            for (k, v) in user_metadata {
                req = req.header(format!("X-Object-Meta-{k}"), v);
            }
        }

        req = req.header("X-Auth-Token", &self.token);
        req = req.header(header::CONTENT_LENGTH, length);

        let req = req
            .extension(Operation::Write)
            .body(body)
            .map_err(new_request_build_error)?;

        self.info.http_client().send(req).await
    }

    pub async fn swift_read(
        &self,
        path: &str,
        range: BytesRange,
        args: &OpRead,
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

        let req = req
            .extension(Operation::Read)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

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

        let req = req
            .extension(Operation::Copy)
            .body(body)
            .map_err(new_request_build_error)?;

        self.info.http_client().send(req).await
    }

    pub async fn swift_get_metadata(&self, path: &str, args: &OpStat) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}",
            &self.endpoint,
            &self.container,
            percent_encode_path(&p)
        );

        let mut req = Request::head(&url);

        req = req.header("X-Auth-Token", &self.token);

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

        let req = req
            .extension(Operation::Stat)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.info.http_client().send(req).await
    }

    /// Build the segment path for an SLO part.
    ///
    /// Segments are stored as: `.segments/{object_path}/{upload_id}/{part_number:08}`
    pub fn slo_segment_path(&self, path: &str, upload_id: &str, part_number: usize) -> String {
        let abs = build_abs_path(&self.root, path);
        format!(
            ".segments/{}{}/{:08}",
            abs.trim_end_matches('/'),
            upload_id,
            part_number
        )
    }

    /// Upload a segment for an SLO multipart upload.
    ///
    /// Reference: <https://docs.openstack.org/swift/latest/overview_large_objects.html>
    pub async fn swift_put_segment(
        &self,
        path: &str,
        upload_id: &str,
        part_number: usize,
        size: u64,
        body: Buffer,
    ) -> Result<Response<Buffer>> {
        let segment = self.slo_segment_path(path, upload_id, part_number);
        let url = format!(
            "{}/{}/{}",
            &self.endpoint,
            &self.container,
            percent_encode_path(&segment)
        );

        let mut req = Request::put(&url);
        req = req.header("X-Auth-Token", &self.token);
        req = req.header(header::CONTENT_LENGTH, size);

        let req = req
            .extension(Operation::Write)
            .body(body)
            .map_err(new_request_build_error)?;

        self.info.http_client().send(req).await
    }

    /// Finalize an SLO by uploading the manifest.
    ///
    /// PUT {container}/{path}?multipart-manifest=put with a JSON body listing
    /// each segment's path, etag, and size.
    ///
    /// Reference: <https://docs.openstack.org/swift/latest/overview_large_objects.html>
    pub async fn swift_put_slo_manifest(
        &self,
        path: &str,
        manifest: &[SloManifestEntry],
        args: &OpWrite,
    ) -> Result<Response<Buffer>> {
        let abs = build_abs_path(&self.root, path);
        let url = format!(
            "{}/{}/{}?multipart-manifest=put",
            &self.endpoint,
            &self.container,
            percent_encode_path(&abs)
        );

        let body = serde_json::to_vec(manifest).map_err(new_json_serialize_error)?;

        let mut req = Request::put(&url);
        req = req.header("X-Auth-Token", &self.token);
        req = req.header(header::CONTENT_LENGTH, body.len());
        req = req.header(header::CONTENT_TYPE, "application/json");

        // Forward user metadata to the manifest object.
        if let Some(user_metadata) = args.user_metadata() {
            for (k, v) in user_metadata {
                req = req.header(format!("X-Object-Meta-{k}"), v);
            }
        }

        let req = req
            .extension(Operation::Write)
            .body(Buffer::from(bytes::Bytes::from(body)))
            .map_err(new_request_build_error)?;

        self.info.http_client().send(req).await
    }

    /// Delete an SLO manifest and all its segments.
    ///
    /// DELETE {container}/{path}?multipart-manifest=delete removes the manifest
    /// and all referenced segments in one call.
    ///
    /// Reference: <https://docs.openstack.org/swift/latest/overview_large_objects.html>
    pub async fn swift_delete_slo(&self, path: &str, upload_id: &str) -> Result<()> {
        // List segments under the upload_id prefix and delete them individually.
        // We can't use multipart-manifest=delete because we haven't created
        // the manifest yet (abort happens before complete).
        let abs = build_abs_path(&self.root, path);
        let prefix = format!(".segments/{}{}/", abs.trim_end_matches('/'), upload_id);

        // List all segments with this prefix.
        let url = QueryPairsWriter::new(&format!("{}/{}/", &self.endpoint, &self.container))
            .push("prefix", &percent_encode_path(&prefix))
            .push("format", "json")
            .finish();

        let mut req = Request::get(&url);
        req = req.header("X-Auth-Token", &self.token);

        let req = req
            .extension(Operation::List)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        let resp = self.info.http_client().send(req).await?;
        if !resp.status().is_success() {
            return Ok(());
        }

        let bs = resp.into_body().to_bytes();
        let segments: Vec<ListOpResponse> = serde_json::from_slice(&bs).unwrap_or_default();

        // Delete each segment.
        for seg in segments {
            if let ListOpResponse::FileInfo { name, .. } = seg {
                let seg_url = format!(
                    "{}/{}/{}",
                    &self.endpoint,
                    &self.container,
                    percent_encode_path(&name)
                );

                let mut req = Request::delete(&seg_url);
                req = req.header("X-Auth-Token", &self.token);

                let req = req
                    .extension(Operation::Delete)
                    .body(Buffer::new())
                    .map_err(new_request_build_error)?;

                // Best effort — ignore individual segment delete failures.
                let _ = self.info.http_client().send(req).await;
            }
        }

        Ok(())
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

/// Response from Swift bulk-delete API.
///
/// Reference: <https://docs.openstack.org/api-ref/object-store/#bulk-delete>
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
#[allow(dead_code)]
pub struct BulkDeleteResponse {
    /// Number of objects successfully deleted.
    #[serde(rename = "Number Deleted")]
    pub number_deleted: i64,
    /// Number of objects not found (treated as success).
    #[serde(rename = "Number Not Found")]
    pub number_not_found: i64,
    /// Response status string, e.g. "200 OK".
    #[serde(rename = "Response Status")]
    pub response_status: String,
    /// Per-object errors as [path, status_string] pairs.
    #[serde(rename = "Errors", default)]
    pub errors: Vec<Vec<String>>,
    /// Response body (usually empty on success).
    #[serde(rename = "Response Body")]
    pub response_body: Option<String>,
}

/// Entry in an SLO manifest JSON array.
///
/// Reference: <https://docs.openstack.org/swift/latest/overview_large_objects.html>
#[derive(Debug, Serialize)]
pub struct SloManifestEntry {
    /// Path to the segment: `/{container}/{segment_name}`
    pub path: String,
    /// MD5 etag of the segment (without quotes).
    pub etag: String,
    /// Size of the segment in bytes.
    pub size_bytes: u64,
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

    #[test]
    fn parse_bulk_delete_response_test() -> Result<()> {
        let resp = bytes::Bytes::from(
            r#"{
                "Number Deleted": 2,
                "Number Not Found": 1,
                "Response Status": "200 OK",
                "Errors": [],
                "Response Body": ""
            }"#,
        );

        let result: BulkDeleteResponse =
            serde_json::from_slice(&resp).map_err(new_json_deserialize_error)?;

        assert_eq!(result.number_deleted, 2);
        assert_eq!(result.number_not_found, 1);
        assert_eq!(result.response_status, "200 OK");
        assert!(result.errors.is_empty());

        Ok(())
    }

    #[test]
    fn parse_bulk_delete_response_with_errors_test() -> Result<()> {
        let resp = bytes::Bytes::from(
            r#"{
                "Number Deleted": 1,
                "Number Not Found": 0,
                "Response Status": "400 Bad Request",
                "Errors": [
                    ["/container/path/to/file", "403 Forbidden"]
                ],
                "Response Body": ""
            }"#,
        );

        let result: BulkDeleteResponse =
            serde_json::from_slice(&resp).map_err(new_json_deserialize_error)?;

        assert_eq!(result.number_deleted, 1);
        assert_eq!(result.errors.len(), 1);
        assert_eq!(result.errors[0][0], "/container/path/to/file");
        assert_eq!(result.errors[0][1], "403 Forbidden");

        Ok(())
    }
}
