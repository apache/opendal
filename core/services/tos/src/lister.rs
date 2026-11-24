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

use std::sync::Arc;

use bytes::Buf;

use crate::core::parse_error;
use crate::core::*;
use opendal_core::MetadataBuilder;
use opendal_core::OperationContext;
use opendal_core::Result;
use opendal_core::raw::*;

pub type TosListers = TwoWays<oio::PageLister<TosLister>, oio::PageLister<TosObjectVersionsLister>>;

pub struct TosLister {
    core: Arc<TosCore>,
    ctx: OperationContext,

    path: String,
    args: OpList,

    delimiter: &'static str,
    start_after: Option<String>,
}

impl TosLister {
    pub fn new(core: Arc<TosCore>, ctx: OperationContext, path: &str, args: OpList) -> Self {
        let delimiter = if args.recursive() { "" } else { "/" };
        let start_after = args.start_after().map(ToString::to_string);

        Self {
            core,
            ctx,

            path: path.to_string(),
            args,

            delimiter,
            start_after,
        }
    }
}

impl oio::PageList for TosLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let resp = self
            .core
            .tos_list_objects_v2(
                &self.ctx,
                &self.path,
                &ctx.token,
                self.delimiter,
                self.args.limit(),
                if ctx.token.is_empty() {
                    self.start_after.clone()
                } else {
                    None
                },
            )
            .await?;

        if resp.status() != http::StatusCode::OK {
            return Err(parse_error(
                ErrorContext::new(ServiceOperation("ListObjectsV2")),
                resp,
            ));
        }

        let bs = resp.into_body();
        let output: ListObjectsOutputV2 =
            serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

        if output.is_truncated
            && let Some(token) = output.next_continuation_token.filter(|v| !v.is_empty())
        {
            ctx.token = token;
        } else {
            ctx.done = true;
            ctx.token.clear();
        }

        for prefix in output.common_prefixes {
            let de = oio::Entry::new(
                &build_relative_path(&self.core.root, &prefix.prefix),
                MetadataBuilder::dir().build(),
            );

            ctx.entries.push_back(de);
        }

        for object in output.contents {
            let mut path = build_relative_path(&self.core.root, &object.key);
            if path.is_empty() {
                path = "/".to_string();
            }

            let mut meta = if path.ends_with('/') {
                MetadataBuilder::dir()
            } else {
                MetadataBuilder::file(object.size)
            };
            meta.is_current(Some(true));
            if let Some(etag) = object
                .etag
                .as_deref()
                .map(|v| v.trim_matches('"'))
                .filter(|v| !v.is_empty())
            {
                meta.etag(etag);
            }
            meta.last_modified(object.last_modified.parse::<Timestamp>()?);

            let de = oio::Entry::with(path, meta.build());
            ctx.entries.push_back(de);
        }

        Ok(())
    }
}

pub struct TosObjectVersionsLister {
    core: Arc<TosCore>,
    ctx: OperationContext,

    prefix: String,
    args: OpList,

    delimiter: &'static str,
    abs_start_after: Option<String>,
}

impl TosObjectVersionsLister {
    pub fn new(core: Arc<TosCore>, ctx: OperationContext, path: &str, args: OpList) -> Self {
        let delimiter = if args.recursive() { "" } else { "/" };
        let abs_start_after = args
            .start_after()
            .map(|start_after| build_absolute_path(&core.root, start_after));

        Self {
            core,
            ctx,
            prefix: path.to_string(),
            args,
            delimiter,
            abs_start_after,
        }
    }
}

impl oio::PageList for TosObjectVersionsLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let markers = ctx.token.split_once('\0');
        let (key_marker, version_id_marker) = if let Some(data) = markers {
            data
        } else if let Some(start_after) = &self.abs_start_after {
            (start_after.as_str(), "")
        } else {
            ("", "")
        };

        let resp = self
            .core
            .tos_list_object_versions(
                &self.ctx,
                &self.prefix,
                self.delimiter,
                self.args.limit(),
                key_marker,
                version_id_marker,
            )
            .await?;

        if resp.status() != http::StatusCode::OK {
            return Err(parse_error(
                ErrorContext::new(ServiceOperation("ListObjectVersions")),
                resp,
            ));
        }

        let bs = resp.into_body();
        let output: ListObjectVersionsOutput =
            serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

        if output.is_truncated == Some(true)
            && let Some(next_key_marker) = output.next_key_marker.filter(|v| !v.is_empty())
        {
            let next_version_id_marker = output.next_version_id_marker.unwrap_or_default();
            ctx.token = format!("{next_key_marker}\0{next_version_id_marker}");
        } else {
            ctx.done = true;
            ctx.token.clear();
        }

        for prefix in output.common_prefixes {
            let de = oio::Entry::new(
                &build_relative_path(&self.core.root, &prefix.prefix),
                MetadataBuilder::dir().build(),
            );

            ctx.entries.push_back(de);
        }

        for version_object in output.versions {
            if !(self.args.versions() || version_object.is_latest) {
                continue;
            }

            let mut path = build_relative_path(&self.core.root, &version_object.key);
            if path.is_empty() {
                path = "/".to_string();
            }

            let mut meta = if path.ends_with('/') {
                MetadataBuilder::dir()
            } else {
                MetadataBuilder::file(version_object.size)
            };
            meta.version(&version_object.version_id);
            meta.is_current(Some(version_object.is_latest));
            meta.last_modified(version_object.last_modified.parse::<Timestamp>()?);

            if let Some(etag) = version_object
                .etag
                .as_deref()
                .map(|v| v.trim_matches('"'))
                .filter(|v| !v.is_empty())
            {
                meta.etag(etag);
            }

            let de = oio::Entry::with(path, meta.build());
            ctx.entries.push_back(de);
        }

        if self.args.deleted() {
            for delete_marker in output.delete_markers {
                let mut path = build_relative_path(&self.core.root, &delete_marker.key);
                if path.is_empty() {
                    path = "/".to_string();
                }

                let mut meta = if path.ends_with('/') {
                    MetadataBuilder::dir()
                } else {
                    MetadataBuilder::file(0)
                };
                meta.version(&delete_marker.version_id);
                meta.is_deleted(true);
                meta.is_current(Some(delete_marker.is_latest));
                meta.last_modified(delete_marker.last_modified.parse::<Timestamp>()?);

                let de = oio::Entry::with(path, meta.build());
                ctx.entries.push_back(de);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use futures::stream;
    use http::{Request, Response, StatusCode};
    use opendal_core::raw::oio::{List, PageLister};
    use opendal_core::{
        Buffer, Builder, Error, ErrorKind, HttpBody, HttpTransport, HttpTransporter, options,
    };

    use super::*;

    #[derive(Clone)]
    struct ScriptedTransport {
        responses: Arc<Mutex<VecDeque<String>>>,
        requests: Arc<Mutex<Vec<String>>>,
    }

    impl ScriptedTransport {
        fn new(responses: Vec<&str>) -> Self {
            Self {
                responses: Arc::new(Mutex::new(
                    responses.into_iter().map(str::to_string).collect(),
                )),
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl HttpTransport for ScriptedTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            self.requests.lock().unwrap().push(req.uri().to_string());
            let body =
                self.responses.lock().unwrap().pop_front().ok_or_else(|| {
                    Error::new(ErrorKind::Unexpected, "unexpected extra list call")
                })?;
            let len = body.len() as u64;
            Response::builder()
                .status(StatusCode::OK)
                .body(HttpBody::new(
                    stream::iter([Ok(Buffer::from(body))]),
                    Some(len),
                ))
                .map_err(new_request_build_error)
        }
    }

    fn build_core() -> Arc<TosCore> {
        let backend = crate::Tos::default()
            .bucket("test-bucket")
            .skip_signature()
            .build()
            .expect("build must succeed");
        let backend = (&backend as &dyn std::any::Any)
            .downcast_ref::<crate::backend::TosBackend>()
            .expect("build must return TosBackend");
        backend.core.clone()
    }

    #[tokio::test]
    async fn test_lister_terminates_when_truncated_without_valid_token() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let core = build_core();
            let missing_token = r#"{
                "IsTruncated": true,
                "Contents": [
                    {"Key": "a.txt", "Size": 1, "LastModified": "2024-01-01T00:00:00Z", "ETag": "\"abc\""}
                ]
            }"#;
            let empty_token = r#"{
                "IsTruncated": true,
                "NextContinuationToken": "",
                "Contents": [
                    {"Key": "b.txt", "Size": 2, "LastModified": "2024-01-01T00:00:00Z", "ETag": "\"def\""}
                ]
            }"#;

            for payload in [missing_token, empty_token] {
                let transport = ScriptedTransport::new(vec![payload]);
                let ctx = OperationContext::new()
                    .with_http_transport(HttpTransporter::new(transport.clone()));
                let mut lister =
                    PageLister::new(TosLister::new(core.clone(), ctx, "", OpList::default()));

                let mut entries = Vec::new();
                while let Some(entry) = lister.next().await.unwrap() {
                    entries.push(entry);
                }

                assert_eq!(entries.len(), 1);
                assert_eq!(
                    transport.requests.lock().unwrap().len(),
                    1,
                    "lister must stop instead of restarting from page 1"
                );
            }
        })
        .await
        .expect("test timed out");
    }

    #[tokio::test]
    async fn test_lister_pagination_and_etag_metadata() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let core = build_core();
            let page1 = r#"{
                "IsTruncated": true,
                "NextContinuationToken": "tok-1",
                "Contents": [
                    {"Key": "a.txt", "Size": 10, "LastModified": "2024-01-01T00:00:00Z", "ETag": "\"d41d8cd98f00b204e9800998ecf8427e-2\""}
                ]
            }"#;
            let page2 = r#"{
                "IsTruncated": false,
                "Contents": [
                    {"Key": "b.txt", "Size": 20, "LastModified": "2024-01-02T00:00:00Z", "ETag": "unquoted-etag"}
                ]
            }"#;
            let transport = ScriptedTransport::new(vec![page1, page2]);
            let ctx = OperationContext::new()
                .with_http_transport(HttpTransporter::new(transport.clone()));
            let mut lister = PageLister::new(TosLister::new(
                core,
                ctx,
                "",
                OpList::from(options::ListOptions {
                    start_after: Some("0.txt".to_string()),
                    ..Default::default()
                }),
            ));

            let mut entries = Vec::new();
            while let Some(entry) = lister.next().await.unwrap() {
                entries.push(entry);
            }

            assert_eq!(entries.len(), 2);
            assert_eq!(
                entries[0].metadata().etag(),
                Some("d41d8cd98f00b204e9800998ecf8427e-2")
            );
            assert_eq!(entries[0].metadata().content_md5(), None);
            assert_eq!(entries[1].metadata().etag(), Some("unquoted-etag"));
            assert_eq!(entries[1].metadata().content_md5(), None);

            let requests = transport.requests.lock().unwrap();
            assert_eq!(requests.len(), 2);
            assert!(requests[0].contains("start-after=0.txt"));
            assert!(requests[1].contains("continuation-token=tok-1"));
            assert!(!requests[1].contains("start-after="));
        })
        .await
        .expect("test timed out");
    }

    #[tokio::test]
    async fn test_versions_lister_terminates_on_missing_truncated_or_empty_marker() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let core = build_core();
            let missing_is_truncated = r#"{
                "Versions": [
                    {"Key": "a.txt", "VersionId": "v1", "IsLatest": true, "LastModified": "2024-01-01T00:00:00Z", "ETag": "\"abc\"", "Size": 1}
                ]
            }"#;
            let missing_key_marker = r#"{
                "IsTruncated": true,
                "Versions": [
                    {"Key": "a.txt", "VersionId": "v1", "IsLatest": true, "LastModified": "2024-01-01T00:00:00Z", "ETag": "\"abc\"", "Size": 1}
                ]
            }"#;
            let empty_key_marker = r#"{
                "IsTruncated": true,
                "NextKeyMarker": "",
                "NextVersionIdMarker": "v1",
                "Versions": [
                    {"Key": "a.txt", "VersionId": "v1", "IsLatest": true, "LastModified": "2024-01-01T00:00:00Z", "ETag": "\"abc\"", "Size": 1}
                ]
            }"#;

            for payload in [missing_is_truncated, missing_key_marker, empty_key_marker] {
                let transport = ScriptedTransport::new(vec![payload]);
                let ctx = OperationContext::new()
                    .with_http_transport(HttpTransporter::new(transport.clone()));
                let mut lister = PageLister::new(TosObjectVersionsLister::new(
                    core.clone(),
                    ctx,
                    "",
                    OpList::from(options::ListOptions {
                        versions: true,
                        ..Default::default()
                    }),
                ));

                let mut entries = Vec::new();
                while let Some(entry) = lister.next().await.unwrap() {
                    entries.push(entry);
                }

                assert_eq!(entries.len(), 1);
                assert_eq!(
                    transport.requests.lock().unwrap().len(),
                    1,
                    "versions lister must stop instead of looping forever"
                );
            }
        })
        .await
        .expect("test timed out");
    }

    #[tokio::test]
    async fn test_versions_lister_markers_with_spaces_and_etag_metadata() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let core = build_core();
            let page1 = r#"{
                "IsTruncated": true,
                "NextKeyMarker": "dir/file with spaces.txt",
                "NextVersionIdMarker": "ver 1",
                "Versions": [
                    {"Key": "dir/file with spaces.txt", "VersionId": "ver 1", "IsLatest": true, "LastModified": "2024-01-01T00:00:00Z", "ETag": "\"etag-multi-3\"", "Size": 42}
                ]
            }"#;
            let page2 = r#"{
                "IsTruncated": true,
                "NextKeyMarker": "dir/next file.txt",
                "NextVersionIdMarker": "",
                "Versions": [
                    {"Key": "dir/next file.txt", "VersionId": "ver-2", "IsLatest": true, "LastModified": "2024-01-02T00:00:00Z", "ETag": "\"etag-2\"", "Size": 12}
                ]
            }"#;
            let page3 = r#"{
                "IsTruncated": false,
                "Versions": []
            }"#;

            let transport = ScriptedTransport::new(vec![page1, page2, page3]);
            let ctx = OperationContext::new()
                .with_http_transport(HttpTransporter::new(transport.clone()));
            let mut lister = PageLister::new(TosObjectVersionsLister::new(
                core,
                ctx,
                "dir/",
                OpList::from(options::ListOptions {
                    versions: true,
                    ..Default::default()
                }),
            ));

            let mut entries = Vec::new();
            while let Some(entry) = lister.next().await.unwrap() {
                entries.push(entry);
            }

            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].metadata().etag(), Some("etag-multi-3"));
            assert_eq!(entries[0].metadata().content_md5(), None);
            assert_eq!(entries[1].metadata().etag(), Some("etag-2"));
            assert_eq!(entries[1].metadata().content_md5(), None);

            let requests = transport.requests.lock().unwrap();
            assert_eq!(requests.len(), 3);
            assert!(
                requests[1].contains("key-marker=dir%2Ffile%20with%20spaces.txt")
                    && requests[1].contains("version-id-marker=ver%201"),
                "page 2 must preserve spaces in both markers, got: {}",
                requests[1]
            );
            assert!(
                requests[2].contains("key-marker=dir%2Fnext%20file.txt")
                    && !requests[2].contains("version-id-marker="),
                "page 3 must send key-marker with spaces and omit empty version-id-marker, got: {}",
                requests[2]
            );
        })
        .await
        .expect("test timed out");
    }
}
