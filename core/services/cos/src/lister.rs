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
use quick_xml::de;

use super::core::parse_error;
use super::core::*;
use opendal_core::Error;
use opendal_core::MetadataBuilder;
use opendal_core::OperationContext;
use opendal_core::Result;
use opendal_core::raw::oio::PageContext;
use opendal_core::raw::*;

pub type CosListers = TwoWays<oio::PageLister<CosLister>, oio::PageLister<CosObjectVersionsLister>>;

pub struct CosLister {
    core: Arc<CosCore>,
    ctx: OperationContext,
    path: String,
    delimiter: &'static str,
    limit: Option<usize>,
}

impl CosLister {
    pub fn new(
        core: Arc<CosCore>,
        ctx: OperationContext,
        path: &str,
        recursive: bool,
        limit: Option<usize>,
    ) -> Self {
        let delimiter = if recursive { "" } else { "/" };
        Self {
            core,
            ctx,
            path: path.to_string(),
            delimiter,
            limit,
        }
    }
}

impl oio::PageList for CosLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let resp = self
            .core
            .cos_list_objects(
                &self.ctx,
                &self.path,
                &ctx.token,
                self.delimiter,
                self.limit,
            )
            .await?;

        if resp.status() != http::StatusCode::OK {
            return Err(parse_error(
                ErrorContext::new(ServiceOperation("ListObjects")),
                resp,
            ));
        }

        let bs = resp.into_body();

        let output: ListObjectsOutput =
            de::from_reader(bs.reader()).map_err(new_xml_deserialize_error)?;

        // Terminate pagination if COS reports the page is not truncated or omits NextMarker.
        let next_marker = output.next_marker.as_deref().unwrap_or_default();
        if output.is_truncated == Some(false) || next_marker.is_empty() {
            ctx.done = true;
            ctx.token.clear();
        } else {
            ctx.done = false;
            ctx.token = next_marker.to_string();
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
            meta.last_modified(object.last_modified.parse::<Timestamp>()?);
            if let Some(etag) = object.etag {
                meta.etag(&etag);
            }

            let de = oio::Entry::with(path, meta.build());
            ctx.entries.push_back(de);
        }

        Ok(())
    }
}

/// refer: https://cloud.tencent.com/document/product/436/35521
pub struct CosObjectVersionsLister {
    core: Arc<CosCore>,
    ctx: OperationContext,

    prefix: String,
    args: OpList,

    delimiter: &'static str,
    abs_start_after: Option<String>,
}

impl CosObjectVersionsLister {
    pub fn new(core: Arc<CosCore>, ctx: OperationContext, path: &str, args: OpList) -> Self {
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

impl oio::PageList for CosObjectVersionsLister {
    async fn next_page(&self, ctx: &mut PageContext) -> Result<()> {
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
            .cos_list_object_versions(
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

        let body = resp.into_body();
        let output: ListObjectVersionsOutput = de::from_reader(body.reader())
            .map_err(new_xml_deserialize_error)
            // Allow Cos list to retry on XML deserialization errors.
            //
            // This is because the Cos list API may return incomplete XML data under high load.
            // We are confident that our XML decoding logic is correct. When this error occurs,
            // we allow retries to obtain the correct data.
            .map_err(Error::set_temporary)?;

        // Terminate pagination unless COS explicitly reports truncation with a non-empty
        // NextKeyMarker. Join markers with NUL (`\0`), which is disallowed in XML 1.0
        // character data, so spaces in keys or version IDs cannot corrupt pagination tokens.
        let next_key_marker = output.next_key_marker.as_deref().unwrap_or_default();
        let next_version_id_marker = output.next_version_id_marker.as_deref().unwrap_or_default();
        if output.is_truncated == Some(true) && !next_key_marker.is_empty() {
            ctx.done = false;
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

        for version_object in output.version {
            // `list` must be additive, so we need to include the latest version object
            // even if `versions` is not enabled.
            //
            // Here we skip all non-latest version objects if `versions` is not enabled.
            if !(self.args.versions() || version_object.is_latest) {
                continue;
            }

            let mut path = build_relative_path(&self.core.root, &version_object.key);
            if path.is_empty() {
                path = "/".to_owned();
            }

            let mut meta = if path.ends_with('/') {
                MetadataBuilder::dir()
            } else {
                MetadataBuilder::file(version_object.size)
            };
            meta.version(&version_object.version_id);
            meta.is_current(Some(version_object.is_latest));
            meta.last_modified(version_object.last_modified.parse::<Timestamp>()?);
            if let Some(etag) = version_object.etag {
                meta.etag(&etag);
            }

            let entry = oio::Entry::new(&path, meta.build());
            ctx.entries.push_back(entry);
        }

        if self.args.deleted() {
            for delete_marker in output.delete_marker {
                let mut path = build_relative_path(&self.core.root, &delete_marker.key);
                if path.is_empty() {
                    path = "/".to_owned();
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

                let entry = oio::Entry::new(&path, meta.build());
                ctx.entries.push_back(entry);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::time::Duration;

    use futures::stream;
    use http::{Request, Response, StatusCode};

    use super::*;
    use crate::core::test_support::test_core;
    use opendal_core::raw::oio::{List, PageList, PageLister};
    use opendal_core::*;

    #[derive(Clone)]
    struct ScriptedListTransport {
        responses: Arc<Mutex<VecDeque<String>>>,
        requests: Arc<Mutex<Vec<String>>>,
    }

    impl ScriptedListTransport {
        fn new(responses: Vec<&str>) -> Self {
            Self {
                responses: Arc::new(Mutex::new(
                    responses.into_iter().map(str::to_string).collect(),
                )),
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl HttpTransport for ScriptedListTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            self.requests.lock().unwrap().push(req.uri().to_string());
            let body_str = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected extra pagination request");
            let buf = Buffer::from(body_str);
            let len = buf.len() as u64;
            Response::builder()
                .status(StatusCode::OK)
                .body(HttpBody::new(stream::iter([Ok(buf)]), Some(len)))
                .map_err(new_request_build_error)
        }
    }

    async fn drain_pages<L: PageList>(lister: L) -> Vec<oio::Entry> {
        let mut lister = PageLister::new(lister);
        let mut entries = Vec::new();
        while let Some(entry) = lister.next().await.unwrap() {
            entries.push(entry);
        }
        entries
    }

    #[tokio::test]
    async fn cos_lister_stops_when_not_truncated_and_does_not_set_content_md5() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
    <Name>test-bucket</Name>
    <Prefix></Prefix>
    <NextMarker>obj004</NextMarker>
    <IsTruncated>false</IsTruncated>
    <Contents>
        <Key>obj001</Key>
        <LastModified>2024-01-01T00:00:00.000Z</LastModified>
        <ETag>"a72e382246ac83e86bd203389849e71d"</ETag>
        <Size>9</Size>
    </Contents>
    <Contents>
        <Key>obj002</Key>
        <LastModified>2024-01-01T00:00:00.000Z</LastModified>
        <ETag>"a72e382246ac83e86bd203389849e71d-3"</ETag>
        <Size>10485760</Size>
    </Contents>
</ListBucketResult>"#;

            let transport = ScriptedListTransport::new(vec![xml]);
            let ctx = OperationContext::default()
                .with_http_transport(HttpTransporter::new(transport.clone()));
            let lister = CosLister::new(test_core("/"), ctx, "", true, None);

            let entries = drain_pages(lister).await;
            assert_eq!(transport.requests.lock().unwrap().len(), 1);
            assert_eq!(entries.len(), 2);
            assert_eq!(
                entries[0].metadata().etag(),
                Some("\"a72e382246ac83e86bd203389849e71d\"")
            );
            assert_eq!(entries[0].metadata().content_md5(), None);
            assert_eq!(
                entries[1].metadata().etag(),
                Some("\"a72e382246ac83e86bd203389849e71d-3\"")
            );
            assert_eq!(entries[1].metadata().content_md5(), None);
        })
        .await
        .expect("test timed out");
    }

    #[tokio::test]
    async fn cos_lister_stops_when_truncated_with_empty_or_missing_next_marker() {
        tokio::time::timeout(Duration::from_secs(5), async {
            for xml in [
                r#"<ListBucketResult><IsTruncated>true</IsTruncated><NextMarker></NextMarker></ListBucketResult>"#,
                r#"<ListBucketResult><IsTruncated>true</IsTruncated></ListBucketResult>"#,
            ] {
                let transport = ScriptedListTransport::new(vec![xml]);
                let ctx = OperationContext::default()
                    .with_http_transport(HttpTransporter::new(transport.clone()));
                let lister = CosLister::new(test_core("/"), ctx, "", true, None);

                let entries = drain_pages(lister).await;
                assert!(entries.is_empty());
                assert_eq!(transport.requests.lock().unwrap().len(), 1);
            }
        })
        .await
        .expect("test timed out");
    }

    #[tokio::test]
    async fn cos_lister_paginates_across_multiple_pages() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let page1 = r#"<ListBucketResult>
    <IsTruncated>true</IsTruncated>
    <NextMarker>obj001</NextMarker>
    <Contents>
        <Key>obj001</Key>
        <LastModified>2024-01-01T00:00:00.000Z</LastModified>
        <ETag>"etag1"</ETag>
        <Size>1</Size>
    </Contents>
</ListBucketResult>"#;
            let page2 = r#"<ListBucketResult>
    <IsTruncated>false</IsTruncated>
    <NextMarker>obj002</NextMarker>
    <Contents>
        <Key>obj002</Key>
        <LastModified>2024-01-01T00:00:00.000Z</LastModified>
        <ETag>"etag2"</ETag>
        <Size>2</Size>
    </Contents>
</ListBucketResult>"#;

            let transport = ScriptedListTransport::new(vec![page1, page2]);
            let ctx = OperationContext::default()
                .with_http_transport(HttpTransporter::new(transport.clone()));
            let lister = CosLister::new(test_core("/"), ctx, "", true, None);

            let entries = drain_pages(lister).await;
            let requests = transport.requests.lock().unwrap();
            assert_eq!(requests.len(), 2);
            assert!(requests[1].contains("marker=obj001"));
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].path(), "obj001");
            assert_eq!(entries[1].path(), "obj002");
        })
        .await
        .expect("test timed out");
    }

    #[tokio::test]
    async fn cos_versions_lister_terminates_on_missing_is_truncated_or_empty_key_marker() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let cases = [
                // Missing <IsTruncated> must not loop forever.
                r#"<ListVersionsResult>
    <NextKeyMarker>obj1</NextKeyMarker>
    <Version>
        <Key>obj1</Key>
        <VersionId>v1</VersionId>
        <IsLatest>true</IsLatest>
        <LastModified>2024-01-01T00:00:00.000Z</LastModified>
        <ETag>"a72e382246ac83e86bd203389849e71d"</ETag>
        <Size>5</Size>
    </Version>
</ListVersionsResult>"#,
                // IsTruncated=true with missing NextKeyMarker (even with NextVersionIdMarker).
                r#"<ListVersionsResult>
    <IsTruncated>true</IsTruncated>
    <NextVersionIdMarker>v1</NextVersionIdMarker>
    <Version>
        <Key>obj1</Key>
        <VersionId>v1</VersionId>
        <IsLatest>true</IsLatest>
        <LastModified>2024-01-01T00:00:00.000Z</LastModified>
        <ETag>"a72e382246ac83e86bd203389849e71d-2"</ETag>
        <Size>5</Size>
    </Version>
</ListVersionsResult>"#,
                // IsTruncated=true with empty NextKeyMarker.
                r#"<ListVersionsResult>
    <IsTruncated>true</IsTruncated>
    <NextKeyMarker></NextKeyMarker>
    <Version>
        <Key>obj1</Key>
        <VersionId>v1</VersionId>
        <IsLatest>true</IsLatest>
        <LastModified>2024-01-01T00:00:00.000Z</LastModified>
        <ETag>"a72e382246ac83e86bd203389849e71d"</ETag>
        <Size>5</Size>
    </Version>
</ListVersionsResult>"#,
            ];

            let core = test_core("/");
            let args = OpList::from_options(
                &core.capability,
                options::ListOptions {
                    versions: true,
                    recursive: true,
                    ..Default::default()
                },
            )
            .unwrap();

            for xml in cases {
                let transport = ScriptedListTransport::new(vec![xml]);
                let ctx = OperationContext::default()
                    .with_http_transport(HttpTransporter::new(transport.clone()));
                let lister = CosObjectVersionsLister::new(core.clone(), ctx, "", args.clone());

                let entries = drain_pages(lister).await;
                assert_eq!(transport.requests.lock().unwrap().len(), 1);
                assert_eq!(entries.len(), 1);
                assert!(entries[0].metadata().etag().is_some());
                assert_eq!(entries[0].metadata().content_md5(), None);
            }
        })
        .await
        .expect("test timed out");
    }

    #[tokio::test]
    async fn cos_versions_lister_preserves_whitespace_in_markers() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let page1 = r#"<ListVersionsResult>
    <IsTruncated>true</IsTruncated>
    <NextKeyMarker>dir/file with spaces.txt</NextKeyMarker>
    <NextVersionIdMarker>ver 1+2</NextVersionIdMarker>
    <Version>
        <Key>dir/file with spaces.txt</Key>
        <VersionId>ver 1+2</VersionId>
        <IsLatest>false</IsLatest>
        <LastModified>2024-01-01T00:00:00.000Z</LastModified>
        <ETag>"etag1"</ETag>
        <Size>3</Size>
    </Version>
</ListVersionsResult>"#;
            let page2 = r#"<ListVersionsResult>
    <IsTruncated>false</IsTruncated>
    <Version>
        <Key>dir/next file.txt</Key>
        <VersionId>ver 2</VersionId>
        <IsLatest>true</IsLatest>
        <LastModified>2024-01-01T00:00:01.000Z</LastModified>
        <ETag>"etag2-2"</ETag>
        <Size>4</Size>
    </Version>
</ListVersionsResult>"#;

            let core = test_core("/");
            let args = OpList::from_options(
                &core.capability,
                options::ListOptions {
                    versions: true,
                    recursive: true,
                    ..Default::default()
                },
            )
            .unwrap();

            let transport = ScriptedListTransport::new(vec![page1, page2]);
            let ctx = OperationContext::default()
                .with_http_transport(HttpTransporter::new(transport.clone()));
            let lister = CosObjectVersionsLister::new(core, ctx, "", args);

            let entries = drain_pages(lister).await;
            let requests = transport.requests.lock().unwrap();
            assert_eq!(requests.len(), 2);
            assert!(
                requests[1].contains("key-marker=dir/file%20with%20spaces.txt"),
                "unexpected page2 uri: {}",
                requests[1]
            );
            assert!(
                requests[1].contains("version-id-marker=ver%201%2B2"),
                "unexpected page2 uri: {}",
                requests[1]
            );
            assert_eq!(entries.len(), 2);
            for entry in &entries {
                assert!(entry.metadata().etag().is_some());
                assert_eq!(entry.metadata().content_md5(), None);
            }
        })
        .await
        .expect("test timed out");
    }
}
