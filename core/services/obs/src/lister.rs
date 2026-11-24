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
use opendal_core::MetadataBuilder;
use opendal_core::OperationContext;
use opendal_core::Result;
use opendal_core::raw::*;

pub struct ObsLister {
    core: Arc<ObsCore>,
    ctx: OperationContext,
    path: String,
    delimiter: &'static str,
    limit: Option<usize>,
}

impl ObsLister {
    pub fn new(
        core: Arc<ObsCore>,
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

impl oio::PageList for ObsLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let resp = self
            .core
            .obs_list_objects(
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

        // Try our best to check whether this list is done.
        //
        // - Check `is_truncated`
        // - Check `next_marker`
        ctx.done = matches!(
            (output.is_truncated, output.next_marker.as_deref()),
            (Some(false), _) | (_, None | Some(""))
        );
        ctx.token = if ctx.done {
            String::new()
        } else {
            output.next_marker.unwrap_or_default()
        };

        let common_prefixes = output.common_prefixes;

        for prefix in common_prefixes {
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

            let metadata = if path.ends_with('/') {
                MetadataBuilder::dir()
            } else {
                MetadataBuilder::file(object.size)
            };
            let meta = metadata.build();

            let de = oio::Entry::with(path, meta);

            ctx.entries.push_back(de);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::time::Duration;

    use bytes::Bytes;
    use futures::stream;
    use http::Request;
    use http::Response;
    use http::StatusCode;
    use opendal_core::Buffer;
    use opendal_core::Builder;
    use opendal_core::HttpBody;
    use opendal_core::HttpTransport;
    use opendal_core::HttpTransporter;
    use opendal_core::options;
    use opendal_core::raw::oio::List;
    use opendal_core::raw::oio::PageList;

    use super::*;
    use crate::backend::ObsBackend;
    use crate::backend::ObsBuilder;

    const TEST_TIMEOUT: Duration = Duration::from_secs(5);

    struct ScriptedListTransport {
        responses: Mutex<VecDeque<&'static str>>,
        queries: Arc<Mutex<Vec<Option<String>>>>,
    }

    impl ScriptedListTransport {
        fn new(responses: Vec<&'static str>) -> (Self, Arc<Mutex<Vec<Option<String>>>>) {
            let queries = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    responses: Mutex::new(responses.into()),
                    queries: queries.clone(),
                },
                queries,
            )
        }
    }

    impl HttpTransport for ScriptedListTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            self.queries
                .lock()
                .expect("mutex must not poison")
                .push(req.uri().query().map(str::to_string));

            let body = self
                .responses
                .lock()
                .expect("mutex must not poison")
                .pop_front()
                .expect("unexpected extra list request");
            let buf = Buffer::from(Bytes::from_static(body.as_bytes()));
            let size = buf.len() as u64;

            Ok(Response::builder()
                .status(StatusCode::OK)
                .body(HttpBody::new(stream::iter(vec![Ok(buf)]), Some(size)))
                .expect("mock response must build"))
        }
    }

    fn build_backend(root: &str) -> ObsBackend {
        let service = ObsBuilder::default()
            .bucket("example-bucket")
            .endpoint("https://obs.cn-north-4.myhuaweicloud.com")
            .root(root)
            .skip_signature()
            .disable_config_load()
            .build()
            .expect("obs backend must build");
        (&service as &dyn std::any::Any)
            .downcast_ref::<ObsBackend>()
            .expect("builder must produce ObsBackend")
            .clone()
    }

    #[tokio::test]
    async fn list_root_omits_prefix_and_subroot_preserves_prefix() {
        tokio::time::timeout(TEST_TIMEOUT, async {
            let empty_page = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
    <Name>example-bucket</Name>
    <IsTruncated>false</IsTruncated>
</ListBucketResult>"#;

            // root == "/" and path == "/" => resolved p is "", so prefix must be omitted.
            let (transport, queries) = ScriptedListTransport::new(vec![empty_page]);
            let backend = build_backend("/");
            let ctx = OperationContext::new().with_http_transport(HttpTransporter::new(transport));
            let mut lister = backend
                .list(&ctx, "/", OpList::default())
                .expect("list must build");
            assert!(lister.next().await.expect("list must succeed").is_none());
            assert_eq!(
                queries.lock().expect("mutex must not poison").as_slice(),
                &[Some("delimiter=/".to_string())]
            );

            // root == "/sub/" and path == "" (direct Service::list) => prefix must be "sub/".
            let (transport, queries) = ScriptedListTransport::new(vec![empty_page]);
            let backend = build_backend("/sub/");
            let ctx = OperationContext::new().with_http_transport(HttpTransporter::new(transport));
            let mut lister = backend
                .list(&ctx, "", OpList::default())
                .expect("list must build");
            assert!(lister.next().await.expect("list must succeed").is_none());
            assert_eq!(
                queries.lock().expect("mutex must not poison").as_slice(),
                &[Some("prefix=sub/&delimiter=/".to_string())]
            );

            // root == "/sub/" and path == "/" => prefix must be "sub/".
            let (transport, queries) = ScriptedListTransport::new(vec![empty_page]);
            let backend = build_backend("/sub/");
            let ctx = OperationContext::new().with_http_transport(HttpTransporter::new(transport));
            let mut lister = backend
                .list(&ctx, "/", OpList::default())
                .expect("list must build");
            assert!(lister.next().await.expect("list must succeed").is_none());
            assert_eq!(
                queries.lock().expect("mutex must not poison").as_slice(),
                &[Some("prefix=sub/&delimiter=/".to_string())]
            );
        })
        .await
        .expect("test must not time out");
    }

    #[tokio::test]
    async fn lister_stops_when_is_truncated_false_even_with_next_marker() {
        tokio::time::timeout(TEST_TIMEOUT, async {
            let page = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
    <Name>example-bucket</Name>
    <NextMarker>obj004</NextMarker>
    <IsTruncated>false</IsTruncated>
    <Contents>
        <Key>obj002</Key>
        <Size>9</Size>
    </Contents>
</ListBucketResult>"#;

            let (transport, queries) = ScriptedListTransport::new(vec![page]);
            let backend = build_backend("/");
            let ctx = OperationContext::new().with_http_transport(HttpTransporter::new(transport));
            let lister = ObsLister::new(backend.core, ctx, "/", true, None);
            let mut page_ctx = oio::PageContext {
                done: false,
                token: String::new(),
                entries: VecDeque::new(),
            };

            lister
                .next_page(&mut page_ctx)
                .await
                .expect("next_page must succeed");

            assert!(page_ctx.done, "IsTruncated=false must mark listing done");
            assert!(page_ctx.token.is_empty(), "token must be cleared when done");
            assert_eq!(page_ctx.entries.len(), 1);
            assert_eq!(page_ctx.entries[0].path(), "obj002");
            assert_eq!(queries.lock().expect("mutex must not poison").len(), 1);
        })
        .await
        .expect("test must not time out");
    }

    #[tokio::test]
    async fn lister_paginates_when_truncated_and_stops_on_missing_marker() {
        tokio::time::timeout(TEST_TIMEOUT, async {
            let page1 = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
    <Name>example-bucket</Name>
    <NextMarker>a.txt</NextMarker>
    <IsTruncated>true</IsTruncated>
    <Contents>
        <Key>a.txt</Key>
        <Size>1</Size>
    </Contents>
</ListBucketResult>"#;
            let page2 = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
    <Name>example-bucket</Name>
    <NextMarker>b.txt</NextMarker>
    <IsTruncated>false</IsTruncated>
    <Contents>
        <Key>b.txt</Key>
        <Size>2</Size>
    </Contents>
</ListBucketResult>"#;

            let recursive_list: OpList = options::ListOptions {
                recursive: true,
                ..Default::default()
            }
            .into();

            let (transport, queries) = ScriptedListTransport::new(vec![page1, page2]);
            let backend = build_backend("/");
            let ctx = OperationContext::new().with_http_transport(HttpTransporter::new(transport));
            let mut lister = backend
                .list(&ctx, "/", recursive_list.clone())
                .expect("list must build");

            let mut paths = Vec::new();
            while let Some(entry) = lister.next().await.expect("list must succeed") {
                paths.push(entry.path().to_string());
            }
            assert_eq!(paths, vec!["a.txt", "b.txt"]);
            assert_eq!(
                queries.lock().expect("mutex must not poison").as_slice(),
                &[None, Some("marker=a.txt".to_string())]
            );

            // Missing IsTruncated still paginates while NextMarker is non-empty and stops when absent.
            let missing_truncated_page1 = r#"<ListBucketResult><NextMarker>m1</NextMarker><Contents><Key>m1</Key><Size>1</Size></Contents></ListBucketResult>"#;
            let missing_truncated_page2 = r#"<ListBucketResult><Contents><Key>m2</Key><Size>2</Size></Contents></ListBucketResult>"#;
            let (transport, queries) =
                ScriptedListTransport::new(vec![missing_truncated_page1, missing_truncated_page2]);
            let backend = build_backend("/");
            let ctx = OperationContext::new().with_http_transport(HttpTransporter::new(transport));
            let mut lister = backend
                .list(&ctx, "/", recursive_list.clone())
                .expect("list must build");

            let mut paths = Vec::new();
            while let Some(entry) = lister.next().await.expect("list must succeed") {
                paths.push(entry.path().to_string());
            }
            assert_eq!(paths, vec!["m1", "m2"]);
            assert_eq!(
                queries.lock().expect("mutex must not poison").as_slice(),
                &[None, Some("marker=m1".to_string())]
            );

            // IsTruncated=true with empty/missing NextMarker must stop instead of looping.
            for truncated_without_marker in [
                r#"<ListBucketResult><IsTruncated>true</IsTruncated><Contents><Key>only.txt</Key><Size>1</Size></Contents></ListBucketResult>"#,
                r#"<ListBucketResult><IsTruncated>true</IsTruncated><NextMarker></NextMarker><Contents><Key>only.txt</Key><Size>1</Size></Contents></ListBucketResult>"#,
            ] {
                let (transport, queries) =
                    ScriptedListTransport::new(vec![truncated_without_marker]);
                let backend = build_backend("/");
                let ctx =
                    OperationContext::new().with_http_transport(HttpTransporter::new(transport));
                let mut lister = backend
                    .list(&ctx, "/", recursive_list.clone())
                    .expect("list must build");

                let mut paths = Vec::new();
                while let Some(entry) = lister.next().await.expect("list must succeed") {
                    paths.push(entry.path().to_string());
                }
                assert_eq!(paths, vec!["only.txt"]);
                assert_eq!(queries.lock().expect("mutex must not poison").len(), 1);
            }
        })
        .await
        .expect("test must not time out");
    }
}
