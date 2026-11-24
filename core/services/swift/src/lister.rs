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

use super::core::parse_error;
use super::core::*;
use opendal_core::raw::*;
use opendal_core::*;

pub struct SwiftLister {
    core: Arc<SwiftCore>,
    ctx: OperationContext,
    path: String,
    delimiter: &'static str,
    limit: Option<usize>,
    abs_start_after: Option<String>,
}

impl SwiftLister {
    pub fn new(
        core: Arc<SwiftCore>,
        ctx: OperationContext,
        path: String,
        recursive: bool,
        limit: Option<usize>,
        start_after: Option<String>,
    ) -> Self {
        let delimiter = if recursive { "" } else { "/" };
        // Swift listing names are absolute (root-prefixed) and the lister pages
        // by `marker`, so the start-after must be made absolute as well.
        let abs_start_after =
            start_after.map(|start_after| build_absolute_path(&core.root, &start_after));
        Self {
            core,
            ctx,
            path,
            delimiter,
            limit,
            abs_start_after,
        }
    }
}

impl oio::PageList for SwiftLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        // `start_after` applies to the first page only; subsequent pages
        // continue from the previous page's last entry carried in `ctx.token`.
        let marker = if ctx.token.is_empty() {
            self.abs_start_after.as_deref().unwrap_or("")
        } else {
            ctx.token.as_str()
        };
        let response = self
            .core
            .swift_list(&self.ctx, &self.path, self.delimiter, self.limit, marker)
            .await?;

        let status_code = response.status();

        if !status_code.is_success() {
            let error = parse_error(ErrorContext::new(ServiceOperation("ListObjects")), response);
            return Err(error);
        }

        let bytes = response.into_body();
        let decoded_response: Vec<ListOpResponse> =
            serde_json::from_reader(bytes.reader()).map_err(new_json_deserialize_error)?;

        // Update token and done based on resp.
        if let Some(entry) = decoded_response.last() {
            let path = match entry {
                ListOpResponse::Subdir { subdir } => subdir,
                ListOpResponse::FileInfo { name, .. } => name,
            };
            if path.is_empty() || path == marker {
                ctx.done = true;
            } else {
                ctx.token.clone_from(path);
            }
        } else {
            ctx.done = true;
        }

        for status in decoded_response {
            let entry: oio::Entry = match status {
                ListOpResponse::Subdir { subdir } => {
                    let mut path = build_relative_path(self.core.root.as_str(), subdir.as_str());
                    if path.is_empty() {
                        path = "/".to_string();
                    }
                    let meta = MetadataBuilder::dir().build();
                    oio::Entry::with(path, meta)
                }
                ListOpResponse::FileInfo {
                    bytes,
                    hash,
                    name,
                    content_type,
                    mut last_modified,
                } => {
                    let mut path = build_relative_path(self.core.root.as_str(), name.as_str());
                    if path.is_empty() {
                        path = "/".to_string();
                    }
                    let mut meta = if path.ends_with('/') {
                        MetadataBuilder::dir()
                    } else {
                        MetadataBuilder::file(bytes)
                    };
                    if !hash.is_empty() {
                        meta.etag(hash.as_str());
                    }

                    // OpenStack Swift returns UTC timestamps without a trailing 'Z'
                    // (e.g. "2023-10-28T19:18:11.682610"), while compatible servers such as
                    // Ceph RGW may include an explicit offset like "+00:00".
                    let ts = match last_modified.parse::<Timestamp>() {
                        Ok(ts) => ts,
                        Err(_) => {
                            last_modified.push('Z');
                            last_modified.parse::<Timestamp>()?
                        }
                    };
                    meta.last_modified(ts);

                    if let Some(content_type) = content_type {
                        meta.content_type(content_type.as_str());
                    }

                    oio::Entry::with(path, meta.build())
                }
            };
            ctx.entries.push_back(entry);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use futures::stream;
    use http::Response;
    use http::StatusCode;

    use super::*;
    use opendal_core::raw::oio::PageList;

    struct ScriptedListTransport {
        responses: Mutex<VecDeque<String>>,
        seen_uris: Mutex<Vec<String>>,
    }

    impl ScriptedListTransport {
        fn new(responses: Vec<&str>) -> Self {
            Self {
                responses: Mutex::new(responses.into_iter().map(str::to_string).collect()),
                seen_uris: Mutex::new(Vec::new()),
            }
        }
    }

    impl HttpTransport for ScriptedListTransport {
        async fn fetch(&self, req: http::Request<Buffer>) -> Result<Response<HttpBody>> {
            self.seen_uris.lock().unwrap().push(req.uri().to_string());
            let body_str = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| "[]".to_string());
            let body = Buffer::from(body_str);
            let size = body.len() as u64;
            Response::builder()
                .status(StatusCode::OK)
                .header(http::header::CONTENT_TYPE, "application/json")
                .body(HttpBody::new(stream::iter([Ok(body)]), Some(size)))
                .map_err(new_request_build_error)
        }
    }

    fn build_lister(
        responses: Vec<&str>,
        start_after: Option<String>,
    ) -> (SwiftLister, Arc<ScriptedListTransport>) {
        let core = Arc::new(SwiftCore {
            info: ServiceInfo::new(crate::SWIFT_SCHEME, "/", "container"),
            capability: Capability {
                list: true,
                list_with_recursive: true,
                list_with_start_after: true,
                ..Default::default()
            },
            root: "/".to_string(),
            endpoint: "http://127.0.0.1:8080/v1/AUTH_test".to_string(),
            container: "container".to_string(),
            token: "test-token".to_string(),
            temp_url_key: String::new(),
            temp_url_hash_algorithm: TempUrlHashAlgorithm::Sha256,
        });
        let transport = Arc::new(ScriptedListTransport::new(responses));
        let ctx = OperationContext::default()
            .with_http_transport(HttpTransporter::new(ArcTransport(transport.clone())));
        let lister = SwiftLister::new(core, ctx, String::new(), true, None, start_after);
        (lister, transport)
    }

    struct ArcTransport(Arc<ScriptedListTransport>);

    impl HttpTransport for ArcTransport {
        async fn fetch(&self, req: http::Request<Buffer>) -> Result<Response<HttpBody>> {
            self.0.fetch(req).await
        }
    }

    fn empty_page_ctx() -> oio::PageContext {
        oio::PageContext {
            done: false,
            token: String::new(),
            entries: VecDeque::new(),
        }
    }

    #[tokio::test]
    async fn next_page_sets_etag_and_parses_all_timestamp_formats() {
        let page = r#"[
            {
                "bytes": 10,
                "hash": "5e6b5b70b0426b1cc1968003e1afa5ad",
                "name": "swift-no-tz.txt",
                "content_type": "text/plain",
                "last_modified": "2023-10-28T19:18:11.682610"
            },
            {
                "bytes": 20,
                "hash": "\"slo-etag-hash\"",
                "name": "ceph-offset.txt",
                "content_type": "application/octet-stream",
                "last_modified": "2023-10-28T19:18:11.682610+00:00"
            },
            {
                "bytes": 30,
                "hash": "abcdef",
                "name": "rfc3339-z.txt",
                "content_type": "text/plain",
                "last_modified": "2023-10-28T19:18:11.682610Z"
            },
            {
                "bytes": 40,
                "hash": "123456",
                "name": "negative-offset.txt",
                "content_type": "text/plain",
                "last_modified": "2023-10-28T14:18:11.682610-05:00"
            }
        ]"#;

        let (lister, _transport) = build_lister(vec![page], None);
        let mut ctx = empty_page_ctx();
        lister.next_page(&mut ctx).await.unwrap();

        assert!(!ctx.done);
        assert_eq!(ctx.token, "negative-offset.txt");
        assert_eq!(ctx.entries.len(), 4);

        let expected_ts: Timestamp = "2023-10-28T19:18:11.682610Z".parse().unwrap();

        let first = ctx.entries.pop_front().unwrap();
        assert_eq!(first.path(), "swift-no-tz.txt");
        assert_eq!(
            first.metadata().etag(),
            Some("5e6b5b70b0426b1cc1968003e1afa5ad")
        );
        assert_eq!(first.metadata().content_md5(), None);
        assert_eq!(first.metadata().last_modified(), Some(expected_ts));

        let second = ctx.entries.pop_front().unwrap();
        assert_eq!(second.path(), "ceph-offset.txt");
        assert_eq!(second.metadata().etag(), Some("\"slo-etag-hash\""));
        assert_eq!(second.metadata().content_md5(), None);
        assert_eq!(second.metadata().last_modified(), Some(expected_ts));

        let third = ctx.entries.pop_front().unwrap();
        assert_eq!(third.metadata().last_modified(), Some(expected_ts));

        let fourth = ctx.entries.pop_front().unwrap();
        assert_eq!(fourth.metadata().last_modified(), Some(expected_ts));
    }

    #[tokio::test]
    async fn next_page_stops_on_repeated_token() {
        let page = r#"[{
            "bytes": 5,
            "hash": "abc",
            "name": "stuck.txt",
            "content_type": "text/plain",
            "last_modified": "2023-10-28T19:18:11.682610"
        }]"#;
        let (lister, transport) = build_lister(vec![page, page], None);
        let mut ctx = empty_page_ctx();

        lister.next_page(&mut ctx).await.unwrap();
        assert!(!ctx.done);
        assert_eq!(ctx.token, "stuck.txt");

        lister.next_page(&mut ctx).await.unwrap();
        assert!(ctx.done);
        assert_eq!(transport.seen_uris.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn next_page_stops_on_empty_or_echoed_start_after_token() {
        let empty_name_page = r#"[{"subdir": ""}]"#;
        let (lister, _) = build_lister(vec![empty_name_page], None);
        let mut ctx = empty_page_ctx();
        lister.next_page(&mut ctx).await.unwrap();
        assert!(ctx.done);

        let echoed_start_after_page = r#"[{
            "bytes": 5,
            "hash": "abc",
            "name": "marker.txt",
            "content_type": "text/plain",
            "last_modified": "2023-10-28T19:18:11.682610"
        }]"#;
        let (lister, _) = build_lister(
            vec![echoed_start_after_page],
            Some("marker.txt".to_string()),
        );
        let mut ctx = empty_page_ctx();
        lister.next_page(&mut ctx).await.unwrap();
        assert!(ctx.done);
    }
}
