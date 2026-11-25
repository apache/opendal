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
use opendal_core::raw::*;
use opendal_core::*;

use super::core::{ErrorContext, ListObjectsResponse, UpyunCore, parse_error};

pub struct UpyunLister {
    core: Arc<UpyunCore>,
    ctx: OperationContext,

    path: String,
    limit: Option<usize>,
}

impl UpyunLister {
    pub(super) fn new(
        core: Arc<UpyunCore>,
        ctx: OperationContext,
        path: &str,
        limit: Option<usize>,
    ) -> Self {
        UpyunLister {
            core,
            ctx,
            path: path.to_string(),
            limit,
        }
    }
}

impl oio::PageList for UpyunLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let resp = self
            .core
            .list_objects(&self.ctx, &self.path, &ctx.token, self.limit)
            .await?;

        if resp.status() == http::StatusCode::NOT_FOUND {
            ctx.done = true;
            return Ok(());
        }

        match resp.status() {
            http::StatusCode::OK => {}
            http::StatusCode::NOT_FOUND => {
                ctx.done = true;
                return Ok(());
            }
            _ => {
                return Err(parse_error(
                    ErrorContext::new(ServiceOperation("ListObjects")),
                    resp,
                ));
            }
        }

        let bs = resp.into_body();

        let response: ListObjectsResponse =
            serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

        // ref https://help.upyun.com/knowledge-base/rest_api/#e88eb7e58f96e79baee5bd95e69687e4bbb6e58897e8a1a8
        // when iter is "g2gCZAAEbmV4dGQAA2VvZg", it means the list is done.
        // Also stop if iter is empty or repeats the previous cursor to avoid infinite pagination.
        ctx.done = response.iter.is_empty()
            || response.iter == "g2gCZAAEbmV4dGQAA2VvZg"
            || response.iter == ctx.token;

        ctx.token = response.iter;

        for file in response.files {
            let path = build_absolute_path(&normalize_root(&self.path), &file.name);

            let entry = if file.type_field == "folder" {
                let path = format!("{path}/");
                oio::Entry::new(&path, MetadataBuilder::dir().build())
            } else {
                let mut m = MetadataBuilder::file(file.length);
                m.content_type(file.type_field)
                    .last_modified(Timestamp::from_second(file.last_modified)?);
                oio::Entry::new(&path, m.build())
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
    use std::time::Duration;

    use futures::stream;
    use http::{Request, Response, StatusCode};
    use opendal_core::raw::oio::List;

    use super::*;
    use crate::UPYUN_SCHEME;
    use crate::core::UpyunSigner;

    #[derive(Clone)]
    struct ScriptedListTransport {
        responses: Arc<Mutex<VecDeque<(StatusCode, String)>>>,
        request_count: Arc<Mutex<usize>>,
    }

    impl ScriptedListTransport {
        fn new(responses: Vec<(StatusCode, String)>) -> Self {
            Self {
                responses: Arc::new(Mutex::new(responses.into())),
                request_count: Arc::new(Mutex::new(0)),
            }
        }

        fn request_count(&self) -> usize {
            *self.request_count.lock().unwrap()
        }
    }

    impl HttpTransport for ScriptedListTransport {
        async fn fetch(&self, _req: Request<Buffer>) -> Result<Response<HttpBody>> {
            *self.request_count.lock().unwrap() += 1;
            let (status, body) = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected extra list request (possible infinite pagination loop)");
            let len = body.len() as u64;
            Ok(Response::builder()
                .status(status)
                .body(HttpBody::new(
                    stream::iter([Ok(Buffer::from(body))]),
                    Some(len),
                ))
                .unwrap())
        }
    }

    fn make_lister(path: &str, transport: ScriptedListTransport) -> oio::PageLister<UpyunLister> {
        let core = Arc::new(UpyunCore {
            info: ServiceInfo::new(UPYUN_SCHEME, "/", ""),
            capability: Capability::default(),
            root: "/".to_string(),
            operator: "op".to_string(),
            bucket: "bucket".to_string(),
            signer: UpyunSigner {
                operator: "op".to_string(),
                password: "secret".to_string(),
            },
        });
        let ctx = OperationContext::new().with_http_transport(HttpTransporter::new(transport));
        oio::PageLister::new(UpyunLister::new(core, ctx, path, None))
    }

    #[tokio::test]
    async fn next_page_terminates_on_empty_iter() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let transport = ScriptedListTransport::new(vec![(
                StatusCode::OK,
                r#"{"iter":"","files":[]}"#.to_string(),
            )]);
            let mut lister = make_lister("/", transport.clone());

            let mut entries = Vec::new();
            while let Some(entry) = lister.next().await.unwrap() {
                entries.push(entry.path().to_string());
            }

            assert!(entries.is_empty());
            assert_eq!(transport.request_count(), 1);
        })
        .await
        .expect("test timed out");
    }

    #[tokio::test]
    async fn next_page_terminates_on_repeating_iter() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let page1 = r#"{"iter":"cursor-1","files":[{"type":"text/plain","name":"a.txt","length":3,"last_modified":1700000000}]}"#.to_string();
            let page2 = r#"{"iter":"cursor-1","files":[{"type":"text/plain","name":"b.txt","length":4,"last_modified":1700000001}]}"#.to_string();
            let transport =
                ScriptedListTransport::new(vec![(StatusCode::OK, page1), (StatusCode::OK, page2)]);
            let mut lister = make_lister("/", transport.clone());

            let mut entries = Vec::new();
            while let Some(entry) = lister.next().await.unwrap() {
                entries.push(entry.path().to_string());
            }

            assert_eq!(entries, vec!["a.txt", "b.txt"]);
            assert_eq!(transport.request_count(), 2);
        })
        .await
        .expect("test timed out");
    }

    #[tokio::test]
    async fn next_page_handles_eof_iter_and_not_found() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let page1 = r#"{"iter":"cursor-1","files":[{"type":"folder","name":"sub","length":0,"last_modified":1700000000}]}"#.to_string();
            let page2 = r#"{"iter":"g2gCZAAEbmV4dGQAA2VvZg","files":[{"type":"text/plain","name":"c.txt","length":5,"last_modified":1700000002}]}"#.to_string();
            let transport =
                ScriptedListTransport::new(vec![(StatusCode::OK, page1), (StatusCode::OK, page2)]);
            let mut lister = make_lister("/", transport.clone());

            let mut entries = Vec::new();
            while let Some(entry) = lister.next().await.unwrap() {
                entries.push(entry.path().to_string());
            }
            assert_eq!(entries, vec!["sub/", "c.txt"]);
            assert_eq!(transport.request_count(), 2);

            let not_found_transport =
                ScriptedListTransport::new(vec![(StatusCode::NOT_FOUND, String::new())]);
            let mut not_found_lister = make_lister("missing/", not_found_transport.clone());
            assert!(not_found_lister.next().await.unwrap().is_none());
            assert_eq!(not_found_transport.request_count(), 1);
        })
        .await
        .expect("test timed out");
    }
}
