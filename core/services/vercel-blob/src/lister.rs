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

use super::core::VercelBlobCore;
use super::core::parse_blob;
use opendal_core::OperationContext;
use opendal_core::Result;
use opendal_core::raw::oio::Entry;
use opendal_core::raw::*;

pub struct VercelBlobLister {
    core: Arc<VercelBlobCore>,
    ctx: OperationContext,

    path: String,
    limit: Option<usize>,
}

impl VercelBlobLister {
    pub(super) fn new(
        core: Arc<VercelBlobCore>,
        ctx: OperationContext,
        path: &str,
        limit: Option<usize>,
    ) -> Self {
        VercelBlobLister {
            core,
            ctx,
            path: path.to_string(),
            limit,
        }
    }
}

impl oio::PageList for VercelBlobLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let p = build_absolute_path(&self.core.root, &self.path);

        let cursor = if ctx.token.is_empty() {
            None
        } else {
            Some(ctx.token.as_str())
        };

        let resp = self.core.list(&self.ctx, &p, self.limit, cursor).await?;

        if !resp.has_more {
            ctx.done = true;
        } else {
            match resp.cursor {
                Some(cursor) if !cursor.is_empty() && cursor != ctx.token => {
                    ctx.token = cursor;
                }
                // More pages were reported, but the cursor is missing, empty, or did not advance.
                // Stopping truncates the listing; the alternative is re-requesting the same page forever.
                _ => ctx.done = true,
            }
        }

        for blob in resp.blobs {
            let path = build_relative_path(&self.core.root, &blob.pathname);

            if path == self.path {
                continue;
            }

            let md = parse_blob(&blob)?;

            ctx.entries.push_back(Entry::new(&path, md));
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
    use opendal_core::{Buffer, Capability, HttpBody, HttpTransport, HttpTransporter};

    use super::*;
    use crate::VERCEL_BLOB_SCHEME;

    #[derive(Clone)]
    struct ScriptedListTransport {
        responses: Arc<Mutex<VecDeque<String>>>,
        requests: Arc<Mutex<Vec<String>>>,
    }

    impl ScriptedListTransport {
        fn new(responses: Vec<String>) -> Self {
            Self {
                responses: Arc::new(Mutex::new(responses.into())),
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn requests(&self) -> Vec<String> {
            self.requests.lock().unwrap().clone()
        }
    }

    impl HttpTransport for ScriptedListTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            self.requests.lock().unwrap().push(req.uri().to_string());
            let body = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected extra list request (possible infinite pagination loop)");
            let len = body.len() as u64;
            Ok(Response::builder()
                .status(StatusCode::OK)
                .body(HttpBody::new(
                    stream::iter([Ok(Buffer::from(body))]),
                    Some(len),
                ))
                .unwrap())
        }
    }

    fn make_lister(
        path: &str,
        transport: ScriptedListTransport,
    ) -> oio::PageLister<VercelBlobLister> {
        let core = Arc::new(VercelBlobCore {
            info: ServiceInfo::new(VERCEL_BLOB_SCHEME, "/", ""),
            capability: Capability::default(),
            root: "/".to_string(),
            token: "test-token".to_string(),
        });
        let ctx = OperationContext::new().with_http_transport(HttpTransporter::new(transport));
        oio::PageLister::new(VercelBlobLister::new(core, ctx, path, None))
    }

    fn blob_json(pathname: &str, size: u64) -> String {
        format!(
            r#"{{"url":"https://store.public.blob.vercel-storage.com/{pathname}","pathname":"{pathname}","size":{size},"uploadedAt":"2026-01-01T00:00:00Z","contentDisposition":"inline"}}"#
        )
    }

    #[tokio::test]
    async fn next_page_terminates_on_empty_cursor() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let page = format!(
                r#"{{"hasMore":true,"cursor":"","blobs":[{}]}}"#,
                blob_json("a.txt", 3)
            );
            let transport = ScriptedListTransport::new(vec![page]);
            let mut lister = make_lister("/", transport.clone());

            let mut entries = Vec::new();
            while let Some(entry) = lister.next().await.unwrap() {
                entries.push(entry.path().to_string());
            }

            assert_eq!(entries, vec!["a.txt"]);
            assert_eq!(transport.requests().len(), 1);
        })
        .await
        .expect("test timed out");
    }

    #[tokio::test]
    async fn next_page_terminates_on_repeating_cursor() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let page1 = format!(
                r#"{{"hasMore":true,"cursor":"tok1","blobs":[{}]}}"#,
                blob_json("a.txt", 3)
            );
            let page2 = format!(
                r#"{{"hasMore":true,"cursor":"tok1","blobs":[{}]}}"#,
                blob_json("b.txt", 4)
            );
            let transport = ScriptedListTransport::new(vec![page1, page2]);
            let mut lister = make_lister("/", transport.clone());

            let mut entries = Vec::new();
            while let Some(entry) = lister.next().await.unwrap() {
                entries.push(entry.path().to_string());
            }

            assert_eq!(entries, vec!["a.txt", "b.txt"]);
            assert_eq!(transport.requests().len(), 2);
        })
        .await
        .expect("test timed out");
    }
}
