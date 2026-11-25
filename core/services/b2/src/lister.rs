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

use super::core::ListFileNamesResponse;
use super::core::parse_error;
use super::core::parse_file_info;
use super::core::{B2Core, ErrorContext};
use opendal_core::raw::*;
use opendal_core::*;

pub struct B2Lister {
    core: Arc<B2Core>,
    ctx: OperationContext,

    path: String,
    delimiter: Option<&'static str>,
    limit: Option<usize>,

    /// B2 starts listing **after** this specified key
    start_after: Option<String>,
}

impl B2Lister {
    pub fn new(
        core: Arc<B2Core>,
        ctx: OperationContext,
        path: &str,
        recursive: bool,
        limit: Option<usize>,
        start_after: Option<&str>,
    ) -> Self {
        let delimiter = if recursive { None } else { Some("/") };
        Self {
            core,
            ctx,

            path: path.to_string(),
            delimiter,
            limit,
            start_after: start_after.map(String::from),
        }
    }
}

impl oio::PageList for B2Lister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let resp = self
            .core
            .list_file_names(
                &self.ctx,
                Some(&self.path),
                self.delimiter,
                self.limit,
                if ctx.token.is_empty() {
                    self.start_after
                        .as_ref()
                        .map(|v| build_absolute_path(&self.core.root, v))
                } else {
                    Some(ctx.token.clone())
                },
            )
            .await?;

        if resp.status() != http::StatusCode::OK {
            return Err(parse_error(
                ErrorContext::new(ServiceOperation("ListFileNames")),
                resp,
            ));
        }

        let bs = resp.into_body();

        let output: ListFileNamesResponse =
            serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

        match output.next_file_name {
            Some(token) if !token.is_empty() => {
                ctx.token = token;
            }
            _ => {
                ctx.done = true;
                ctx.token.clear();
            }
        }

        for file in output.files {
            if let Some(start_after) = self.start_after.clone()
                && build_absolute_path(&self.core.root, &start_after) == file.file_name
            {
                continue;
            }
            let metadata = parse_file_info(&file);
            let path = if file.file_name == self.core.root {
                "/".to_string()
            } else {
                build_relative_path(&self.core.root, &file.file_name)
            };

            ctx.entries.push_back(oio::Entry::new(&path, metadata));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use futures::stream;
    use http::Request;
    use http::Response;
    use http::StatusCode;
    use opendal_core::raw::oio::List as _;

    use super::*;
    use crate::core::tests::test_core;

    struct ScriptedListTransport {
        pages: Mutex<Vec<String>>,
        requests: Arc<Mutex<Vec<String>>>,
    }

    impl HttpTransport for ScriptedListTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            self.requests.lock().unwrap().push(req.uri().to_string());
            let mut pages = self.pages.lock().unwrap();
            assert!(!pages.is_empty(), "unexpected extra list page request");
            let body = pages.remove(0);
            let buf = Buffer::from(body);
            let len = buf.len() as u64;
            Response::builder()
                .status(StatusCode::OK)
                .body(HttpBody::new(stream::iter([Ok(buf)]), Some(len)))
                .map_err(new_request_build_error)
        }
    }

    #[tokio::test]
    async fn test_lister_stops_on_empty_next_file_name() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedListTransport {
            pages: Mutex::new(vec![
                r#"{
                    "files": [
                        {"fileId": "4_z1", "fileName": "a.txt", "contentLength": 1}
                    ],
                    "nextFileName": ""
                }"#
                .to_string(),
            ]),
            requests: requests.clone(),
        };
        let core = test_core("/");
        let ctx = OperationContext::default().with_http_transport(HttpTransporter::new(transport));
        let mut lister = oio::PageLister::new(B2Lister::new(core, ctx, "", false, None, None));

        let mut entries = Vec::new();
        while let Some(entry) = lister.next().await.unwrap() {
            entries.push(entry);
        }

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path(), "a.txt");
        assert_eq!(requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_lister_normalizes_root_entry_to_slash() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedListTransport {
            pages: Mutex::new(vec![
                r#"{
                    "files": [
                        {"fileId": "4_z0", "fileName": "sub/", "contentLength": 0},
                        {"fileId": "4_z1", "fileName": "sub/a.txt", "contentLength": 5},
                        {"fileId": null, "fileName": "sub/dir/", "contentLength": 0}
                    ],
                    "nextFileName": null
                }"#
                .to_string(),
            ]),
            requests,
        };
        let core = test_core("/sub/");
        let ctx = OperationContext::default().with_http_transport(HttpTransporter::new(transport));
        let mut lister = oio::PageLister::new(B2Lister::new(core, ctx, "", false, None, None));

        let mut entries = Vec::new();
        while let Some(entry) = lister.next().await.unwrap() {
            entries.push((entry.path().to_string(), entry.metadata().mode()));
        }

        assert_eq!(
            entries,
            vec![
                ("/".to_string(), EntryMode::DIR),
                ("a.txt".to_string(), EntryMode::FILE),
                ("dir/".to_string(), EntryMode::DIR),
            ]
        );
    }

    #[tokio::test]
    async fn test_lister_paginates_non_empty_next_file_name() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedListTransport {
            pages: Mutex::new(vec![
                r#"{
                    "files": [{"fileId": "4_z1", "fileName": "a.txt", "contentLength": 1}],
                    "nextFileName": "b.txt"
                }"#
                .to_string(),
                r#"{
                    "files": [{"fileId": "4_z2", "fileName": "b.txt", "contentLength": 2}],
                    "nextFileName": ""
                }"#
                .to_string(),
            ]),
            requests: requests.clone(),
        };
        let core = test_core("/");
        let ctx = OperationContext::default().with_http_transport(HttpTransporter::new(transport));
        let mut lister = oio::PageLister::new(B2Lister::new(core, ctx, "", false, None, None));

        let mut paths = Vec::new();
        while let Some(entry) = lister.next().await.unwrap() {
            paths.push(entry.path().to_string());
        }

        assert_eq!(paths, vec!["a.txt", "b.txt"]);
        let recorded = requests.lock().unwrap();
        assert_eq!(recorded.len(), 2);
        assert!(recorded[1].contains("startFileName=b.txt"));
    }
}
