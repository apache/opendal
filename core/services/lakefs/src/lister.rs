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

use super::core::LakefsListResponse;
use super::core::parse_error;
use super::core::{ErrorContext, LakefsCore};

pub struct LakefsLister {
    core: Arc<LakefsCore>,
    ctx: OperationContext,
    path: String,
    delimiter: &'static str,
    amount: Option<usize>,
    after: Option<String>,
}

impl LakefsLister {
    pub fn new(
        core: Arc<LakefsCore>,
        ctx: OperationContext,
        path: String,
        amount: Option<usize>,
        after: Option<&str>,
        recursive: bool,
    ) -> Self {
        let delimiter = if recursive { "" } else { "/" };
        Self {
            core,
            ctx,
            path,
            delimiter,
            amount,
            after: after.map(String::from),
        }
    }
}

impl oio::PageList for LakefsLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        // start_after applies to the first page; later pages resume from the
        // cursor the previous response returned.
        let request_after = if ctx.token.is_empty() {
            self.after.clone()
        } else {
            Some(ctx.token.clone())
        };

        let response = self
            .core
            .list_objects(
                &self.ctx,
                &self.path,
                self.delimiter,
                &self.amount,
                request_after.clone(),
            )
            .await?;

        let status_code = response.status();
        if !status_code.is_success() {
            let error = parse_error(ErrorContext::new(ServiceOperation("ListObjects")), response);
            return Err(error);
        }

        let bytes = response.into_body();

        let decoded_response: LakefsListResponse =
            serde_json::from_reader(bytes.reader()).map_err(new_json_deserialize_error)?;

        let pagination = decoded_response.pagination;
        ctx.done = !pagination.has_more
            || pagination.next_offset.is_empty()
            || request_after.as_deref() == Some(pagination.next_offset.as_str());
        ctx.token = pagination.next_offset;

        for status in decoded_response.results {
            let entry_type = match status.path_type.as_str() {
                "common_prefix" => EntryMode::DIR,
                "object" => EntryMode::FILE,
                _ => EntryMode::Unknown,
            };

            let path = if entry_type == EntryMode::DIR && !status.path.ends_with('/') {
                format!("{}/", status.path)
            } else {
                status.path
            };

            let mut rel_path = if path == self.core.root {
                "/".to_string()
            } else {
                build_relative_path(&self.core.root, &path)
            };
            if rel_path.is_empty() {
                rel_path = "/".to_string();
            }

            let mut meta = if rel_path.ends_with('/') {
                MetadataBuilder::dir()
            } else {
                match entry_type {
                    EntryMode::FILE => status
                        .size_bytes
                        .map_or_else(MetadataBuilder::unknown, MetadataBuilder::file),
                    EntryMode::DIR => MetadataBuilder::dir(),
                    EntryMode::Unknown => MetadataBuilder::unknown(),
                }
            };

            if status.mtime != 0
                && let Ok(ts) = Timestamp::from_second(status.mtime)
            {
                meta.last_modified(ts);
            }

            ctx.entries
                .push_back(oio::Entry::with(rel_path, meta.build()));
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
    use http::Response;
    use http::StatusCode;
    use opendal_core::raw::oio::List as _;

    use super::*;
    use crate::LAKEFS_SCHEME;

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

    struct ArcTransport(Arc<ScriptedListTransport>);

    impl HttpTransport for ArcTransport {
        async fn fetch(&self, req: http::Request<Buffer>) -> Result<Response<HttpBody>> {
            self.0.seen_uris.lock().unwrap().push(req.uri().to_string());
            let body_str = self
                .0
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| {
                    r#"{"pagination":{"has_more":false,"max_per_page":100,"next_offset":"","results":0},"results":[]}"#.to_string()
                });
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
        root: &str,
        path: &str,
        after: Option<&str>,
        recursive: bool,
        responses: Vec<&str>,
    ) -> (oio::PageLister<LakefsLister>, Arc<ScriptedListTransport>) {
        let root = normalize_root(root);
        let core = Arc::new(LakefsCore {
            info: ServiceInfo::new(LAKEFS_SCHEME, &root, "repo"),
            capability: Capability {
                list: true,
                ..Default::default()
            },
            endpoint: "http://127.0.0.1:8000".to_string(),
            repository: "repo".to_string(),
            branch: "main".to_string(),
            root,
            username: "user".to_string(),
            password: "pass".to_string(),
        });
        let transport = Arc::new(ScriptedListTransport::new(responses));
        let ctx = OperationContext::default()
            .with_http_transport(HttpTransporter::new(ArcTransport(transport.clone())));
        let lister = oio::PageLister::new(LakefsLister::new(
            core,
            ctx,
            path.to_string(),
            None,
            after,
            recursive,
        ));
        (lister, transport)
    }

    async fn collect_entries(
        lister: &mut oio::PageLister<LakefsLister>,
    ) -> Result<Vec<oio::Entry>> {
        let mut entries = Vec::new();
        while let Some(entry) = lister.next().await? {
            entries.push(entry);
        }
        Ok(entries)
    }

    #[tokio::test]
    async fn next_page_handles_out_of_range_mtime_without_panicking() -> Result<()> {
        let page = format!(
            r#"{{
                "pagination": {{"has_more": false, "max_per_page": 100, "next_offset": "", "results": 4}},
                "results": [
                    {{"path": "valid.txt", "path_type": "object", "physical_address": "p1", "checksum": "c1", "size_bytes": 10, "mtime": 1700000000}},
                    {{"path": "zero.txt", "path_type": "object", "physical_address": "p2", "checksum": "c2", "size_bytes": 20, "mtime": 0}},
                    {{"path": "max.txt", "path_type": "object", "physical_address": "p3", "checksum": "c3", "size_bytes": 30, "mtime": {}}},
                    {{"path": "min.txt", "path_type": "object", "physical_address": "p4", "checksum": "c4", "size_bytes": 40, "mtime": {}}}
                ]
            }}"#,
            i64::MAX,
            i64::MIN
        );

        let (mut lister, _) = build_lister("/", "", None, false, vec![&page]);
        let entries = tokio::time::timeout(Duration::from_secs(5), collect_entries(&mut lister))
            .await
            .expect("listing must not hang")?;

        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0].path(), "valid.txt");
        assert_eq!(
            entries[0].metadata().last_modified(),
            Some(Timestamp::from_second(1_700_000_000)?)
        );
        assert_eq!(entries[1].path(), "zero.txt");
        assert_eq!(entries[1].metadata().last_modified(), None);
        assert_eq!(entries[2].path(), "max.txt");
        assert_eq!(entries[2].metadata().last_modified(), None);
        assert_eq!(entries[3].path(), "min.txt");
        assert_eq!(entries[3].metadata().last_modified(), None);
        Ok(())
    }

    #[tokio::test]
    async fn next_page_terminates_when_has_more_has_empty_next_offset() -> Result<()> {
        let page = r#"{
            "pagination": {"has_more": true, "max_per_page": 100, "next_offset": "", "results": 1},
            "results": [
                {"path": "a.txt", "path_type": "object", "physical_address": "p1", "checksum": "c1", "size_bytes": 1, "mtime": 0}
            ]
        }"#;

        let (mut lister, transport) = build_lister("/", "", None, false, vec![page, page]);
        let entries = tokio::time::timeout(Duration::from_secs(5), collect_entries(&mut lister))
            .await
            .expect("listing must terminate instead of looping infinitely")?;

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path(), "a.txt");
        assert_eq!(transport.seen_uris.lock().unwrap().len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn next_page_terminates_when_has_more_repeats_cursor_and_paginates_normally() -> Result<()>
    {
        let page1 = r#"{
            "pagination": {"has_more": true, "max_per_page": 1, "next_offset": "a.txt", "results": 1},
            "results": [
                {"path": "a.txt", "path_type": "object", "physical_address": "p1", "checksum": "c1", "size_bytes": 1, "mtime": 0}
            ]
        }"#;
        let stuck_page2 = r#"{
            "pagination": {"has_more": true, "max_per_page": 1, "next_offset": "a.txt", "results": 1},
            "results": [
                {"path": "b.txt", "path_type": "object", "physical_address": "p2", "checksum": "c2", "size_bytes": 2, "mtime": 0}
            ]
        }"#;

        let (mut lister, transport) =
            build_lister("/", "", None, false, vec![page1, stuck_page2, stuck_page2]);
        let entries = tokio::time::timeout(Duration::from_secs(5), collect_entries(&mut lister))
            .await
            .expect("listing must terminate when cursor does not advance")?;

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].path(), "a.txt");
        assert_eq!(entries[1].path(), "b.txt");
        let uris = transport.seen_uris.lock().unwrap();
        assert_eq!(uris.len(), 2);
        assert!(uris[1].contains("after=a.txt"));
        Ok(())
    }

    #[tokio::test]
    async fn next_page_avoids_double_slash_common_prefix_and_normalizes_root() -> Result<()> {
        let page = r#"{
            "pagination": {"has_more": false, "max_per_page": 100, "next_offset": "", "results": 5},
            "results": [
                {"path": "prefix/dir1/", "path_type": "common_prefix", "physical_address": "", "checksum": "", "size_bytes": null, "mtime": 0},
                {"path": "prefix/dir2", "path_type": "common_prefix", "physical_address": "", "checksum": "", "size_bytes": null, "mtime": 0},
                {"path": "prefix/", "path_type": "common_prefix", "physical_address": "", "checksum": "", "size_bytes": null, "mtime": 0},
                {"path": "/prefix/", "path_type": "common_prefix", "physical_address": "", "checksum": "", "size_bytes": null, "mtime": 0},
                {"path": "prefix/", "path_type": "object", "physical_address": "p", "checksum": "c", "size_bytes": 0, "mtime": 0}
            ]
        }"#;

        let (mut lister, _) = build_lister("/prefix/", "", None, false, vec![page]);
        let entries = tokio::time::timeout(Duration::from_secs(5), collect_entries(&mut lister))
            .await
            .expect("listing must not hang")?;

        let paths_and_modes: Vec<(&str, EntryMode)> = entries
            .iter()
            .map(|e| (e.path(), e.metadata().mode()))
            .collect();
        assert_eq!(
            paths_and_modes,
            vec![
                ("dir1/", EntryMode::DIR),
                ("dir2/", EntryMode::DIR),
                ("/", EntryMode::DIR),
                ("/", EntryMode::DIR),
                ("/", EntryMode::DIR),
            ]
        );
        Ok(())
    }
}
