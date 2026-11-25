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

/// GcsLister takes over task of listing objects and
/// helps walking directory
pub struct GcsLister {
    core: Arc<GcsCore>,
    ctx: OperationContext,

    path: String,
    args: OpList,
}

impl GcsLister {
    /// Generate a new directory walker
    pub fn new(core: Arc<GcsCore>, ctx: OperationContext, path: &str, args: OpList) -> Self {
        Self {
            core,
            ctx,
            path: path.to_string(),
            args,
        }
    }
}

impl oio::PageList for GcsLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let resp = self
            .core
            .gcs_list_objects(&self.ctx, &self.path, &ctx.token, &self.args)
            .await?;

        if !resp.status().is_success() {
            return Err(parse_error(
                ErrorContext::new(ServiceOperation("ListObjects")),
                resp,
            ));
        }
        let bytes = resp.into_body();

        let output: ListResponse =
            serde_json::from_reader(bytes.reader()).map_err(new_json_deserialize_error)?;

        if let Some(token) = output.next_page_token.as_deref().filter(|t| !t.is_empty()) {
            ctx.token = token.to_string();
        } else {
            ctx.token.clear();
            ctx.done = true;
        }

        for prefix in output.prefixes {
            let de = oio::Entry::new(
                &build_relative_path(&self.core.root, &prefix),
                MetadataBuilder::dir().build(),
            );

            ctx.entries.push_back(de);
        }

        for object in output.items {
            // exclude the inclusive start_after itself
            let mut path = build_relative_path(&self.core.root, &object.name);
            if path.is_empty() {
                path = "/".to_string();
            }
            if self.args.start_after() == Some(path.as_str()) {
                continue;
            }

            let size = object.size.parse().map_err(|e| {
                Error::new(ErrorKind::Unexpected, "parse u64 from list response").set_source(e)
            })?;
            let mut meta = if path.ends_with('/') {
                MetadataBuilder::dir()
            } else {
                MetadataBuilder::file(size)
            };

            // set metadata fields
            meta.content_md5(object.md5_hash.as_str());
            meta.etag(object.etag.as_str());
            if !object.generation.is_empty() {
                meta.version(&object.generation);
            }

            if !object.content_type.is_empty() {
                meta.content_type(&object.content_type);
            }

            meta.last_modified(object.updated.parse::<Timestamp>()?);

            let de = oio::Entry::with(path, meta.build());

            ctx.entries.push_back(de);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use futures::stream;
    use http::Response;
    use http::StatusCode;
    use reqsign_core::Context;
    use reqsign_core::ProvideCredentialChain;
    use reqsign_google::RequestSigner;
    use reqsign_google::TokenCredentialProvider;

    use super::*;
    use opendal_core::raw::oio::PageList;

    struct StaticListTransport(String);

    impl HttpTransport for StaticListTransport {
        async fn fetch(&self, _: http::Request<Buffer>) -> Result<Response<HttpBody>> {
            let body = Buffer::from(self.0.clone());
            let size = body.len() as u64;
            Response::builder()
                .status(StatusCode::OK)
                .body(HttpBody::new(stream::iter([Ok(body)]), Some(size)))
                .map_err(new_request_build_error)
        }
    }

    fn lister_with_body(body: &str) -> GcsLister {
        let core = Arc::new(GcsCore {
            info: ServiceInfo::new("gcs", "/", "test-bucket"),
            capability: Capability {
                list: true,
                ..Default::default()
            },
            endpoint: "https://storage.googleapis.com".to_string(),
            bucket: "test-bucket".to_string(),
            root: "/".to_string(),
            sign_ctx: Context::new(),
            credential_provider: Arc::new(
                ProvideCredentialChain::new().push(TokenCredentialProvider::new("test-token")),
            ),
            request_signer: Arc::new(RequestSigner::new("storage")),
            signers: HttpTransportCache::new(),
            predefined_acl: None,
            default_storage_class: None,
            skip_signature: true,
        });
        let ctx = OperationContext::default()
            .with_http_transport(HttpTransporter::new(StaticListTransport(body.to_string())));
        GcsLister::new(core, ctx, "", OpList::default())
    }

    #[tokio::test]
    async fn next_page_stops_on_empty_page_token() {
        let lister = lister_with_body(
            r#"{
                "kind": "storage#objects",
                "nextPageToken": "",
                "items": [
                    {
                        "name": "composite.bin",
                        "size": "42",
                        "generation": "1",
                        "updated": "2022-08-15T11:33:34.866Z"
                    },
                    {
                        "name": "regular.bin",
                        "size": "12",
                        "md5Hash": "fHcEH1vPwA6eTPqxuasXcg==",
                        "etag": "CKWasoTgyPkCEAE=",
                        "generation": "2",
                        "updated": "2022-08-15T11:33:34.866Z"
                    }
                ]
            }"#,
        );

        let mut page_ctx = oio::PageContext {
            done: false,
            token: "previous-token".to_string(),
            entries: VecDeque::new(),
        };
        lister
            .next_page(&mut page_ctx)
            .await
            .expect("listing page should succeed");

        assert!(page_ctx.done);
        assert!(page_ctx.token.is_empty());
        assert_eq!(page_ctx.entries.len(), 2);

        let composite = page_ctx.entries.pop_front().unwrap();
        assert_eq!(composite.path(), "composite.bin");
        assert_eq!(composite.metadata().content_md5(), Some(""));
        assert_eq!(composite.metadata().etag(), Some(""));

        let regular = page_ctx.entries.pop_front().unwrap();
        assert_eq!(regular.path(), "regular.bin");
        assert_eq!(
            regular.metadata().content_md5(),
            Some("fHcEH1vPwA6eTPqxuasXcg==")
        );
        assert_eq!(regular.metadata().etag(), Some("CKWasoTgyPkCEAE="));
    }

    #[tokio::test]
    async fn next_page_preserves_non_empty_page_token() {
        let lister = lister_with_body(
            r#"{
                "kind": "storage#objects",
                "nextPageToken": "page-2",
                "items": []
            }"#,
        );

        let mut page_ctx = oio::PageContext {
            done: false,
            token: String::new(),
            entries: VecDeque::new(),
        };
        lister
            .next_page(&mut page_ctx)
            .await
            .expect("listing page should succeed");

        assert!(!page_ctx.done);
        assert_eq!(page_ctx.token, "page-2");
    }
}
