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

use super::backend::*;
use super::core::ErrorContext;
use super::core::constants::X_GOOG_GENERATION;
use super::core::parse_error;
use http::Response;
use http::StatusCode;
use opendal_core::raw::*;
use opendal_core::*;

/// Reader returned by this backend.
pub struct GcsReader {
    backend: GcsBackend,
    // StreamReader opens requests lazily, so keep the per-operation context
    // available for later range requests.
    ctx: OperationContext,
    path: String,
    args: OpRead,
}

impl GcsReader {
    pub(super) fn new(
        backend: GcsBackend,
        ctx: OperationContext,
        path: &str,
        args: OpRead,
    ) -> Self {
        Self {
            backend,
            ctx,
            path: path.to_string(),
            args,
        }
    }
}

impl oio::StreamRead for GcsReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let args = self.args.clone();
        let error_ctx = ErrorContext::new(ServiceOperation("GetObject"))
            .with_caller_condition(args.is_conditional());
        let resp = backend
            .core
            .gcs_get_object(&self.ctx, path, range, &args)
            .await?;

        let status = resp.status();

        let (rp, stream) = match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                let mut metadata = parse_into_metadata(path, resp.headers())?.into_builder();
                // GCS sends the JSON API etag unquoted, as stat and list report
                // it, and matches only that form in preconditions. Some
                // emulators quote it.
                if let Some(etag) = parse_etag(resp.headers())? {
                    let etag = etag
                        .strip_prefix('"')
                        .and_then(|etag| etag.strip_suffix('"'))
                        .unwrap_or(etag);
                    metadata.etag(etag);
                }
                if let Some(generation) = parse_header_to_str(resp.headers(), X_GOOG_GENERATION)? {
                    metadata.version(generation);
                }
                (RpRead::new(metadata.build()), resp.into_body())
            }
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                return Err(parse_error(error_ctx, Response::from_parts(part, buf)));
            }
        };

        Ok((rp, Box::new(stream) as Box<dyn oio::ReadStreamDyn>))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::stream;
    use http::header::CONTENT_RANGE;
    use http::header::ETAG;
    use opendal_core::raw::oio::StreamRead;
    use reqsign_core::Context;
    use reqsign_core::ProvideCredentialChain;
    use reqsign_google::RequestSigner;
    use reqsign_google::TokenCredentialProvider;

    use super::super::core::GcsCore;
    use super::*;

    struct EtagTransport(&'static str);

    impl HttpTransport for EtagTransport {
        async fn fetch(&self, _: http::Request<Buffer>) -> Result<Response<HttpBody>> {
            let body = Buffer::from("data");
            Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header(CONTENT_RANGE, "bytes 0-3/4")
                .header(ETAG, self.0)
                .header(X_GOOG_GENERATION, "7")
                .body(HttpBody::new(stream::iter([Ok(body)]), Some(4)))
                .map_err(new_request_build_error)
        }
    }

    async fn read_etag(header: &'static str) -> Option<String> {
        let backend = GcsBackend {
            core: Arc::new(GcsCore {
                info: ServiceInfo::new("gcs", "/", "test-bucket"),
                capability: Capability::default(),
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
            }),
        };
        let ctx = OperationContext::default()
            .with_http_transport(HttpTransporter::new(EtagTransport(header)));
        let reader = GcsReader::new(backend, ctx, "object", OpRead::new());
        let (rp, _) = reader.open(BytesRange::new(0, Some(4))).await.unwrap();
        rp.into_metadata()
            .and_then(|meta| meta.etag().map(str::to_string))
    }

    #[tokio::test]
    async fn test_read_etag_matches_json_api_form() {
        // GCS sends the JSON API resource etag unquoted, as stat and list report it.
        assert_eq!(
            read_etag("CKWasoTgyPkCEAE=").await.as_deref(),
            Some("CKWasoTgyPkCEAE=")
        );
        // Emulators such as fake-gcs-server quote it.
        assert_eq!(
            read_etag("\"CKWasoTgyPkCEAE=\"").await.as_deref(),
            Some("CKWasoTgyPkCEAE=")
        );
    }
}
