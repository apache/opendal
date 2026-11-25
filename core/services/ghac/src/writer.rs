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

use std::str::FromStr;
use std::sync::Arc;

use super::core::parse_error;
use super::core::*;
use opendal_core::raw::*;
use opendal_core::*;
use opendal_service_azblob::core::AzblobCore;
use opendal_service_azblob::writer::AzblobWriter;
use reqsign_azure_storage::RequestSigner;
use reqsign_azure_storage::StaticCredentialProvider;
use reqsign_core::Context;

pub struct GhacWriter(pub TwoWays<GhacWriterV1, GhacWriterV2>);

impl GhacWriter {
    /// TODO: maybe we can move the signed url logic to azblob service instead.
    pub fn new(
        core: Arc<GhacCore>,
        ctx: OperationContext,
        executor: Executor,
        write_path: String,
        url: String,
    ) -> Result<Self> {
        match core.service_version {
            GhacVersion::V1 => Ok(GhacWriter(TwoWays::One(GhacWriterV1 {
                core,
                ctx,
                path: write_path,
                url,
                size: 0,
            }))),
            GhacVersion::V2 => {
                let uri = http::Uri::from_str(&url)
                    .map_err(new_http_uri_invalid_error)?
                    .into_parts();
                let (Some(scheme), Some(authority), Some(pq)) =
                    (uri.scheme, uri.authority, uri.path_and_query)
                else {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        "ghac returns invalid signed url",
                    )
                    .with_context("url", HttpUri::new(url).redacted_uri()));
                };
                let endpoint = format!("{scheme}://{authority}");
                let Some((container, path)) = pq.path().trim_matches('/').split_once("/") else {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        "ghac returns invalid signed url that bucket or path is missing",
                    )
                    .with_context("url", HttpUri::new(url).redacted_uri()));
                };
                let Some(query) = pq.query() else {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        "ghac returns invalid signed url that sas is missing",
                    )
                    .with_context("url", HttpUri::new(url).redacted_uri()));
                };
                let azure_core = Arc::new(AzblobCore {
                    info: ServiceInfo::new("azblob", "/", container),
                    capability: Capability {
                        stat: true,
                        stat_with_if_match: true,
                        stat_with_if_none_match: true,

                        read: true,

                        read_with_if_match: true,
                        read_with_if_none_match: true,
                        read_with_override_content_disposition: true,
                        read_with_if_modified_since: true,
                        read_with_if_unmodified_since: true,

                        write: true,
                        write_can_append: true,
                        write_can_empty: true,
                        write_can_multi: true,
                        write_with_cache_control: true,
                        write_with_content_type: true,
                        write_with_if_not_exists: true,
                        write_with_if_none_match: true,
                        write_with_user_metadata: true,

                        copy: true,

                        list: true,
                        list_with_recursive: true,

                        shared: true,

                        ..Default::default()
                    },
                    container: container.to_string(),
                    root: "/".to_string(),
                    endpoint,
                    encryption_key: None,
                    encryption_key_sha256: None,
                    encryption_algorithm: None,
                    skip_signature: false,
                    sign_ctx: Context::new(),
                    credential_provider: Arc::new(StaticCredentialProvider::new_sas_token(query)),
                    request_signer: Arc::new(RequestSigner::new()),
                    signers: HttpTransportCache::new(),
                });
                let w = AzblobWriter::new(
                    azure_core,
                    ctx.clone(),
                    OpWrite::default(),
                    percent_decode_path(path),
                );
                let writer = oio::BlockWriter::new(executor, w, 4);
                Ok(GhacWriter(TwoWays::Two(GhacWriterV2 {
                    core,
                    ctx,
                    writer,
                    path: write_path,
                    url,
                    size: 0,
                })))
            }
        }
    }
}

pub struct GhacLazyWriter {
    core: Arc<GhacCore>,
    ctx: OperationContext,
    executor: Executor,
    path: String,
    inner: Option<GhacWriter>,
}

impl GhacLazyWriter {
    pub fn new(
        core: Arc<GhacCore>,
        ctx: OperationContext,
        executor: Executor,
        path: String,
    ) -> Self {
        Self {
            core,
            ctx,
            executor,
            path,
            inner: None,
        }
    }

    async fn inner(&mut self) -> Result<&mut GhacWriter> {
        if self.inner.is_none() {
            let url = self.core.ghac_get_upload_url(&self.ctx, &self.path).await?;
            self.inner = Some(GhacWriter::new(
                self.core.clone(),
                self.ctx.clone(),
                self.executor.clone(),
                self.path.clone(),
                url,
            )?);
        }

        Ok(self.inner.as_mut().expect("writer must be initialized"))
    }
}

impl oio::Write for GhacLazyWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        self.inner().await?.write(bs).await
    }

    async fn abort(&mut self) -> Result<()> {
        match &mut self.inner {
            Some(w) => w.abort().await,
            None => Ok(()),
        }
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.inner().await?.close().await
    }
}

impl oio::Write for GhacWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        self.0.write(bs).await
    }

    async fn abort(&mut self) -> Result<()> {
        self.0.abort().await
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.0.close().await
    }
}

pub struct GhacWriterV1 {
    core: Arc<GhacCore>,
    ctx: OperationContext,

    path: String,
    url: String,
    size: u64,
}

impl oio::Write for GhacWriterV1 {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        if bs.is_empty() {
            return Ok(());
        }

        let size = bs.len() as u64;
        let offset = self.size;

        let resp = self
            .core
            .ghac_v1_write(&self.ctx, &self.url, size, offset, bs)
            .await?;
        if !resp.status().is_success() {
            return Err(
                parse_error(ErrorContext::new(ServiceOperation("UploadChunk")), resp)
                    .with_operation("Backend::ghac_upload"),
            );
        }
        self.size += size;
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.core
            .ghac_finalize_upload(&self.ctx, &self.path, &self.url, self.size)
            .await?;
        Ok({
            let mut metadata = MetadataBuilder::unknown();
            metadata.set_file(self.size);
            metadata.build()
        })
    }
}

pub struct GhacWriterV2 {
    core: Arc<GhacCore>,
    ctx: OperationContext,
    writer: oio::BlockWriter<AzblobWriter>,

    path: String,
    url: String,
    size: u64,
}

impl oio::Write for GhacWriterV2 {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let size = bs.len() as u64;

        self.writer.write(bs).await?;
        self.size += size;
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.writer.close().await?;
        self.core
            .ghac_finalize_upload(&self.ctx, &self.path, &self.url, self.size)
            .await?;
        Ok({
            let mut metadata = MetadataBuilder::unknown();
            metadata.set_file(self.size);
            metadata.build()
        })
    }

    async fn abort(&mut self) -> Result<()> {
        self.writer.abort().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use ::ghac::v1 as ghac_types;
    use bytes::Bytes;
    use http::Method;
    use http::Request;
    use http::Response;
    use http::StatusCode;
    use http::header::CONTENT_LENGTH;
    use http::header::CONTENT_RANGE;
    use opendal_core::raw::oio::Write;
    use prost::Message;
    use tokio::sync::oneshot;

    use super::*;

    struct TokioExecutor;

    impl Execute for TokioExecutor {
        fn execute(&self, f: BoxedStaticFuture<()>) {
            let _handle = tokio::task::spawn(f);
        }
    }

    fn test_core(service_version: GhacVersion) -> Arc<GhacCore> {
        Arc::new(GhacCore {
            info: ServiceInfo::new("ghac", "/", "test-version"),
            capability: Capability {
                stat: true,
                read: true,
                write: true,
                write_can_multi: true,
                shared: true,
                ..Default::default()
            },
            root: "/".to_string(),
            cache_url: "https://results.actions.githubusercontent.com".to_string(),
            catch_token: "test-token".to_string(),
            version: "test-version".to_string(),
            service_version,
        })
    }

    #[derive(Debug)]
    struct RecordedRequest {
        method: Method,
        uri: String,
        content_range: Option<String>,
        content_length: Option<String>,
        body: Bytes,
    }

    type Responder = Box<dyn Fn(&Request<Buffer>) -> (StatusCode, Bytes) + Send + Sync>;

    struct RecordingTransport {
        requests: Arc<Mutex<Vec<RecordedRequest>>>,
        responder: Responder,
    }

    impl HttpTransport for RecordingTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let (status, resp_body) = (self.responder)(&req);
            let method = req.method().clone();
            let uri = req.uri().to_string();
            let content_range = req
                .headers()
                .get(CONTENT_RANGE)
                .and_then(|v| v.to_str().ok())
                .map(ToOwned::to_owned);
            let content_length = req
                .headers()
                .get(CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .map(ToOwned::to_owned);
            let body = req.into_body().to_bytes();

            self.requests.lock().unwrap().push(RecordedRequest {
                method,
                uri,
                content_range,
                content_length,
                body,
            });

            let buf = Buffer::from(resp_body);
            let len = buf.len() as u64;
            Response::builder()
                .status(status)
                .body(HttpBody::new(futures::stream::iter([Ok(buf)]), Some(len)))
                .map_err(new_request_build_error)
        }
    }

    #[tokio::test]
    async fn test_ghac_v1_writer_skips_empty_writes() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let requests = Arc::new(Mutex::new(Vec::new()));
            let transport = RecordingTransport {
                requests: requests.clone(),
                responder: Box::new(|_| (StatusCode::NO_CONTENT, Bytes::new())),
            };
            let ctx = OperationContext::from_parts(
                HttpTransporter::new(transport),
                Executor::with(TokioExecutor),
            );
            let core = test_core(GhacVersion::V1);

            let mut writer = GhacWriter::new(
                core.clone(),
                ctx.clone(),
                ctx.executor().clone(),
                "cache-key".to_string(),
                "https://results.actions.githubusercontent.com/_apis/artifactcache/caches/7"
                    .to_string(),
            )
            .unwrap();

            // Empty write at offset 0 must not panic or send any HTTP request.
            writer.write(Buffer::new()).await.unwrap();
            assert!(requests.lock().unwrap().is_empty());

            writer.write(Buffer::from("abc")).await.unwrap();
            // Empty write at offset > 0 must not send an inverted Content-Range.
            writer.write(Buffer::new()).await.unwrap();
            writer.write(Buffer::from("de")).await.unwrap();

            let meta = writer.close().await.unwrap();
            assert_eq!(meta.content_length(), 5);

            let recorded = requests.lock().unwrap();
            assert_eq!(recorded.len(), 3);
            assert_eq!(recorded[0].method, Method::PATCH);
            assert_eq!(recorded[0].content_range.as_deref(), Some("bytes 0-2/*"));
            assert_eq!(recorded[0].content_length.as_deref(), Some("3"));
            assert_eq!(recorded[0].body, Bytes::from_static(b"abc"));

            assert_eq!(recorded[1].method, Method::PATCH);
            assert_eq!(recorded[1].content_range.as_deref(), Some("bytes 3-4/*"));
            assert_eq!(recorded[1].content_length.as_deref(), Some("2"));
            assert_eq!(recorded[1].body, Bytes::from_static(b"de"));

            assert_eq!(recorded[2].method, Method::POST);
            assert_eq!(recorded[2].body, Bytes::from_static(br#"{"size":5}"#));
        })
        .await
        .expect("test timed out");
    }

    #[tokio::test]
    async fn test_ghac_v1_writer_empty_file_commits_zero_size() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let requests = Arc::new(Mutex::new(Vec::new()));
            let transport = RecordingTransport {
                requests: requests.clone(),
                responder: Box::new(|_| (StatusCode::NO_CONTENT, Bytes::new())),
            };
            let ctx = OperationContext::from_parts(
                HttpTransporter::new(transport),
                Executor::with(TokioExecutor),
            );
            let core = test_core(GhacVersion::V1);

            let mut writer = GhacWriter::new(
                core,
                ctx.clone(),
                ctx.executor().clone(),
                "empty-key".to_string(),
                "https://results.actions.githubusercontent.com/_apis/artifactcache/caches/8"
                    .to_string(),
            )
            .unwrap();

            writer.write(Buffer::new()).await.unwrap();
            let meta = writer.close().await.unwrap();
            assert_eq!(meta.content_length(), 0);

            let recorded = requests.lock().unwrap();
            assert_eq!(recorded.len(), 1);
            assert_eq!(recorded[0].method, Method::POST);
            assert_eq!(recorded[0].body, Bytes::from_static(br#"{"size":0}"#));
        })
        .await
        .expect("test timed out");
    }

    #[tokio::test]
    async fn test_ghac_lazy_writer_abort_without_write_sends_no_requests() {
        tokio::time::timeout(Duration::from_secs(10), async {
            for version in [GhacVersion::V1, GhacVersion::V2] {
                let requests = Arc::new(Mutex::new(Vec::new()));
                let transport = RecordingTransport {
                    requests: requests.clone(),
                    responder: Box::new(|_| panic!("abort without write must not send requests")),
                };
                let ctx = OperationContext::from_parts(
                    HttpTransporter::new(transport),
                    Executor::with(TokioExecutor),
                );
                let mut writer = GhacLazyWriter::new(
                    test_core(version),
                    ctx.clone(),
                    ctx.executor().clone(),
                    "unused-key".to_string(),
                );

                writer.abort().await.unwrap();
                assert!(requests.lock().unwrap().is_empty());
            }
        })
        .await
        .expect("test timed out");
    }

    struct HangingBlockTransport {
        started: Mutex<Option<oneshot::Sender<()>>>,
        cancelled: Mutex<Option<oneshot::Sender<()>>>,
    }

    struct DropSignal(Option<oneshot::Sender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(tx) = self.0.take() {
                let _ = tx.send(());
            }
        }
    }

    impl HttpTransport for HangingBlockTransport {
        async fn fetch(&self, _req: Request<Buffer>) -> Result<Response<HttpBody>> {
            if let Some(tx) = self.started.lock().unwrap().take() {
                let _ = tx.send(());
            }
            let _guard = DropSignal(self.cancelled.lock().unwrap().take());
            std::future::pending::<Result<Response<HttpBody>>>().await
        }
    }

    #[tokio::test]
    async fn test_ghac_v2_writer_abort_cancels_inflight_block_upload() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let (started_tx, started_rx) = oneshot::channel();
            let (cancelled_tx, cancelled_rx) = oneshot::channel();
            let transport = HangingBlockTransport {
                started: Mutex::new(Some(started_tx)),
                cancelled: Mutex::new(Some(cancelled_tx)),
            };
            let ctx = OperationContext::from_parts(
                HttpTransporter::new(transport),
                Executor::with(TokioExecutor),
            );
            let mut writer = GhacWriter::new(
                test_core(GhacVersion::V2),
                ctx.clone(),
                ctx.executor().clone(),
                "cache-key".to_string(),
                "https://account.blob.core.windows.net/cache-container/archive.tzst?sv=2023-01-03&sig=test-sig"
                    .to_string(),
            )
            .unwrap();

            // First write fills BlockWriter's single-block cache; second write schedules
            // the first block onto the background executor.
            writer.write(Buffer::from("chunk-1")).await.unwrap();
            writer.write(Buffer::from("chunk-2")).await.unwrap();

            started_rx
                .await
                .expect("background block upload should start");

            writer.abort().await.unwrap();

            // Keep `writer` alive across this wait so cancellation proves `abort()` itself
            // tore down the in-flight BlockWriter task rather than `Drop`.
            cancelled_rx
                .await
                .expect("abort() must cancel in-flight BlockWriter upload tasks");
            drop(writer);
        })
        .await
        .expect("test timed out");
    }

    #[test]
    fn test_ghac_v2_writer_redacts_sas_url_in_errors() {
        let core = test_core(GhacVersion::V2);
        let ctx = OperationContext::new();
        let executor = Executor::default();

        let cases = [
            (
                "/container/blob?sv=2023-01-03&sig=super-secret-sas-token",
                "super-secret-sas-token",
                "sig=[REDACTED]",
            ),
            (
                "https://account.blob.core.windows.net/container?sv=2023-01-03&sig=super-secret-sas-token",
                "super-secret-sas-token",
                "sig=[REDACTED]",
            ),
            (
                "https://user:secret-pass@account.blob.core.windows.net/container/blob",
                "secret-pass",
                "[REDACTED]",
            ),
        ];

        for (url, secret, redacted_marker) in cases {
            let err = GhacWriter::new(
                core.clone(),
                ctx.clone(),
                executor.clone(),
                "cache-key".to_string(),
                url.to_string(),
            )
            .err()
            .expect("invalid signed url must fail");

            let rendered = format!("{err:?}");
            assert!(
                !rendered.contains(secret),
                "error leaked secret in {rendered}"
            );
            assert!(
                rendered.contains(redacted_marker),
                "error missing redaction marker `{redacted_marker}` in {rendered}"
            );
        }
    }

    #[tokio::test]
    async fn test_ghac_v2_writer_preserves_single_percent_encoding_for_blob_path() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let requests = Arc::new(Mutex::new(Vec::new()));
            let finalize_ok = Bytes::from(
                ghac_types::FinalizeCacheEntryUploadResponse {
                    ok: true,
                    entry_id: 99,
                }
                .encode_to_vec(),
            );
            let transport = RecordingTransport {
                requests: requests.clone(),
                responder: Box::new(move |req| {
                    if req.uri().path().ends_with("FinalizeCacheEntryUpload") {
                        (StatusCode::OK, finalize_ok.clone())
                    } else {
                        (StatusCode::CREATED, Bytes::new())
                    }
                }),
            };
            let ctx = OperationContext::from_parts(
                HttpTransporter::new(transport),
                Executor::with(TokioExecutor),
            );
            let mut writer = GhacWriter::new(
                test_core(GhacVersion::V2),
                ctx.clone(),
                ctx.executor().clone(),
                "cache-key".to_string(),
                "https://account.blob.core.windows.net/cache-container/dir/my%20cache%2B1.tzst?sv=2023-01-03&sp=racw&sig=test-sig"
                    .to_string(),
            )
            .unwrap();

            writer.write(Buffer::from("hello")).await.unwrap();
            let meta = writer.close().await.unwrap();
            assert_eq!(meta.content_length(), 5);

            let recorded = requests.lock().unwrap();
            assert_eq!(recorded.len(), 2);
            assert_eq!(recorded[0].method, Method::PUT);
            let parsed_uri: http::Uri = recorded[0].uri.parse().unwrap();
            assert_eq!(
                parsed_uri.path(),
                "/cache-container/dir/my%20cache%2B1.tzst"
            );
            assert!(!recorded[0].uri.contains("%2520"));
            assert!(!recorded[0].uri.contains("%252B"));
        })
        .await
        .expect("test timed out");
    }
}
