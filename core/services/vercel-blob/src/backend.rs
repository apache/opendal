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

use std::fmt::Debug;
use std::sync::Arc;

use bytes::Buf;
use http::StatusCode;
use log::debug;

use super::VERCEL_BLOB_SCHEME;
use super::config::VercelBlobConfig;
use super::core::Blob;
use super::core::parse_blob;
use super::core::parse_error;
use super::core::{ErrorContext, VercelBlobCore};
use super::deleter::VercelBlobDeleter;
use super::lister::VercelBlobLister;
use super::reader::*;
use super::writer::VercelBlobWriter;
use super::writer::VercelBlobWriters;
use opendal_core::raw::*;
use opendal_core::*;

/// [VercelBlob](https://vercel.com/docs/storage/vercel-blob) services support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct VercelBlobBuilder {
    pub(super) config: VercelBlobConfig,
}

impl Debug for VercelBlobBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VercelBlobBuilder")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl VercelBlobBuilder {
    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// Vercel Blob token.
    ///
    /// Get from Vercel environment variable `BLOB_READ_WRITE_TOKEN`.
    /// It is required.
    pub fn token(mut self, token: &str) -> Self {
        self.config.token = if token.is_empty() {
            None
        } else {
            Some(token.to_string())
        };
        self
    }
}

impl Builder for VercelBlobBuilder {
    type Config = VercelBlobConfig;

    /// Builds the backend and returns the result of VercelBlobBackend.
    fn build(self) -> Result<impl Service> {
        debug!("backend build started: {:?}", self);

        let root = normalize_root(&self.config.root.clone().unwrap_or_default());
        debug!("backend use root {}", root);

        // Handle token.
        let Some(token) = self.config.token.clone() else {
            return Err(Error::new(ErrorKind::ConfigInvalid, "token is empty")
                .with_operation("Builder::build")
                .with_context("service", VERCEL_BLOB_SCHEME));
        };

        Ok(VercelBlobBackend {
            core: Arc::new(VercelBlobCore {
                info: ServiceInfo::new(VERCEL_BLOB_SCHEME, &root, ""),
                capability: Capability {
                    stat: true,

                    read: true,
                    read_with_suffix: true,

                    write: true,
                    write_can_empty: true,
                    write_can_multi: true,
                    write_multi_min_size: Some(5 * 1024 * 1024),

                    copy: true,

                    list: true,
                    list_with_limit: true,

                    delete: true,

                    shared: true,

                    ..Default::default()
                },
                root,
                token,
            }),
        })
    }
}

/// Backend for VercelBlob services.
#[derive(Debug, Clone)]
pub struct VercelBlobBackend {
    pub(crate) core: Arc<VercelBlobCore>,
}

impl Service for VercelBlobBackend {
    type Reader = oio::StreamReader<VercelBlobReader>;
    type Writer = VercelBlobWriters;
    type Lister = oio::PageLister<VercelBlobLister>;
    type Deleter = oio::OneShotDeleter<VercelBlobDeleter>;
    type Copier = oio::OneShotCopier;
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        self.core.info.clone()
    }

    fn capability(&self) -> Capability {
        self.core.capability
    }

    async fn create_dir(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, _args: OpStat) -> Result<RpStat> {
        let resp = self.core.head(ctx, path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let bs = resp.into_body();

                let resp: Blob =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

                parse_blob(&resp).map(RpStat::new)
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("HeadBlob")),
                resp,
            )),
        }
    }
    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let output: oio::StreamReader<VercelBlobReader> = {
            Ok(oio::StreamReader::new(VercelBlobReader::new(
                self.clone(),
                ctx.clone(),
                path,
                args,
            )))
        }?;

        Ok(output)
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let output: VercelBlobWriters = {
            let concurrent = args.concurrent();
            let writer =
                VercelBlobWriter::new(self.core.clone(), ctx.clone(), args, path.to_string());

            let w = oio::MultipartWriter::new(ctx.executor().clone(), writer, concurrent);

            Ok(w)
        }?;

        Ok(output)
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> Result<Self::Copier> {
        let backend = self.clone();
        let core = self.core.clone();
        let ctx = ctx.clone();
        let from = from.to_string();
        let to = to.to_string();
        let source_content_length_hint = args.source_content_length_hint();

        Ok(oio::OneShotCopier::new_with(move || {
            let backend = backend.clone();
            let core = core.clone();
            let ctx = ctx.clone();
            let from = from.clone();
            let to = to.clone();

            async move {
                let source_size = match source_content_length_hint {
                    Some(size) => size,
                    None => backend
                        .stat(&ctx, &from, OpStat::default())
                        .await?
                        .into_metadata()
                        .content_length(),
                };

                let resp = core.copy(&ctx, &from, &to).await?;
                let status = resp.status();

                match status {
                    StatusCode::OK => Ok(MetadataBuilder::file(source_size).build()),
                    _ => Err(parse_error(
                        ErrorContext::new(ServiceOperation("CopyBlob")),
                        resp,
                    )),
                }
            }
        }))
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        let output: oio::PageLister<VercelBlobLister> = {
            let l = VercelBlobLister::new(self.core.clone(), ctx.clone(), path, args.limit());
            Ok(oio::PageLister::new(l))
        }?;

        Ok(output)
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        let output: oio::OneShotDeleter<VercelBlobDeleter> = {
            Ok(oio::OneShotDeleter::new(VercelBlobDeleter::new(
                self.core.clone(),
                ctx.clone(),
            )))
        }?;

        Ok(output)
    }

    async fn rename(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpRename,
    ) -> Result<RpRename> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn presign(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpPresign,
    ) -> Result<RpPresign> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use futures::stream;
    use http::{Method, Request, Response, StatusCode};
    use opendal_core::raw::oio::Copy;

    use super::*;

    #[derive(Clone, Default)]
    struct RetryCopyTransport {
        put_attempts: Arc<AtomicUsize>,
    }

    impl HttpTransport for RetryCopyTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let (status, body) = if req.method() == Method::GET {
                if req.uri().query().unwrap_or_default().contains("url=") {
                    (
                        StatusCode::OK,
                        r#"{"url":"https://store.public.blob.vercel-storage.com/src.txt","pathname":"src.txt","size":12,"uploadedAt":"2026-01-01T00:00:00Z","contentDisposition":"inline"}"#.to_string(),
                    )
                } else {
                    (
                        StatusCode::OK,
                        r#"{"hasMore":false,"blobs":[{"url":"https://store.public.blob.vercel-storage.com/src.txt","pathname":"src.txt","size":12,"uploadedAt":"2026-01-01T00:00:00Z","contentDisposition":"inline"}]}"#.to_string(),
                    )
                }
            } else if req.method() == Method::PUT {
                let attempt = self.put_attempts.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    (
                        StatusCode::SERVICE_UNAVAILABLE,
                        r#"{"error":{"code":"service_unavailable","message":"try again"}}"#
                            .to_string(),
                    )
                } else {
                    (StatusCode::OK, "{}".to_string())
                }
            } else {
                (StatusCode::METHOD_NOT_ALLOWED, String::new())
            };

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

    #[tokio::test]
    async fn copy_retries_after_transient_error() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let transport = RetryCopyTransport::default();
            let backend = VercelBlobBuilder::default()
                .token("valid-token")
                .build()
                .unwrap();
            let ctx = OperationContext::new()
                .with_http_transport(HttpTransporter::new(transport.clone()));

            let mut copier = backend
                .copy(&ctx, "src.txt", "dst.txt", OpCopy::default())
                .unwrap();

            let first_err = copier
                .close()
                .await
                .expect_err("first attempt must return transient 503");
            assert!(first_err.is_temporary());

            let meta = copier
                .close()
                .await
                .expect("retry on same OneShotCopier must succeed");
            assert_eq!(meta.content_length(), 12);
            assert_eq!(transport.put_attempts.load(Ordering::SeqCst), 2);
        })
        .await
        .expect("test timed out");
    }
}
