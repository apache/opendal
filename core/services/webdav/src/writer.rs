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

use http::{HeaderMap, Response, StatusCode};
use opendal_core::raw::*;
use opendal_core::*;
use uuid::Uuid;

use super::core::*;

pub struct WebdavWriter {
    core: Arc<WebdavCore>,
    ctx: OperationContext,
    op: OpWrite,
    path: String,
}

impl WebdavWriter {
    pub fn new(core: Arc<WebdavCore>, ctx: OperationContext, op: OpWrite, path: String) -> Self {
        Self {
            core,
            ctx,
            op,
            path,
        }
    }

    async fn ensure_parent(&self) -> Result<()> {
        if !self.core.disable_create_dir {
            self.core
                .webdav_mkcol(&self.ctx, get_parent(&self.path))
                .await?;
        }
        Ok(())
    }

    async fn put(
        &self,
        path: &str,
        range: Option<BytesContentRange>,
        body: Buffer,
    ) -> Result<Response<Buffer>> {
        let response = self
            .core
            .webdav_put(&self.ctx, path, range, &self.op, body)
            .await?;
        match response.status() {
            StatusCode::CREATED | StatusCode::OK | StatusCode::NO_CONTENT => Ok(response),
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("Put")),
                response,
            )),
        }
    }

    async fn set_user_metadata(&self, path: &str) -> Result<()> {
        let Some(user_metadata) = self.op.user_metadata() else {
            return Ok(());
        };
        let user_metadata = user_metadata
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect();
        let response = self
            .core
            .webdav_proppatch(&self.ctx, path, &user_metadata)
            .await?;

        match response.status() {
            StatusCode::MULTI_STATUS => {
                let body = response.into_body().to_bytes();
                check_proppatch_response(&String::from_utf8_lossy(&body))
            }
            status if status.is_success() => Ok(()),
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("Proppatch")),
                response,
            )),
        }
    }
}

impl oio::OneShotWrite for WebdavWriter {
    async fn write_once(&self, bs: Buffer) -> Result<Metadata> {
        self.ensure_parent().await?;
        let response = self.put(&self.path, None, bs).await?;
        let metadata = write_metadata(response.headers(), MetadataBuilder::unknown())?;
        self.set_user_metadata(&self.path).await?;
        Ok(metadata)
    }
}

pub struct WebdavChunkedWriter {
    inner: WebdavWriter,
    state: UploadState,
}

enum UploadState {
    Buffered(Option<Buffer>),
    Staged(StagedWrite),
    Finished { cleanup_path: Option<String> },
}

struct StagedWrite {
    path: String,
    first_chunk: Option<Buffer>,
    written: u64,
    // A failed or cancelled partial PUT may have written bytes. Only a retry
    // covering the same range can make the staged file safe to publish again.
    pending_end: Option<u64>,
}

impl WebdavChunkedWriter {
    pub fn new(inner: WebdavWriter) -> Self {
        Self {
            inner,
            state: UploadState::Buffered(None),
        }
    }

    async fn publish(&self, path: &str, size: u64) -> Result<Metadata> {
        self.inner.set_user_metadata(path).await?;
        let response = self
            .inner
            .core
            .webdav_move(
                &self.inner.ctx,
                path,
                &self.inner.path,
                false,
                Operation::Write,
            )
            .await?;
        match response.status() {
            StatusCode::CREATED | StatusCode::OK | StatusCode::NO_CONTENT => {
                Ok(MetadataBuilder::file(size).build())
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("Move")),
                response,
            )),
        }
    }
}

impl oio::Write for WebdavChunkedWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        if bs.is_empty() {
            return Ok(());
        }
        if let UploadState::Buffered(first) = &mut self.state {
            let Some(first) = first else {
                *first = Some(bs);
                return Ok(());
            };
            self.inner.ensure_parent().await?;
            let parent = get_parent(&self.inner.path).trim_start_matches('/');
            let path = loop {
                let path = format!("{parent}.opendal-webdav-{}", Uuid::new_v4());
                if path != self.inner.path {
                    break path;
                }
            };
            self.state = UploadState::Staged(StagedWrite {
                path,
                first_chunk: Some(first.clone()),
                written: 0,
                pending_end: None,
            });
        }

        let UploadState::Staged(stage) = &mut self.state else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "WebDAV upload is finished",
            ));
        };
        if let Some(first) = &stage.first_chunk {
            self.inner.put(&stage.path, None, first.clone()).await?;
            stage.written = first.len() as u64;
            stage.first_chunk = None;
        }

        let end = stage.written.checked_add(bs.len() as u64).ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "WebDAV upload size exceeds u64::MAX")
        })?;
        if stage
            .pending_end
            .is_some_and(|pending_end| pending_end != end)
        {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "WebDAV partial PUT retry must cover the same range",
            ));
        }
        let range = BytesContentRange::default().with_range(stage.written, end - 1);
        stage.pending_end = Some(end);
        self.inner.put(&stage.path, Some(range), bs).await?;
        stage.written = end;
        stage.pending_end = None;
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        let result = match &self.state {
            UploadState::Buffered(first) => {
                let metadata =
                    oio::OneShotWrite::write_once(&self.inner, first.clone().unwrap_or_default())
                        .await?;
                Ok(metadata)
            }
            UploadState::Staged(stage) => {
                let path = stage.path.clone();
                let size = stage.written;
                let complete = stage.first_chunk.is_none() && stage.pending_end.is_none();
                self.state = UploadState::Finished {
                    cleanup_path: Some(path.clone()),
                };
                if complete {
                    self.publish(&path, size).await
                } else {
                    Err(Error::new(
                        ErrorKind::Unexpected,
                        "cannot publish an incomplete WebDAV upload",
                    ))
                }
            }
            UploadState::Finished { .. } => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "WebDAV upload is finished",
                ));
            }
        };

        match result {
            Ok(metadata) => {
                self.state = UploadState::Finished { cleanup_path: None };
                Ok(metadata)
            }
            Err(error) => {
                if let Err(cleanup_error) = self.abort().await {
                    log::warn!("failed to delete the staged WebDAV upload: {cleanup_error}");
                }
                // After cleanup the source is unavailable for a MOVE retry.
                Err(error.set_persistent())
            }
        }
    }

    async fn abort(&mut self) -> Result<()> {
        if let UploadState::Staged(stage) = &self.state {
            self.state = UploadState::Finished {
                cleanup_path: Some(stage.path.clone()),
            };
        }
        if let UploadState::Finished {
            cleanup_path: Some(path),
        } = &self.state
        {
            let response = self
                .inner
                .core
                .webdav_delete(&self.inner.ctx, path, &OpDelete::default())
                .await?;
            match response.status() {
                StatusCode::NO_CONTENT | StatusCode::NOT_FOUND => (),
                _ => {
                    return Err(parse_error(
                        ErrorContext::new(ServiceOperation("Delete")),
                        response,
                    ));
                }
            }
        }
        self.state = UploadState::Finished { cleanup_path: None };
        Ok(())
    }
}

fn write_metadata(headers: &HeaderMap, mut metadata: MetadataBuilder) -> Result<Metadata> {
    if let Some(etag) = parse_etag(headers)? {
        metadata.etag(etag);
    }
    if let Some(last_modified) = parse_last_modified(headers)? {
        metadata.last_modified(last_modified);
    }
    Ok(metadata.build())
}
