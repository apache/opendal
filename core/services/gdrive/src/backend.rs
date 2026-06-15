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
use http::Response;
use http::StatusCode;

use super::core::GdriveCore;
use super::core::GdriveFile;
use super::core::GdriveRecentPathState;
use super::core::normalize_dir_path;
use super::deleter::GdriveDeleter;
use super::error::parse_error;
use super::lister::GdriveFlatLister;
use super::lister::GdriveLister;
use super::writer::GdriveWriter;
use opendal_core::raw::*;
use opendal_core::*;

#[derive(Clone, Debug)]
pub struct GdriveBackend {
    pub core: Arc<GdriveCore>,
}

/// Lister type that supports both recursive and non-recursive listing
pub type GdriveListers = TwoWays<oio::PageLister<GdriveLister>, GdriveFlatLister>;

/// Reader returned by this backend.
pub struct GdriveReader {
    backend: GdriveBackend,
    ctx: OperationContext,
    path: String,
}

impl GdriveReader {
    fn new(backend: GdriveBackend, ctx: OperationContext, path: &str, _: OpRead) -> Self {
        Self {
            backend,
            ctx,
            path: path.to_string(),
        }
    }
}

impl oio::StreamRead for GdriveReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let abs_path = build_abs_path(&backend.core.root, path);
        let resp = match backend.core.gdrive_get(&self.ctx, path, range).await {
            Ok(resp) => resp,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                backend.core.refresh_path(&abs_path).await;
                backend.core.gdrive_get(&self.ctx, path, range).await?
            }
            Err(err) => return Err(err),
        };

        let status = resp.status();
        let (rp, stream) = match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => (
                RpRead::new(parse_into_metadata(path, resp.headers())?),
                resp.into_body(),
            ),
            StatusCode::NOT_FOUND => {
                backend.core.refresh_path(&abs_path).await;
                let resp = backend.core.gdrive_get(&self.ctx, path, range).await?;
                let status = resp.status();
                match status {
                    StatusCode::OK | StatusCode::PARTIAL_CONTENT => (
                        RpRead::new(parse_into_metadata(path, resp.headers())?),
                        resp.into_body(),
                    ),
                    _ => {
                        let (part, mut body) = resp.into_parts();
                        let buf = body.to_buffer().await?;
                        return Err(parse_error(Response::from_parts(part, buf)));
                    }
                }
            }
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                return Err(parse_error(Response::from_parts(part, buf)));
            }
        };

        Ok((rp, Box::new(stream) as Box<dyn oio::ReadStreamDyn>))
    }
}

impl Service for GdriveBackend {
    type Reader = oio::StreamReader<GdriveReader>;
    type Writer = oio::OneShotWriter<GdriveWriter>;
    type Lister = GdriveListers;
    type Deleter = oio::OneShotDeleter<GdriveDeleter>;
    type Copier = ();

    fn info(&self) -> ServiceInfo {
        self.core.info.clone()
    }

    fn capability(&self) -> Capability {
        self.core.capability
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        _args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        let path = build_abs_path(&self.core.root, path);
        let dir_id = self.core.ensure_dir(ctx, &path).await?;
        let metadata = Metadata::new(EntryMode::DIR);

        self.core.cache_dir_id(&path, &dir_id).await;
        self.core.record_recent_upsert(&path, metadata).await;

        Ok(RpCreateDir::default())
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, _args: OpStat) -> Result<RpStat> {
        let path = build_abs_path(&self.core.root, path);

        match self.core.recent_entry_for_path(&path).await {
            GdriveRecentPathState::Present(metadata) => {
                if metadata.mode().is_dir() && path.ends_with('/') {
                    return Ok(RpStat::new(*metadata));
                }
            }
            GdriveRecentPathState::Deleted => {
                return Err(Error::new(
                    ErrorKind::NotFound,
                    format!("path not found: {path}"),
                ));
            }
            GdriveRecentPathState::Missing => {}
        }

        let mut file_id = match self.core.resolve_path(ctx, &path).await? {
            Some(id) => id,
            None => match self.core.resolve_path_after_refresh(ctx, &path).await? {
                Some(id) => id,
                None => {
                    return Err(Error::new(
                        ErrorKind::NotFound,
                        format!("path not found: {path}"),
                    ));
                }
            },
        };
        let mut resp = self.core.gdrive_stat_by_id(ctx, &file_id).await?;

        if resp.status() == StatusCode::NOT_FOUND {
            file_id = self
                .core
                .resolve_path_after_refresh(ctx, &path)
                .await?
                .ok_or(Error::new(
                    ErrorKind::NotFound,
                    format!("path not found: {path}"),
                ))?;
            resp = self.core.gdrive_stat_by_id(ctx, &file_id).await?;
        }

        if resp.status() != StatusCode::OK {
            return Err(parse_error(resp));
        }

        let bs = resp.into_body();
        let gdrive_file: GdriveFile =
            serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

        let file_type = if gdrive_file.mime_type == "application/vnd.google-apps.folder" {
            EntryMode::DIR
        } else {
            EntryMode::FILE
        };
        let mut meta = Metadata::new(file_type).with_content_type(gdrive_file.mime_type);
        if let Some(v) = gdrive_file.size {
            meta = meta.with_content_length(v.parse::<u64>().map_err(|e| {
                Error::new(ErrorKind::Unexpected, "parse content length").set_source(e)
            })?);
        }
        if let Some(v) = gdrive_file.modified_time {
            meta = meta.with_last_modified(v.parse::<Timestamp>().map_err(|e| {
                Error::new(ErrorKind::Unexpected, "parse last modified time").set_source(e)
            })?);
        }
        Ok(RpStat::new(meta))
    }
    async fn read(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRead,
    ) -> Result<(RpRead, Self::Reader)> {
        let (rp, output): (_, oio::StreamReader<GdriveReader>) = {
            Ok((
                RpRead::default(),
                oio::StreamReader::new(GdriveReader::new(self.clone(), ctx.clone(), path, args)),
            ))
        }?;

        Ok((rp, output))
    }

    async fn write(
        &self,
        ctx: &OperationContext,
        path: &str,
        _: OpWrite,
    ) -> Result<(RpWrite, Self::Writer)> {
        let (rp, output): (_, oio::OneShotWriter<GdriveWriter>) = {
            let path = build_abs_path(&self.core.root, path);

            // As Google Drive allows files have the same name, we need to check if the file exists.
            // If the file exists, we will keep its ID and update it.
            let file_id = match self.core.resolve_path(ctx, &path).await? {
                Some(id) => Some(id),
                None => self.core.resolve_path_after_refresh(ctx, &path).await?,
            };

            Ok((
                RpWrite::default(),
                oio::OneShotWriter::new(GdriveWriter::new(
                    self.core.clone(),
                    ctx.clone(),
                    path,
                    file_id,
                )),
            ))
        }?;

        Ok((rp, output))
    }

    async fn delete(&self, ctx: &OperationContext) -> Result<(RpDelete, Self::Deleter)> {
        let (rp, output): (_, oio::OneShotDeleter<GdriveDeleter>) = {
            Ok((
                RpDelete::default(),
                oio::OneShotDeleter::new(GdriveDeleter::new(self.core.clone(), ctx.clone())),
            ))
        }?;

        Ok((rp, output))
    }

    async fn list(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpList,
    ) -> Result<(RpList, Self::Lister)> {
        let (rp, output): (_, GdriveListers) = {
            let path = build_abs_path(&self.core.root, path);

            if args.recursive() {
                // Use optimized batch-query recursive lister
                let l = GdriveFlatLister::new(path, self.core.clone(), ctx.clone());
                Ok((RpList::default(), TwoWays::Two(l)))
            } else {
                // Use standard page-based lister for non-recursive
                let l = GdriveLister::new(path, self.core.clone(), ctx.clone());
                Ok((RpList::default(), TwoWays::One(oio::PageLister::new(l))))
            }
        }?;

        Ok((rp, output))
    }

    async fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        _args: OpCopy,
        _opts: OpCopier,
    ) -> Result<(RpCopy, Self::Copier)> {
        let (rp, output): (_, ()) = {
            let source = build_abs_path(&self.core.root, from);
            let target = build_abs_path(&self.core.root, to);
            let resp = self.core.gdrive_copy(ctx, from, to).await?;

            match resp.status() {
                StatusCode::OK => {
                    let body = resp.into_body();
                    let meta: GdriveFile = serde_json::from_reader(body.reader())
                        .map_err(new_json_deserialize_error)?;

                    let to_path = build_abs_path(&self.core.root, to);
                    let mut metadata = if meta.mime_type == "application/vnd.google-apps.folder" {
                        Metadata::new(EntryMode::DIR)
                    } else {
                        Metadata::new(EntryMode::FILE)
                    };
                    if let Some(size) = meta.size {
                        metadata =
                            metadata.with_content_length(size.parse::<u64>().map_err(|e| {
                                Error::new(ErrorKind::Unexpected, "parse content length")
                                    .set_source(e)
                            })?);
                    }

                    if metadata.mode().is_dir() {
                        self.core.cache_dir_id(&to_path, &meta.id).await;
                    } else {
                        self.core.cache_file_id(&to_path, &meta.id).await;
                    }
                    self.core.record_recent_upsert(&to_path, metadata).await;

                    Ok((RpCopy::default(), ()))
                }
                StatusCode::NOT_FOUND => {
                    self.core.refresh_path(&source).await;
                    self.core.refresh_path(&target).await;
                    let resp = self.core.gdrive_copy(ctx, from, to).await?;
                    match resp.status() {
                        StatusCode::OK => {
                            let body = resp.into_body();
                            let meta: GdriveFile = serde_json::from_reader(body.reader())
                                .map_err(new_json_deserialize_error)?;

                            let to_path = build_abs_path(&self.core.root, to);
                            let mut metadata =
                                if meta.mime_type == "application/vnd.google-apps.folder" {
                                    Metadata::new(EntryMode::DIR)
                                } else {
                                    Metadata::new(EntryMode::FILE)
                                };
                            if let Some(size) = meta.size {
                                metadata = metadata.with_content_length(
                                    size.parse::<u64>().map_err(|e| {
                                        Error::new(ErrorKind::Unexpected, "parse content length")
                                            .set_source(e)
                                    })?,
                                );
                            }

                            if metadata.mode().is_dir() {
                                self.core.cache_dir_id(&to_path, &meta.id).await;
                            } else {
                                self.core.cache_file_id(&to_path, &meta.id).await;
                            }
                            self.core.record_recent_upsert(&to_path, metadata).await;

                            Ok((RpCopy::default(), ()))
                        }
                        _ => Err(parse_error(resp)),
                    }
                }
                _ => Err(parse_error(resp)),
            }
        }?;

        Ok((rp, output))
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        _args: OpRename,
    ) -> Result<RpRename> {
        let source = build_abs_path(&self.core.root, from);
        let target = build_abs_path(&self.core.root, to);

        // rename will overwrite `to`, delete it if exist
        self.core.trash_path_if_exists(ctx, &target).await?;

        let resp = self
            .core
            .gdrive_patch_metadata_request(ctx, &source, &target)
            .await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let body = resp.into_body();
                let meta: GdriveFile =
                    serde_json::from_reader(body.reader()).map_err(new_json_deserialize_error)?;

                let source_path = if meta.mime_type == "application/vnd.google-apps.folder" {
                    normalize_dir_path(&build_abs_path(&self.core.root, from))
                } else {
                    build_abs_path(&self.core.root, from)
                };
                let target_path = if meta.mime_type == "application/vnd.google-apps.folder" {
                    normalize_dir_path(&build_abs_path(&self.core.root, to))
                } else {
                    build_abs_path(&self.core.root, to)
                };
                let mut metadata = if meta.mime_type == "application/vnd.google-apps.folder" {
                    Metadata::new(EntryMode::DIR)
                } else {
                    Metadata::new(EntryMode::FILE)
                };
                if let Some(size) = meta.size {
                    metadata = metadata.with_content_length(size.parse::<u64>().map_err(|e| {
                        Error::new(ErrorKind::Unexpected, "parse content length").set_source(e)
                    })?);
                }

                if metadata.mode().is_dir() {
                    self.core.invalidate_dir_id(&source_path).await;
                    self.core.cache_dir_id(&target_path, &meta.id).await;
                } else {
                    self.core.invalidate_file_id(&source_path).await;
                    self.core.cache_file_id(&target_path, &meta.id).await;
                }
                self.core
                    .record_recent_delete(&source_path, metadata.mode())
                    .await;
                self.core.record_recent_upsert(&target_path, metadata).await;

                Ok(RpRename::default())
            }
            StatusCode::NOT_FOUND => {
                self.core.refresh_path(&source).await;
                self.core.refresh_path(&target).await;

                let resp = self
                    .core
                    .gdrive_patch_metadata_request(ctx, &source, &target)
                    .await?;

                if resp.status() != StatusCode::OK {
                    return Err(parse_error(resp));
                }

                let body = resp.into_body();
                let meta: GdriveFile =
                    serde_json::from_reader(body.reader()).map_err(new_json_deserialize_error)?;

                let source_path = if meta.mime_type == "application/vnd.google-apps.folder" {
                    normalize_dir_path(&build_abs_path(&self.core.root, from))
                } else {
                    build_abs_path(&self.core.root, from)
                };
                let target_path = if meta.mime_type == "application/vnd.google-apps.folder" {
                    normalize_dir_path(&build_abs_path(&self.core.root, to))
                } else {
                    build_abs_path(&self.core.root, to)
                };
                let mut metadata = if meta.mime_type == "application/vnd.google-apps.folder" {
                    Metadata::new(EntryMode::DIR)
                } else {
                    Metadata::new(EntryMode::FILE)
                };
                if let Some(size) = meta.size {
                    metadata = metadata.with_content_length(size.parse::<u64>().map_err(|e| {
                        Error::new(ErrorKind::Unexpected, "parse content length").set_source(e)
                    })?);
                }

                if metadata.mode().is_dir() {
                    self.core.invalidate_dir_id(&source_path).await;
                    self.core.cache_dir_id(&target_path, &meta.id).await;
                } else {
                    self.core.invalidate_file_id(&source_path).await;
                    self.core.cache_file_id(&target_path, &meta.id).await;
                }
                self.core
                    .record_recent_delete(&source_path, metadata.mode())
                    .await;
                self.core.record_recent_upsert(&target_path, metadata).await;

                Ok(RpRename::default())
            }
            _ => Err(parse_error(resp)),
        }
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
