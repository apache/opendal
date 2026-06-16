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
use http::Response;
use http::StatusCode;

use super::core::*;
use super::deleter::DropboxDeleter;
use super::error::*;
use super::lister::DropboxLister;
use super::writer::DropboxWriter;
use opendal_core::raw::*;
use opendal_core::*;

#[derive(Clone, Debug)]
pub struct DropboxBackend {
    pub core: Arc<DropboxCore>,
}

/// Reader returned by this backend.
pub struct DropboxReader {
    backend: DropboxBackend,
    ctx: OperationContext,
    path: String,
    args: OpRead,
}

impl DropboxReader {
    fn new(backend: DropboxBackend, ctx: OperationContext, path: &str, args: OpRead) -> Self {
        Self {
            backend,
            ctx,
            path: path.to_string(),
            args,
        }
    }
}

impl oio::StreamRead for DropboxReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let args = self.args.clone();
        let resp = backend
            .core
            .dropbox_get(&self.ctx, path, range, &args)
            .await?;

        let status = resp.status();
        let (rp, stream) = match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => (
                RpRead::new(parse_into_metadata(path, resp.headers())?),
                resp.into_body(),
            ),
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                return Err(parse_error(Response::from_parts(part, buf)));
            }
        };

        Ok((rp, Box::new(stream) as Box<dyn oio::ReadStreamDyn>))
    }
}

impl Service for DropboxBackend {
    type Reader = oio::StreamReader<DropboxReader>;
    type Writer = oio::OneShotWriter<DropboxWriter>;
    type Lister = oio::PageLister<DropboxLister>;
    type Deleter = oio::OneShotDeleter<DropboxDeleter>;
    type Copier = oio::OneShotCopier;

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
        // Check if the folder already exists.
        let resp = self.core.dropbox_get_metadata(ctx, path).await?;
        if StatusCode::OK == resp.status() {
            let bytes = resp.into_body();
            let decoded_response: DropboxMetadataResponse =
                serde_json::from_reader(bytes.reader()).map_err(new_json_deserialize_error)?;
            if "folder" == decoded_response.tag {
                return Ok(RpCreateDir::default());
            }
            if "file" == decoded_response.tag {
                return Err(Error::new(
                    ErrorKind::NotADirectory,
                    format!("it's not a directory {path}"),
                ));
            }
        }

        let res = self.core.dropbox_create_folder(ctx, path).await?;
        Ok(res)
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, _: OpStat) -> Result<RpStat> {
        let resp = self.core.dropbox_get_metadata(ctx, path).await?;
        let status = resp.status();
        match status {
            StatusCode::OK => {
                let bytes = resp.into_body();
                let decoded_response: DropboxMetadataResponse =
                    serde_json::from_reader(bytes.reader()).map_err(new_json_deserialize_error)?;
                let entry_mode: EntryMode = match decoded_response.tag.as_str() {
                    "file" => EntryMode::FILE,
                    "folder" => EntryMode::DIR,
                    _ => EntryMode::Unknown,
                };

                let mut metadata = Metadata::new(entry_mode);
                // Only set last_modified and size if entry_mode is FILE, because Dropbox API
                // returns last_modified and size only for files.
                // FYI: https://www.dropbox.com/developers/documentation/http/documentation#files-get_metadata
                if entry_mode == EntryMode::FILE {
                    let date_utc_last_modified =
                        decoded_response.client_modified.parse::<Timestamp>()?;
                    metadata.set_last_modified(date_utc_last_modified);

                    if let Some(size) = decoded_response.size {
                        metadata.set_content_length(size);
                    } else {
                        return Err(Error::new(
                            ErrorKind::Unexpected,
                            format!("no size found for file {path}"),
                        ));
                    }
                }
                Ok(RpStat::new(metadata))
            }
            _ => Err(parse_error(resp)),
        }
    }
    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let output: oio::StreamReader<DropboxReader> = {
            Ok(oio::StreamReader::new(DropboxReader::new(
                self.clone(),
                ctx.clone(),
                path,
                args,
            )))
        }?;

        Ok(output)
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let output: oio::OneShotWriter<DropboxWriter> = {
            Ok(oio::OneShotWriter::new(DropboxWriter::new(
                self.core.clone(),
                ctx.clone(),
                args,
                String::from(path),
            )))
        }?;

        Ok(output)
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        let output: oio::OneShotDeleter<DropboxDeleter> = {
            Ok(oio::OneShotDeleter::new(DropboxDeleter::new(
                self.core.clone(),
                ctx.clone(),
            )))
        }?;

        Ok(output)
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        let output: oio::PageLister<DropboxLister> = {
            Ok(oio::PageLister::new(DropboxLister::new(
                self.core.clone(),
                ctx.clone(),
                path.to_string(),
                args.recursive(),
                args.limit(),
            )))
        }?;

        Ok(output)
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        _: OpCopy,
        _opts: OpCopier,
    ) -> Result<Self::Copier> {
        let core = self.core.clone();
        let ctx = ctx.clone();
        let from = from.to_string();
        let to = to.to_string();

        Ok(oio::OneShotCopier::new(async move {
            let resp = core.dropbox_copy(&ctx, &from, &to).await?;
            let status = resp.status();

            match status {
                StatusCode::OK => Ok(Metadata::default()),
                _ => {
                    let err = parse_error(resp);
                    match err.kind() {
                        ErrorKind::NotFound => Ok(Metadata::default()),
                        _ => Err(err),
                    }
                }
            }
        }))
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        _: OpRename,
    ) -> Result<RpRename> {
        let resp = self.core.dropbox_move(ctx, from, to).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(RpRename::default()),
            _ => {
                let err = parse_error(resp);
                match err.kind() {
                    ErrorKind::NotFound => Ok(RpRename::default()),
                    _ => Err(err),
                }
            }
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
