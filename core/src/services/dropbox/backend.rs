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

use super::core::*;
use super::delete::DropboxDeleter;
use super::error::*;
use super::lister::DropboxLister;
use super::writer::DropboxWriter;
use crate::raw::*;
use crate::*;

#[derive(Clone, Debug)]
pub struct DropboxBackend {
    pub core: Arc<DropboxCore>,
}

impl Access for DropboxBackend {
    type Reader = HttpBody;
    type Writer = oio::OneShotWriter<DropboxWriter>;
    type Lister = oio::PageLister<DropboxLister>;
    type Deleter = oio::OneShotDeleter<DropboxDeleter>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn create_dir(&self, path: &str, _args: OpCreateDir) -> Result<RpCreateDir> {
        // Check if the folder already exists.
        let resp = self.core.dropbox_get_metadata(path).await?;
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
                    format!("it's not a directory {}", path),
                ));
            }
        }

        let res = self.core.dropbox_create_folder(path).await?;
        Ok(res)
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let resp = self.core.dropbox_get_metadata(path).await?;
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
                        parse_datetime_from_rfc3339(&decoded_response.client_modified)?;
                    metadata.set_last_modified(date_utc_last_modified);

                    if let Some(size) = decoded_response.size {
                        metadata.set_content_length(size);
                    } else {
                        return Err(Error::new(
                            ErrorKind::Unexpected,
                            format!("no size found for file {}", path),
                        ));
                    }
                }
                Ok(RpStat::new(metadata))
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.core.dropbox_get(path, args.range(), &args).await?;

        let status = resp.status();
        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                Ok((RpRead::default(), resp.into_body()))
            }
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                Err(parse_error(Response::from_parts(part, buf)))
            }
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        Ok((
            RpWrite::default(),
            oio::OneShotWriter::new(DropboxWriter::new(
                self.core.clone(),
                args,
                String::from(path),
            )),
        ))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(DropboxDeleter::new(self.core.clone())),
        ))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        Ok((
            RpList::default(),
            oio::PageLister::new(DropboxLister::new(
                self.core.clone(),
                path.to_string(),
                args.recursive(),
                args.limit(),
            )),
        ))
    }

    async fn copy(&self, from: &str, to: &str, _: OpCopy) -> Result<RpCopy> {
        let resp = self.core.dropbox_copy(from, to).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(RpCopy::default()),
            _ => {
                let err = parse_error(resp);
                match err.kind() {
                    ErrorKind::NotFound => Ok(RpCopy::default()),
                    _ => Err(err),
                }
            }
        }
    }

    async fn rename(&self, from: &str, to: &str, _: OpRename) -> Result<RpRename> {
        let resp = self.core.dropbox_move(from, to).await?;

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
}
