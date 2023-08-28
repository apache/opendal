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

use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;

use super::core::GdriveCore;
use super::error::parse_error;
use super::writer::GdriveWriter;
use crate::raw::*;
use crate::services::gdrive::core::GdriveFile;
use crate::services::gdrive::core::GdriveFileList;
use crate::types::Result;
use crate::*;

#[derive(Clone, Debug)]
pub struct GdriveBackend {
    pub core: Arc<GdriveCore>,
}

#[async_trait]
impl Accessor for GdriveBackend {
    type Reader = IncomingAsyncBody;
    type BlockingReader = ();
    type Writer = GdriveWriter;
    type BlockingWriter = ();
    type Pager = ();
    type BlockingPager = ();

    fn info(&self) -> AccessorInfo {
        let mut ma = AccessorInfo::default();
        ma.set_scheme(Scheme::Gdrive)
            .set_root(&self.core.root)
            .set_full_capability(Capability {
                stat: true,

                read: true,

                write: true,

                create_dir: true,

                delete: true,

                ..Default::default()
            });

        ma
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        let resp = self.core.gdrive_stat(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let meta = self.parse_metadata(resp.into_body().bytes().await?)?;
                Ok(RpStat::new(meta))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn create_dir(&self, path: &str, _args: OpCreateDir) -> Result<RpCreateDir> {
        let parent = self.core.ensure_parent_path(path).await?;

        let path = path.split('/').filter(|&x| !x.is_empty()).last().unwrap();

        // As Google Drive allows files have the same name, we need to check if the folder exists.
        let resp = self.core.gdrive_search_folder(path, &parent).await?;
        let status = resp.status();

        match status {
            StatusCode::OK => {
                let body = resp.into_body().bytes().await?;
                let meta = serde_json::from_slice::<GdriveFileList>(&body)
                    .map_err(new_json_deserialize_error)?;

                if !meta.files.is_empty() {
                    let mut cache = self.core.path_cache.lock().await;

                    cache.insert(path.to_string(), meta.files[0].id.clone());

                    return Ok(RpCreateDir::default());
                }
            }
            _ => return Err(parse_error(resp).await?),
        }

        let resp = self.core.gdrive_create_folder(path, Some(parent)).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let body = resp.into_body().bytes().await?;
                let meta = serde_json::from_slice::<GdriveFile>(&body)
                    .map_err(new_json_deserialize_error)?;

                let mut cache = self.core.path_cache.lock().await;

                cache.insert(path.to_string(), meta.id.clone());

                Ok(RpCreateDir::default())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn read(&self, path: &str, _args: OpRead) -> Result<(RpRead, Self::Reader)> {
        // We need to request for metadata and body separately here.
        // Request for metadata first to check if the file exists.
        let resp = self.core.gdrive_stat(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let body = resp.into_body().bytes().await?;
                let meta = self.parse_metadata(body)?;

                let resp = self.core.gdrive_get(path).await?;

                let status = resp.status();

                match status {
                    StatusCode::OK => Ok((RpRead::with_metadata(meta), resp.into_body())),
                    _ => Err(parse_error(resp).await?),
                }
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        if args.content_length().is_none() {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "write without content length is not supported",
            ));
        }

        // As Google Drive allows files have the same name, we need to check if the file exists.
        // If the file exists, we will keep its ID and update it.
        let mut file_id: Option<String> = None;

        let resp = self.core.gdrive_stat(path).await;
        // We don't care about the error here.
        // As long as the file doesn't exist, we will create a new one.
        if let Ok(resp) = resp {
            let status = resp.status();

            if status == StatusCode::OK {
                let body = resp.into_body().bytes().await?;
                let meta = serde_json::from_slice::<GdriveFile>(&body)
                    .map_err(new_json_deserialize_error)?;

                file_id = if meta.id.is_empty() {
                    None
                } else {
                    Some(meta.id)
                };
            }
        }

        Ok((
            RpWrite::default(),
            GdriveWriter::new(self.core.clone(), String::from(path), file_id),
        ))
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let resp = self.core.gdrive_delete(path).await;
        if let Ok(resp) = resp {
            let status = resp.status();

            match status {
                StatusCode::NO_CONTENT => {
                    let mut cache = self.core.path_cache.lock().await;

                    cache.remove(path);

                    return Ok(RpDelete::default());
                }
                _ => return Err(parse_error(resp).await?),
            }
        };

        let e = resp.err().unwrap();
        if e.kind() == ErrorKind::NotFound {
            Ok(RpDelete::default())
        } else {
            Err(e)
        }
    }
}

impl GdriveBackend {
    pub(crate) fn parse_metadata(&self, body: bytes::Bytes) -> Result<Metadata> {
        let metadata =
            serde_json::from_slice::<GdriveFile>(&body).map_err(new_json_deserialize_error)?;

        let mut meta = Metadata::new(match metadata.mime_type.as_str() {
            "application/vnd.google-apps.folder" => EntryMode::DIR,
            _ => EntryMode::FILE,
        });

        let size = if meta.mode() == EntryMode::DIR {
            // Google Drive does not return the size for folders.
            0
        } else {
            metadata
                .size
                .expect("file size must exist")
                .parse::<u64>()
                .map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "parse content length").set_source(e)
                })?
        };
        meta = meta.with_content_length(size);
        meta = meta.with_last_modified(
            metadata
                .modified_time
                .expect("modified time must exist. please check your query param - fields")
                .parse::<chrono::DateTime<Utc>>()
                .map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "parse last modified time").set_source(e)
                })?,
        );
        Ok(meta)
    }
}
