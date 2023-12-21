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
use bytes::Bytes;
use chrono::Utc;
use http::Request;
use http::StatusCode;
use serde_json::json;

use super::core::GdriveCore;
use super::error::parse_error;
use super::lister::GdriveLister;
use super::writer::GdriveWriter;
use crate::raw::*;
use crate::services::gdrive::core::GdriveFile;
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
    type Writer = oio::OneShotWriter<GdriveWriter>;
    type BlockingWriter = ();
    type Lister = oio::PageLister<GdriveLister>;
    type BlockingLister = ();

    fn info(&self) -> AccessorInfo {
        let mut ma = AccessorInfo::default();
        ma.set_scheme(Scheme::Gdrive)
            .set_root(&self.core.root)
            .set_native_capability(Capability {
                stat: true,

                read: true,

                list: true,

                write: true,

                create_dir: true,

                rename: true,

                delete: true,

                copy: true,

                ..Default::default()
            });

        ma
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        let resp = self.core.gdrive_stat(path).await?;

        if resp.status() != StatusCode::OK {
            return Err(parse_error(resp).await?);
        }

        let meta = self.parse_metadata(resp.into_body().bytes().await?)?;
        Ok(RpStat::new(meta))
    }

    async fn create_dir(&self, path: &str, _args: OpCreateDir) -> Result<RpCreateDir> {
        let parent = self.core.ensure_parent_path(path).await?;

        // Make sure `/` has been trimmed.
        let path = get_basename(path).trim_end_matches('/');

        // As Google Drive allows files have the same name, we need to check if the folder exists.
        let folder_id = self.core.gdrive_search_folder(&parent, path).await?;

        let id = if let Some(id) = folder_id {
            id
        } else {
            self.core.gdrive_create_folder(&parent, path).await?
        };

        let mut cache = self.core.path_cache.lock().await;
        cache.insert(build_abs_path(&self.core.root, path), id);

        Ok(RpCreateDir::default())
    }

    async fn read(&self, path: &str, _args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.core.gdrive_get(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let size = parse_content_length(resp.headers())?;
                let range = parse_content_range(resp.headers())?;
                Ok((
                    RpRead::new().with_size(size).with_range(range),
                    resp.into_body(),
                ))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn write(&self, path: &str, _: OpWrite) -> Result<(RpWrite, Self::Writer)> {
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
            oio::OneShotWriter::new(GdriveWriter::new(
                self.core.clone(),
                String::from(path),
                file_id,
            )),
        ))
    }

    async fn rename(&self, from: &str, to: &str, _args: OpRename) -> Result<RpRename> {
        let resp = self.core.gdrive_patch_metadata_request(from, to).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let body = resp.into_body().bytes().await?;
                let meta = serde_json::from_slice::<GdriveFile>(&body)
                    .map_err(new_json_deserialize_error)?;

                let mut cache = self.core.path_cache.lock().await;

                cache.remove(&build_abs_path(&self.core.root, from));
                cache.insert(build_abs_path(&self.core.root, to), meta.id.clone());

                Ok(RpRename::default())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let resp = self.core.gdrive_delete(path).await;
        if let Ok(resp) = resp {
            let status = resp.status();

            match status {
                StatusCode::NO_CONTENT | StatusCode::NOT_FOUND => {
                    let mut cache = self.core.path_cache.lock().await;

                    cache.remove(&build_abs_path(&self.core.root, path));

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

    async fn list(&self, path: &str, _args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = GdriveLister::new(path.into(), self.core.clone());
        Ok((RpList::default(), oio::PageLister::new(l)))
    }

    async fn copy(&self, from: &str, to: &str, _args: OpCopy) -> Result<RpCopy> {
        let from_file_id = self
            .core
            .get_file_id_by_path(from)
            .await?
            .ok_or(Error::new(ErrorKind::NotFound, "invalid 'from' path"))?;

        // split `to` into parent and name according to the last `/`
        let mut to_path_items: Vec<&str> = to.split('/').filter(|&x| !x.is_empty()).collect();

        let to_name = if let Some(name) = to_path_items.pop() {
            name
        } else {
            return Err(Error::new(ErrorKind::InvalidInput, "invalid 'to' path"));
        };

        let to_parent = to_path_items.join("/") + "/";

        let to_parent_id =
            if let Some(id) = self.core.get_file_id_by_path(to_parent.as_str()).await? {
                id
            } else {
                self.create_dir(&to_parent, OpCreateDir::new()).await?;
                self.core
                    .get_file_id_by_path(to_parent.as_str())
                    .await?
                    .ok_or_else(|| {
                        Error::new(ErrorKind::Unexpected, "create to's parent folder failed")
                    })?
            };

        // copy will overwrite `to`, delete it if exist
        if self
            .core
            .get_file_id_by_path(to)
            .await
            .is_ok_and(|id| id.is_some())
        {
            self.delete(to, OpDelete::new()).await?;
        }

        let url = format!(
            "https://www.googleapis.com/drive/v3/files/{}/copy",
            from_file_id
        );

        let request_body = &json!({
            "name": to_name,
            "parents": [to_parent_id],
        });
        let body = AsyncBody::Bytes(Bytes::from(request_body.to_string()));

        let mut req = Request::post(&url)
            .body(body)
            .map_err(new_request_build_error)?;
        self.core.sign(&mut req).await?;

        let resp = self.core.client.send(req).await?;

        match resp.status() {
            StatusCode::OK => Ok(RpCopy::default()),
            _ => Err(parse_error(resp).await?),
        }
    }
}

impl GdriveBackend {
    pub(crate) fn parse_metadata(&self, body: Bytes) -> Result<Metadata> {
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
