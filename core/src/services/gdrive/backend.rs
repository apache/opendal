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
use crate::types::Result;
use crate::*;

#[derive(Clone, Debug)]
pub struct GdriveBackend {
    core: Arc<GdriveCore>,
}

impl GdriveBackend {
    pub(crate) fn new(root: String, access_token: String, http_client: HttpClient) -> Self {
        GdriveBackend {
            core: Arc::new(GdriveCore {
                root,
                access_token,
                client: http_client,
                path_cache: Arc::default(),
            }),
        }
    }
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
        ma.set_scheme(crate::Scheme::Gdrive)
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

        println!("status out: {:?}", status);

        match status {
            StatusCode::OK => {
                let meta = self.parse_metadata(resp.into_body().bytes().await?)?;
                Ok(RpStat::new(meta))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn create_dir(&self, path: &str, _args: OpCreateDir) -> Result<RpCreateDir> {
        let resp = self.core.gdrive_create_dir(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(RpCreateDir::default()),
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
        Ok((
            RpWrite::default(),
            GdriveWriter::new(self.core.clone(), args, String::from(path), None),
        ))
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let resp = self.core.gdrive_delete(path).await?;

        let status = resp.status();

        match status {
            StatusCode::NO_CONTENT => Ok(RpDelete::default()),
            _ => Err(parse_error(resp).await?),
        }
    }
}

impl GdriveBackend {
    pub(crate) fn parse_metadata(&self, body: bytes::Bytes) -> Result<Metadata> {
        let metadata =
            serde_json::from_slice::<GdriveFile>(&body).map_err(new_json_deserialize_error)?;

        println!("metadata: {:?}", metadata.size);

        let mut meta = Metadata::new(match metadata.mime_type.as_str() {
            "application/vnd.google-apps.folder" => EntryMode::DIR,
            _ => EntryMode::FILE,
        });
        meta = meta.with_content_length(metadata.size.parse::<u64>().unwrap_or(0));
        meta = meta.with_last_modified(
            metadata
                .modified_time
                .parse::<chrono::DateTime<Utc>>()
                .unwrap_or_default(),
        );
        Ok(meta)
    }
}
