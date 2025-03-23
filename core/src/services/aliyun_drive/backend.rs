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
use std::fmt::Formatter;
use std::sync::Arc;

use bytes::Buf;
use chrono::Utc;
use http::header;
use http::Request;
use http::Response;
use http::StatusCode;
use log::debug;
use tokio::sync::Mutex;

use super::core::*;
use super::delete::AliyunDriveDeleter;
use super::error::parse_error;
use super::lister::AliyunDriveLister;
use super::lister::AliyunDriveParent;
use super::writer::AliyunDriveWriter;
use crate::raw::*;
use crate::services::AliyunDriveConfig;
use crate::*;

impl Configurator for AliyunDriveConfig {
    type Builder = AliyunDriveBuilder;

    #[allow(deprecated)]
    fn into_builder(self) -> Self::Builder {
        AliyunDriveBuilder {
            config: self,
            http_client: None,
        }
    }
}

#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct AliyunDriveBuilder {
    config: AliyunDriveConfig,

    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
    http_client: Option<HttpClient>,
}

impl Debug for AliyunDriveBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("AliyunDriveBuilder");

        d.field("config", &self.config);
        d.finish_non_exhaustive()
    }
}

impl AliyunDriveBuilder {
    /// Set the root of this backend.
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

    /// Set access_token of this backend.
    pub fn access_token(mut self, access_token: &str) -> Self {
        self.config.access_token = Some(access_token.to_string());

        self
    }

    /// Set client_id of this backend.
    pub fn client_id(mut self, client_id: &str) -> Self {
        self.config.client_id = Some(client_id.to_string());

        self
    }

    /// Set client_secret of this backend.
    pub fn client_secret(mut self, client_secret: &str) -> Self {
        self.config.client_secret = Some(client_secret.to_string());

        self
    }

    /// Set refresh_token of this backend.
    pub fn refresh_token(mut self, refresh_token: &str) -> Self {
        self.config.refresh_token = Some(refresh_token.to_string());

        self
    }

    /// Set drive_type of this backend.
    pub fn drive_type(mut self, drive_type: &str) -> Self {
        self.config.drive_type = drive_type.to_string();

        self
    }

    /// Specify the http client that used by this service.
    ///
    /// # Notes
    ///
    /// This API is part of OpenDAL's Raw API. `HttpClient` could be changed
    /// during minor updates.
    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
    #[allow(deprecated)]
    pub fn http_client(mut self, client: HttpClient) -> Self {
        self.http_client = Some(client);
        self
    }
}

impl Builder for AliyunDriveBuilder {
    const SCHEME: Scheme = Scheme::AliyunDrive;
    type Config = AliyunDriveConfig;

    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.config.root.clone().unwrap_or_default());
        debug!("backend use root {}", &root);

        let sign = match self.config.access_token.clone() {
            Some(access_token) if !access_token.is_empty() => {
                AliyunDriveSign::Access(access_token)
            }
            _ => match (
                self.config.client_id.clone(),
                self.config.client_secret.clone(),
                self.config.refresh_token.clone(),
            ) {
                (Some(client_id), Some(client_secret), Some(refresh_token)) if
                !client_id.is_empty() && !client_secret.is_empty() && !refresh_token.is_empty() => {
                    AliyunDriveSign::Refresh(client_id, client_secret, refresh_token, None, 0)
                }
                _ => return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "access_token and a set of client_id, client_secret, and refresh_token are both missing.")
                    .with_operation("Builder::build")
                    .with_context("service", Scheme::AliyunDrive)),
            },
        };

        let drive_type = match self.config.drive_type.as_str() {
            "" | "default" => DriveType::Default,
            "resource" => DriveType::Resource,
            "backup" => DriveType::Backup,
            _ => {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "drive_type is invalid.",
                ))
            }
        };
        debug!("backend use drive_type {:?}", drive_type);

        Ok(AliyunDriveBackend {
            core: Arc::new(AliyunDriveCore {
                info: {
                    let am = AccessorInfo::default();
                    am.set_scheme(Scheme::AliyunDrive)
                        .set_root(&root)
                        .set_native_capability(Capability {
                            stat: true,
                            create_dir: true,
                            read: true,
                            write: true,
                            write_can_multi: true,
                            // The min multipart size of AliyunDrive is 100 KiB.
                            write_multi_min_size: Some(100 * 1024),
                            // The max multipart size of AliyunDrive is 5 GiB.
                            write_multi_max_size: if cfg!(target_pointer_width = "64") {
                                Some(5 * 1024 * 1024 * 1024)
                            } else {
                                Some(usize::MAX)
                            },
                            delete: true,
                            copy: true,
                            rename: true,
                            list: true,
                            list_with_limit: true,
                            shared: true,
                            stat_has_content_length: true,
                            stat_has_content_type: true,
                            list_has_last_modified: true,
                            list_has_content_length: true,
                            list_has_content_type: true,
                            ..Default::default()
                        });

                    // allow deprecated api here for compatibility
                    #[allow(deprecated)]
                    if let Some(client) = self.http_client {
                        am.update_http_client(|_| client);
                    }

                    am.into()
                },
                endpoint: "https://openapi.alipan.com".to_string(),
                root,
                drive_type,
                signer: Arc::new(Mutex::new(AliyunDriveSigner {
                    drive_id: None,
                    sign,
                })),
                dir_lock: Arc::new(Mutex::new(())),
            }),
        })
    }
}

#[derive(Clone, Debug)]
pub struct AliyunDriveBackend {
    core: Arc<AliyunDriveCore>,
}

impl Access for AliyunDriveBackend {
    type Reader = HttpBody;
    type Writer = AliyunDriveWriter;
    type Lister = oio::PageLister<AliyunDriveLister>;
    type Deleter = oio::OneShotDeleter<AliyunDriveDeleter>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn create_dir(&self, path: &str, _args: OpCreateDir) -> Result<RpCreateDir> {
        self.core.ensure_dir_exists(path).await?;

        Ok(RpCreateDir::default())
    }

    async fn rename(&self, from: &str, to: &str, _args: OpRename) -> Result<RpRename> {
        if from == to {
            return Ok(RpRename::default());
        }
        let res = self.core.get_by_path(from).await?;
        let file: AliyunDriveFile =
            serde_json::from_reader(res.reader()).map_err(new_json_serialize_error)?;
        // rename can overwrite.
        match self.core.get_by_path(to).await {
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => return Err(err),
            Ok(res) => {
                let file: AliyunDriveFile =
                    serde_json::from_reader(res.reader()).map_err(new_json_serialize_error)?;
                self.core.delete_path(&file.file_id).await?;
            }
        };

        let parent_file_id = self.core.ensure_dir_exists(get_parent(to)).await?;
        self.core.move_path(&file.file_id, &parent_file_id).await?;

        let from_name = get_basename(from);
        let to_name = get_basename(to);

        if from_name != to_name {
            self.core.update_path(&file.file_id, to_name).await?;
        }

        Ok(RpRename::default())
    }

    async fn copy(&self, from: &str, to: &str, _args: OpCopy) -> Result<RpCopy> {
        if from == to {
            return Ok(RpCopy::default());
        }
        let res = self.core.get_by_path(from).await?;
        let file: AliyunDriveFile =
            serde_json::from_reader(res.reader()).map_err(new_json_serialize_error)?;
        // copy can overwrite.
        match self.core.get_by_path(to).await {
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => return Err(err),
            Ok(res) => {
                let file: AliyunDriveFile =
                    serde_json::from_reader(res.reader()).map_err(new_json_serialize_error)?;
                self.core.delete_path(&file.file_id).await?;
            }
        };
        // there is no direct copy in AliyunDrive.
        // so we need to copy the path first and then rename it.
        let parent_path = get_parent(to);
        let parent_file_id = self.core.ensure_dir_exists(parent_path).await?;

        // if from and to are going to be placed in the same folder,
        // copy_path will fail as we cannot change the name during this action.
        // it has to be auto renamed.
        let auto_rename = file.parent_file_id == parent_file_id;
        let res = self
            .core
            .copy_path(&file.file_id, &parent_file_id, auto_rename)
            .await?;
        let file: CopyResponse =
            serde_json::from_reader(res.reader()).map_err(new_json_serialize_error)?;
        let file_id = file.file_id;

        let from_name = get_basename(from);
        let to_name = get_basename(to);

        if from_name != to_name {
            self.core.update_path(&file_id, to_name).await?;
        }

        Ok(RpCopy::default())
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        let res = self.core.get_by_path(path).await?;
        let file: AliyunDriveFile =
            serde_json::from_reader(res.reader()).map_err(new_json_serialize_error)?;

        if file.path_type == "folder" {
            let meta = Metadata::new(EntryMode::DIR).with_last_modified(
                file.updated_at
                    .parse::<chrono::DateTime<Utc>>()
                    .map_err(|e| {
                        Error::new(ErrorKind::Unexpected, "parse last modified time").set_source(e)
                    })?,
            );

            return Ok(RpStat::new(meta));
        }

        let mut meta = Metadata::new(EntryMode::FILE).with_last_modified(
            file.updated_at
                .parse::<chrono::DateTime<Utc>>()
                .map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "parse last modified time").set_source(e)
                })?,
        );
        if let Some(v) = file.size {
            meta = meta.with_content_length(v);
        }
        if let Some(v) = file.content_type {
            meta = meta.with_content_type(v);
        }

        Ok(RpStat::new(meta))
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let res = self.core.get_by_path(path).await?;
        let file: AliyunDriveFile =
            serde_json::from_reader(res.reader()).map_err(new_json_serialize_error)?;

        let download_url = self.core.get_download_url(&file.file_id).await?;
        let req = Request::get(&download_url)
            .header(header::RANGE, args.range().to_header())
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        let resp = self.core.info.http_client().fetch(req).await?;
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

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(AliyunDriveDeleter::new(self.core.clone())),
        ))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let parent = match self.core.get_by_path(path).await {
            Err(err) if err.kind() == ErrorKind::NotFound => None,
            Err(err) => return Err(err),
            Ok(res) => {
                let file: AliyunDriveFile =
                    serde_json::from_reader(res.reader()).map_err(new_json_serialize_error)?;
                Some(AliyunDriveParent {
                    file_id: file.file_id,
                    path: path.to_string(),
                    updated_at: file.updated_at,
                })
            }
        };

        let l = AliyunDriveLister::new(self.core.clone(), parent, args.limit());

        Ok((RpList::default(), oio::PageLister::new(l)))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let parent_path = get_parent(path);
        let parent_file_id = self.core.ensure_dir_exists(parent_path).await?;

        // write can overwrite
        match self.core.get_by_path(path).await {
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => return Err(err),
            Ok(res) => {
                let file: AliyunDriveFile =
                    serde_json::from_reader(res.reader()).map_err(new_json_serialize_error)?;
                self.core.delete_path(&file.file_id).await?;
            }
        };

        let writer =
            AliyunDriveWriter::new(self.core.clone(), &parent_file_id, get_basename(path), args);

        Ok((RpWrite::default(), writer))
    }
}
