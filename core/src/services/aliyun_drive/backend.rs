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
use log::debug;
use serde::Deserialize;
use tokio::sync::Mutex;

use super::core::*;
use super::lister::AliyunDriveLister;
use super::lister::AliyunDriveParent;
use super::reader::AliyunDriveReader;
use super::writer::{AliyunDriveWriter, AliyunDriveWriters};
use crate::raw::*;
use crate::*;

/// Aliyun Drive services support.
#[derive(Default, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct AliyunDriveConfig {
    /// root of this backend.
    ///
    /// All operations will happen under this root.
    ///
    /// default to `/` if not set.
    pub root: Option<String>,
    /// client_id of this backend.
    ///
    /// required.
    pub client_id: String,
    /// client_secret of this backend.
    ///
    /// required.
    pub client_secret: String,
    /// refresh_token of this backend.
    ///
    /// required.
    pub refresh_token: String,
    /// drive_type of this backend.
    ///
    /// All operations will happen under this type of drive.
    ///
    /// Available values are `default`, `backup` and `resource`.
    ///
    /// Fallback to default if not set or no other drives can be found.
    pub drive_type: String,
    /// rapid_upload of this backend.
    ///
    /// Skip uploading files that are already in the drive by hashing their content.
    ///
    /// Only works under the write_once operation.
    pub rapid_upload: bool,
}

impl Debug for AliyunDriveConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("AliyunDriveConfig");

        d.field("root", &self.root)
            .field("drive_type", &self.drive_type);

        d.finish_non_exhaustive()
    }
}

#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct AliyunDriveBuilder {
    config: AliyunDriveConfig,

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
    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// Set client_id of this backend.
    pub fn client_id(&mut self, client_id: &str) -> &mut Self {
        self.config.client_id = client_id.to_string();

        self
    }

    /// Set client_secret of this backend.
    pub fn client_secret(&mut self, client_secret: &str) -> &mut Self {
        self.config.client_secret = client_secret.to_string();

        self
    }

    /// Set refresh_token of this backend.
    pub fn refresh_token(&mut self, refresh_token: &str) -> &mut Self {
        self.config.refresh_token = refresh_token.to_string();

        self
    }

    /// Set drive_type of this backend.
    pub fn drive_type(&mut self, drive_type: &str) -> &mut Self {
        self.config.drive_type = drive_type.to_string();

        self
    }

    /// Set rapid_upload of this backend.
    pub fn rapid_upload(&mut self, rapid_upload: bool) -> &mut Self {
        self.config.rapid_upload = rapid_upload;

        self
    }

    /// Specify the http client that used by this service.
    ///
    /// # Notes
    ///
    /// This API is part of OpenDAL's Raw API. `HttpClient` could be changed
    /// during minor updates.
    pub fn http_client(&mut self, client: HttpClient) -> &mut Self {
        self.http_client = Some(client);
        self
    }
}

impl Builder for AliyunDriveBuilder {
    const SCHEME: Scheme = Scheme::AliyunDrive;

    type Accessor = AliyunDriveBackend;

    fn from_map(map: std::collections::HashMap<String, String>) -> Self {
        let config = AliyunDriveConfig::deserialize(ConfigDeserializer::new(map))
            .expect("config deserialize must succeed");
        AliyunDriveBuilder {
            config,

            http_client: None,
        }
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.config.root.clone().unwrap_or_default());
        debug!("backend use root {}", &root);

        let client = if let Some(client) = self.http_client.take() {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::AliyunDrive)
            })?
        };

        let client_id = self.config.client_id.clone();
        if client_id.is_empty() {
            return Err(
                Error::new(ErrorKind::ConfigInvalid, "client_id is missing.")
                    .with_operation("Builder::build")
                    .with_context("service", Scheme::AliyunDrive),
            );
        }

        let client_secret = self.config.client_secret.clone();
        if client_secret.is_empty() {
            return Err(
                Error::new(ErrorKind::ConfigInvalid, "client_secret is missing.")
                    .with_operation("Builder::build")
                    .with_context("service", Scheme::AliyunDrive),
            );
        }

        let refresh_token = self.config.refresh_token.clone();
        if refresh_token.is_empty() {
            return Err(
                Error::new(ErrorKind::ConfigInvalid, "refresh_token is missing.")
                    .with_operation("Builder::build")
                    .with_context("service", Scheme::AliyunDrive),
            );
        }

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

        let rapid_upload = self.config.rapid_upload;
        debug!("backend use rapid_upload {}", rapid_upload);

        Ok(AliyunDriveBackend {
            core: Arc::new(AliyunDriveCore {
                endpoint: "https://openapi.alipan.com".to_string(),
                root,
                client_id,
                client_secret,
                drive_type,
                rapid_upload,
                signer: Arc::new(Mutex::new(AliyunDriveSigner {
                    drive_id: None,
                    access_token: None,
                    refresh_token,
                    expire_at: 0,
                })),
                client,
            }),
        })
    }
}

#[derive(Clone, Debug)]
pub struct AliyunDriveBackend {
    core: Arc<AliyunDriveCore>,
}

impl Access for AliyunDriveBackend {
    type Reader = AliyunDriveReader;
    type Writer = AliyunDriveWriters;
    type Lister = oio::PageLister<AliyunDriveLister>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();

    fn info(&self) -> AccessorInfo {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::AliyunDrive)
            .set_root(&self.core.root)
            .set_native_capability(Capability {
                stat: true,
                create_dir: true,
                read: true,
                write: true,
                write_can_multi: true,
                // The min multipart size of AliyunDrive is 100 KiB.
                write_multi_min_size: Some(100 * 1024),
                // The max multipart size of AliyunDrive is 5 GiB.
                write_multi_max_size: Some(5 * 1024 * 1024 * 1024),
                delete: true,
                copy: true,
                rename: true,
                list: true,
                list_with_limit: true,

                ..Default::default()
            });
        am
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

        // if from and to are going to be placed in the same folder
        // copy_path will fail as we cannot change name during this action.
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

        let Some(size) = file.size else {
            return Err(Error::new(ErrorKind::Unexpected, "cannot get file size"));
        };

        let download_url = self.core.get_download_url(&file.file_id).await?;

        Ok((
            RpRead::default(),
            AliyunDriveReader::new(self.core.clone(), &download_url, size, args),
        ))
    }

    async fn delete(&self, path: &str, _args: OpDelete) -> Result<RpDelete> {
        let res = match self.core.get_by_path(path).await {
            Ok(output) => Some(output),
            Err(err) if err.kind() == ErrorKind::NotFound => None,
            Err(err) => return Err(err),
        };
        if let Some(res) = res {
            let file: AliyunDriveFile =
                serde_json::from_reader(res.reader()).map_err(new_json_serialize_error)?;
            self.core.delete_path(&file.file_id).await?;
        }
        Ok(RpDelete::default())
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let parent = match self.core.get_by_path(path).await {
            Err(err) if err.kind() == ErrorKind::NotFound => None,
            Err(err) => return Err(err),
            Ok(res) => {
                let file: AliyunDriveFile =
                    serde_json::from_reader(res.reader()).map_err(new_json_serialize_error)?;
                Some(AliyunDriveParent {
                    parent_path: path.to_string(),
                    parent_file_id: file.file_id,
                })
            }
        };

        let l = AliyunDriveLister::new(self.core.clone(), parent, args.limit());

        Ok((RpList::default(), oio::PageLister::new(l)))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let parent_path = get_parent(path);
        let parent_file_id = self.core.ensure_dir_exists(parent_path).await?;

        let executor = args.executor().cloned();

        let writer =
            AliyunDriveWriter::new(self.core.clone(), &parent_file_id, get_basename(path), args);

        let w = oio::MultipartWriter::new(writer, executor, 1);

        Ok((RpWrite::default(), w))
    }
}
