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

use log::debug;
use smb::{
    CreateDisposition, CreateOptions, DirAccessMask, FileAccessMask, FileAttributes,
    FileCreateArgs, Resource,
};

use super::SMB_SCHEME;
use super::config::SmbConfig;
use super::core::SmbCore;
use super::core::close_resource;
use super::deleter::SmbDeleter;
use super::error::is_already_exists;
use super::error::is_not_found;
use super::error::parse_smb_error;
use super::lister::SmbLister;
use super::reader::SmbReader;
use super::writer::SmbWriter;
use opendal_core::raw::*;
use opendal_core::*;

/// SMB service support.
#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct SmbBuilder {
    pub(super) config: SmbConfig,
}

impl SmbBuilder {
    /// Set endpoint for this backend.
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        self.config.endpoint = if endpoint.is_empty() {
            None
        } else {
            Some(endpoint.to_string())
        };
        self
    }

    /// Set share name for this backend.
    pub fn share(mut self, share: &str) -> Self {
        if !share.is_empty() {
            self.config.share = share.to_string();
        }
        self
    }

    /// Set root path for this backend.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };
        self
    }

    /// Set user for this backend.
    pub fn user(mut self, user: &str) -> Self {
        self.config.user = if user.is_empty() {
            None
        } else {
            Some(user.to_string())
        };
        self
    }

    /// Set password for this backend.
    pub fn password(mut self, password: &str) -> Self {
        self.config.password = if password.is_empty() {
            None
        } else {
            Some(password.to_string())
        };
        self
    }
}

impl Builder for SmbBuilder {
    type Config = SmbConfig;

    fn build(self) -> Result<impl Access> {
        debug!("smb backend build started: {:?}", &self);

        let endpoint = self
            .config
            .endpoint
            .clone()
            .ok_or_else(|| Error::new(ErrorKind::ConfigInvalid, "endpoint is empty"))?;
        let share = if self.config.share.is_empty() {
            return Err(Error::new(ErrorKind::ConfigInvalid, "share is empty"));
        } else {
            self.config.share.clone()
        };
        let root = normalize_root(&self.config.root.clone().unwrap_or_default());

        let info = AccessorInfo::default();
        info.set_scheme(SMB_SCHEME)
            .set_root(&root)
            .set_native_capability(Capability {
                stat: true,

                read: true,

                write: true,
                write_can_multi: true,
                write_with_if_not_exists: true,

                create_dir: true,
                delete: true,

                list: true,

                shared: true,

                ..Default::default()
            });

        let core = Arc::new(SmbCore::new(
            Arc::new(info),
            endpoint,
            share,
            root,
            self.config.user.clone(),
            self.config.password.clone(),
        ));

        debug!("smb backend build finished: {:?}", &self);
        Ok(SmbBackend { core })
    }
}

#[derive(Clone, Debug)]
pub struct SmbBackend {
    core: Arc<SmbCore>,
}

impl Access for SmbBackend {
    type Reader = SmbReader;
    type Writer = SmbWriter;
    type Lister = Option<SmbLister>;
    type Deleter = oio::OneShotDeleter<SmbDeleter>;

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        let client = self.core.connect().await?;
        let path = self.core.build_relative_path(path);
        self.core.ensure_dir_exists(&client, &path).await?;
        Ok(RpCreateDir::default())
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let client = self.core.connect().await?;
        let path = self.core.build_relative_path(path);
        let meta = self.core.stat_path(&client, &path).await?;
        Ok(RpStat::new(meta))
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let client = self.core.connect().await?;
        let path = self.core.build_relative_path(path);
        let unc = self.core.build_unc_path(&path)?;
        let resource = client
            .create_file(
                &unc,
                &FileCreateArgs {
                    disposition: CreateDisposition::Open,
                    attributes: FileAttributes::new(),
                    options: CreateOptions::new().with_non_directory_file(true),
                    desired_access: FileAccessMask::new()
                        .with_generic_read(true)
                        .with_file_read_attributes(true)
                        .with_synchronize(true),
                },
            )
            .await;

        let resource = resource.map_err(parse_smb_error)?;

        let file = match resource {
            Resource::File(file) => file,
            other => {
                close_resource(other).await?;
                return Err(Error::new(
                    ErrorKind::IsADirectory,
                    "read path is a directory",
                ));
            }
        };

        Ok((RpRead::new(), SmbReader::new(client, file, args.range())))
    }

    async fn write(&self, path: &str, op: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let client = self.core.connect().await?;
        let parent = self.core.build_relative_path(get_parent(path));
        let path = self.core.build_relative_path(path);
        self.core.ensure_dir_exists(&client, &parent).await?;

        let unc = self.core.build_unc_path(&path)?;
        let disposition = if op.if_not_exists() {
            CreateDisposition::Create
        } else {
            CreateDisposition::OverwriteIf
        };
        let resource = client
            .create_file(
                &unc,
                &FileCreateArgs {
                    disposition,
                    attributes: FileAttributes::new(),
                    options: CreateOptions::new().with_non_directory_file(true),
                    desired_access: FileAccessMask::new()
                        .with_generic_write(true)
                        .with_file_read_attributes(true)
                        .with_synchronize(true),
                },
            )
            .await;

        let resource = match resource {
            Ok(resource) => resource,
            Err(err) if op.if_not_exists() && is_already_exists(&err) => {
                return Err(
                    Error::new(ErrorKind::ConditionNotMatch, "file already exists").set_source(err),
                );
            }
            Err(err) => return Err(parse_smb_error(err)),
        };

        let file = match resource {
            Resource::File(file) => file,
            other => {
                close_resource(other).await?;
                return Err(Error::new(
                    ErrorKind::IsADirectory,
                    "write path is a directory",
                ));
            }
        };

        Ok((RpWrite::new(), SmbWriter::new(file, client)))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(SmbDeleter::new(self.core.clone())),
        ))
    }

    async fn list(&self, path: &str, _: OpList) -> Result<(RpList, Self::Lister)> {
        let client = self.core.connect().await?;
        let rel_path = if path == "/" {
            "/".to_string()
        } else {
            path.to_string()
        };
        let abs_path = self.core.build_relative_path(path);
        let unc = self.core.build_unc_path(&abs_path)?;

        let resource = match client
            .create_file(
                &unc,
                &FileCreateArgs {
                    disposition: CreateDisposition::Open,
                    attributes: FileAttributes::new(),
                    options: CreateOptions::new().with_directory_file(true),
                    desired_access: DirAccessMask::new()
                        .with_list_directory(true)
                        .with_read_attributes(true)
                        .with_synchronize(true)
                        .into(),
                },
            )
            .await
        {
            Ok(resource) => resource,
            Err(err) if is_not_found(&err) => return Ok((RpList::default(), None)),
            Err(err) => return Err(parse_smb_error(err)),
        };

        let dir = match resource {
            Resource::Directory(dir) => dir,
            other => {
                close_resource(other).await?;
                return Ok((RpList::default(), None));
            }
        };

        Ok((
            RpList::default(),
            Some(SmbLister::new(self.core.clone(), client, rel_path, abs_path, dir).await?),
        ))
    }
}
