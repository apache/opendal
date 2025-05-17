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
use std::io::SeekFrom;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use log::debug;
use openssh::KnownHosts;
use tokio::io::AsyncSeekExt;
use tokio::sync::OnceCell;

use super::core::SftpCore;
use super::delete::SftpDeleter;
use super::error::is_not_found;
use super::error::is_sftp_protocol_error;
use super::error::parse_sftp_error;
use super::lister::SftpLister;
use super::reader::SftpReader;
use super::writer::SftpWriter;
use crate::raw::*;
use crate::services::SftpConfig;
use crate::*;

impl Configurator for SftpConfig {
    type Builder = SftpBuilder;
    fn into_builder(self) -> Self::Builder {
        SftpBuilder { config: self }
    }
}

/// SFTP services support. (only works on unix)
///
/// If you are interested in working on windows, please refer to [this](https://github.com/apache/opendal/issues/2963) issue.
/// Welcome to leave your comments or make contributions.
///
/// Warning: Maximum number of file holdings is depending on the remote system configuration.
///
/// For example, the default value is 255 in macOS, and 1024 in linux. If you want to open
/// lots of files, you should pay attention to close the file after using it.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct SftpBuilder {
    config: SftpConfig,
}

impl Debug for SftpBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SftpBuilder")
            .field("config", &self.config)
            .finish()
    }
}

impl SftpBuilder {
    /// set endpoint for sftp backend.
    /// The format is same as `openssh`, using either `[user@]hostname` or `ssh://[user@]hostname[:port]`. A username or port that is specified in the endpoint overrides the one set in the builder (but does not change the builder).
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        self.config.endpoint = if endpoint.is_empty() {
            None
        } else {
            Some(endpoint.to_string())
        };

        self
    }

    /// set root path for sftp backend.
    /// It uses the default directory set by the remote `sftp-server` as default.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// set user for sftp backend.
    pub fn user(mut self, user: &str) -> Self {
        self.config.user = if user.is_empty() {
            None
        } else {
            Some(user.to_string())
        };

        self
    }

    /// set key path for sftp backend.
    pub fn key(mut self, key: &str) -> Self {
        self.config.key = if key.is_empty() {
            None
        } else {
            Some(key.to_string())
        };

        self
    }

    /// set known_hosts strategy for sftp backend.
    /// available values:
    /// - Strict (default)
    /// - Accept
    /// - Add
    pub fn known_hosts_strategy(mut self, strategy: &str) -> Self {
        self.config.known_hosts_strategy = if strategy.is_empty() {
            None
        } else {
            Some(strategy.to_string())
        };

        self
    }

    /// set enable_copy for sftp backend.
    /// It requires the server supports copy-file extension.
    pub fn enable_copy(mut self, enable_copy: bool) -> Self {
        self.config.enable_copy = enable_copy;

        self
    }
}

impl Builder for SftpBuilder {
    const SCHEME: Scheme = Scheme::Sftp;
    type Config = SftpConfig;

    fn build(self) -> Result<impl Access> {
        debug!("sftp backend build started: {:?}", &self);
        let endpoint = match self.config.endpoint.clone() {
            Some(v) => v,
            None => return Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")),
        };

        let user = self.config.user.clone();

        let root = self
            .config
            .root
            .clone()
            .map(|r| normalize_root(r.as_str()))
            .unwrap_or_default();

        let known_hosts_strategy = match &self.config.known_hosts_strategy {
            Some(v) => {
                let v = v.to_lowercase();
                if v == "strict" {
                    KnownHosts::Strict
                } else if v == "accept" {
                    KnownHosts::Accept
                } else if v == "add" {
                    KnownHosts::Add
                } else {
                    return Err(Error::new(
                        ErrorKind::ConfigInvalid,
                        format!("unknown known_hosts strategy: {}", v).as_str(),
                    ));
                }
            }
            None => KnownHosts::Strict,
        };

        let info = AccessorInfo::default();
        info.set_root(root.as_str())
            .set_scheme(Scheme::Sftp)
            .set_native_capability(Capability {
                stat: true,
                stat_has_content_length: true,
                stat_has_last_modified: true,

                read: true,

                write: true,
                write_can_multi: true,

                create_dir: true,
                delete: true,

                list: true,
                list_with_limit: true,
                list_has_content_length: true,
                list_has_last_modified: true,

                copy: self.config.enable_copy,
                rename: true,

                shared: true,

                ..Default::default()
            });

        let accessor_info = Arc::new(info);
        let core = Arc::new(SftpCore {
            info: accessor_info,
            endpoint,
            root,
            user,
            key: self.config.key.clone(),
            known_hosts_strategy,

            client: OnceCell::new(),
        });

        debug!("sftp backend finished: {:?}", &self);
        Ok(SftpBackend { core })
    }
}

/// Backend is used to serve `Accessor` support for sftp.
#[derive(Clone)]
pub struct SftpBackend {
    pub core: Arc<SftpCore>,
}

impl Debug for SftpBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SftpBackend")
            .field("core", &self.core)
            .finish()
    }
}

impl Access for SftpBackend {
    type Reader = SftpReader;
    type Writer = SftpWriter;
    type Lister = Option<SftpLister>;
    type Deleter = oio::OneShotDeleter<SftpDeleter>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        let client = self.core.connect().await?;
        let mut fs = client.fs();
        fs.set_cwd(&self.core.root);

        let paths = Path::new(&path).components();
        let mut current = PathBuf::from(&self.core.root);
        for p in paths {
            current = current.join(p);
            let res = fs.create_dir(p).await;

            if let Err(e) = res {
                // ignore error if dir already exists
                if !is_sftp_protocol_error(&e) {
                    return Err(parse_sftp_error(e));
                }
            }
            fs.set_cwd(&current);
        }

        Ok(RpCreateDir::default())
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let client = self.core.connect().await?;
        let mut fs = client.fs();
        fs.set_cwd(&self.core.root);

        let meta: Metadata = fs.metadata(path).await.map_err(parse_sftp_error)?.into();

        Ok(RpStat::new(meta))
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let client = self.core.connect().await?;

        let mut fs = client.fs();
        fs.set_cwd(&self.core.root);

        let path = fs.canonicalize(path).await.map_err(parse_sftp_error)?;

        let mut f = client
            .open(path.as_path())
            .await
            .map_err(parse_sftp_error)?;

        if args.range().offset() != 0 {
            f.seek(SeekFrom::Start(args.range().offset()))
                .await
                .map_err(new_std_io_error)?;
        }

        Ok((
            RpRead::default(),
            SftpReader::new(client, f, args.range().size()),
        ))
    }

    async fn write(&self, path: &str, op: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        if let Some((dir, _)) = path.rsplit_once('/') {
            self.create_dir(dir, OpCreateDir::default()).await?;
        }

        let client = self.core.connect().await?;

        let mut fs = client.fs();
        fs.set_cwd(&self.core.root);
        let path = fs.canonicalize(path).await.map_err(parse_sftp_error)?;

        let mut option = client.options();
        option.create(true);
        if op.append() {
            option.append(true);
        } else {
            option.write(true).truncate(true);
        }

        let file = option.open(path).await.map_err(parse_sftp_error)?;

        Ok((RpWrite::new(), SftpWriter::new(file)))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(SftpDeleter::new(self.core.clone())),
        ))
    }

    async fn list(&self, path: &str, _: OpList) -> Result<(RpList, Self::Lister)> {
        let client = self.core.connect().await?;
        let mut fs = client.fs();
        fs.set_cwd(&self.core.root);

        let file_path = format!("./{}", path);

        let dir = match fs.open_dir(&file_path).await {
            Ok(dir) => dir,
            Err(e) => {
                if is_not_found(&e) {
                    return Ok((RpList::default(), None));
                } else {
                    return Err(parse_sftp_error(e));
                }
            }
        }
        .read_dir();

        Ok((
            RpList::default(),
            Some(SftpLister::new(dir, path.to_owned())),
        ))
    }

    async fn copy(&self, from: &str, to: &str, _: OpCopy) -> Result<RpCopy> {
        let client = self.core.connect().await?;

        let mut fs = client.fs();
        fs.set_cwd(&self.core.root);

        if let Some((dir, _)) = to.rsplit_once('/') {
            self.create_dir(dir, OpCreateDir::default()).await?;
        }

        let src = fs.canonicalize(from).await.map_err(parse_sftp_error)?;
        let dst = fs.canonicalize(to).await.map_err(parse_sftp_error)?;
        let mut src_file = client.open(&src).await.map_err(parse_sftp_error)?;
        let mut dst_file = client.create(dst).await.map_err(parse_sftp_error)?;

        src_file
            .copy_all_to(&mut dst_file)
            .await
            .map_err(parse_sftp_error)?;

        Ok(RpCopy::default())
    }

    async fn rename(&self, from: &str, to: &str, _: OpRename) -> Result<RpRename> {
        let client = self.core.connect().await?;

        let mut fs = client.fs();
        fs.set_cwd(&self.core.root);

        if let Some((dir, _)) = to.rsplit_once('/') {
            self.create_dir(dir, OpCreateDir::default()).await?;
        }
        fs.rename(from, to).await.map_err(parse_sftp_error)?;

        Ok(RpRename::default())
    }
}
