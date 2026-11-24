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
use openssh::KnownHosts;

use super::SFTP_SCHEME;
use super::config::SftpConfig;
use super::core::Manager;
use super::core::SftpCore;
use super::core::create_dir_all;
use super::core::parse_sftp_error;
use super::core::to_metadata;
use super::deleter::SftpDeleter;
use super::reader::*;
use opendal_core::raw::*;
use opendal_core::*;

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
#[derive(Debug, Default)]
pub struct SftpBuilder {
    pub(super) config: SftpConfig,
    pub(super) acquire_timeout: Option<Duration>,
    pub(super) connect_timeout: Option<Duration>,
}

impl SftpBuilder {
    /// Sets the time limit for waiting for a connection pool slot.
    ///
    /// Once a slot is available, [`Self::connect_timeout`] applies to connection setup
    /// and pooled connection health checks. Defaults to 10 seconds. A zero duration
    /// disables the pool wait limit.
    pub fn acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = Some(timeout);
        self
    }

    /// Sets the time limit for each connection setup stage and each pooled connection health check.
    ///
    /// The limit applies separately to SSH connection and authentication, SFTP subsystem
    /// initialization, and root directory initialization. Defaults to 10 seconds.
    /// A zero duration disables these limits.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

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

    /// set control directory for sftp backend.
    /// The SSH client creates a temporary directory holding the control socket here.
    /// Defaults to `$XDG_STATE_HOME` or `$HOME/.local/state`, falling back to the current directory.
    pub fn control_directory(mut self, dir: &str) -> Self {
        self.config.control_directory = if dir.is_empty() {
            None
        } else {
            Some(dir.to_string())
        };

        self
    }

    /// Deprecated: SFTP copy capability is enabled by default.
    #[deprecated(
        since = "0.57.0",
        note = "SFTP copy capability is enabled by default and this option is no longer needed."
    )]
    pub fn enable_copy(self, _enable_copy: bool) -> Self {
        self
    }
}

impl Builder for SftpBuilder {
    type Config = SftpConfig;

    fn build(self) -> Result<impl Service> {
        debug!("sftp backend build started: {:?}", self);
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
                        format!("unknown known_hosts strategy: {v}").as_str(),
                    ));
                }
            }
            None => KnownHosts::Strict,
        };

        const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
        let resolve_timeout = |override_value, configured: Option<SignedDuration>, key| {
            let timeout = match override_value {
                Some(timeout) => timeout,
                None => configured
                    .map(signed_duration_to_duration)
                    .transpose()
                    .map_err(|err| err.with_context("config", key))?
                    .unwrap_or(DEFAULT_TIMEOUT),
            };
            Ok::<_, Error>((!timeout.is_zero()).then_some(timeout))
        };
        let acquire_timeout = resolve_timeout(
            self.acquire_timeout,
            self.config.acquire_timeout,
            "acquire_timeout",
        )?;
        let connect_timeout = resolve_timeout(
            self.connect_timeout,
            self.config.connect_timeout,
            "connect_timeout",
        )?;

        let info = ServiceInfo::new(SFTP_SCHEME, root.as_str(), "");
        let capability = Capability {
            stat: true,

            read: true,

            write: true,
            write_can_multi: true,
            write_with_if_not_exists: true,

            create_dir: true,
            delete: true,

            list: true,

            copy: true,
            rename: true,

            shared: true,

            ..Default::default()
        };

        let core = Arc::new(SftpCore::new(
            info,
            capability,
            Manager {
                endpoint,
                root,
                user,
                key: self.config.key.clone(),
                known_hosts_strategy,
                connect_timeout,
                #[cfg(not(windows))]
                control_directory: self.config.control_directory.clone(),
            },
            acquire_timeout,
        ));

        debug!("sftp backend finished: {:?}", self);
        Ok(SftpBackend { core })
    }
}

#[derive(Clone, Debug)]
pub struct SftpBackend {
    pub core: Arc<SftpCore>,
}

impl Service for SftpBackend {
    type Reader = oio::StreamReader<SftpReader>;
    type Writer = SftpLazyWriter;
    type Lister = SftpLazyLister;
    type Deleter = oio::OneShotDeleter<SftpDeleter>;
    type Copier = oio::OneShotCopier;
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        self.core.info.clone()
    }

    fn capability(&self) -> Capability {
        self.core.capability
    }

    async fn create_dir(
        &self,
        _ctx: &OperationContext,
        path: &str,
        _: OpCreateDir,
    ) -> Result<RpCreateDir> {
        let client = self.core.connect().await?;
        create_dir_all(&client, &self.core.root, path.trim_start_matches('/')).await?;

        Ok(RpCreateDir::default())
    }

    async fn stat(&self, _ctx: &OperationContext, path: &str, _: OpStat) -> Result<RpStat> {
        let client = self.core.connect().await?;
        let mut fs = client.fs();
        fs.set_cwd(&self.core.root);

        let meta: Metadata = to_metadata(fs.metadata(path).await.map_err(parse_sftp_error)?)?;

        Ok(RpStat::new(meta))
    }
    fn read(&self, _ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let output: oio::StreamReader<SftpReader> = {
            Ok(oio::StreamReader::new(SftpReader::new(
                self.clone(),
                path,
                args,
            )))
        }?;

        Ok(output)
    }

    fn write(&self, _ctx: &OperationContext, path: &str, op: OpWrite) -> Result<Self::Writer> {
        Ok(SftpLazyWriter::new(self.clone(), path, op))
    }

    fn delete(&self, _ctx: &OperationContext) -> Result<Self::Deleter> {
        let output: oio::OneShotDeleter<SftpDeleter> = {
            Ok(oio::OneShotDeleter::new(SftpDeleter::new(
                self.core.clone(),
            )))
        }?;

        Ok(output)
    }

    fn list(&self, _ctx: &OperationContext, path: &str, _: OpList) -> Result<Self::Lister> {
        Ok(SftpLazyLister::new(self.clone(), path))
    }

    fn copy(
        &self,
        _ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> Result<Self::Copier> {
        let backend = self.clone();
        let from = from.to_string();
        let to = to.to_string();
        let source_content_length_hint = args.source_content_length_hint();

        Ok(oio::OneShotCopier::new_with(move || {
            let backend = backend.clone();
            let from = from.clone();
            let to = to.clone();

            async move {
                let client = backend.core.connect().await?;
                if !client.support_copy() {
                    return Err(Error::new(
                        ErrorKind::Unsupported,
                        "sftp server does not support copy-data extension",
                    ));
                }

                let mut fs = client.fs();
                fs.set_cwd(&backend.core.root);

                let src = fs.canonicalize(&from).await.map_err(parse_sftp_error)?;
                let mut src_file = client.open(&src).await.map_err(parse_sftp_error)?;

                let source_size = match source_content_length_hint {
                    Some(size) => size,
                    None => match src_file.metadata().await.map_err(parse_sftp_error) {
                        Ok(meta) => match meta.len() {
                            Some(len) => len,
                            None => {
                                let _ = src_file.close().await;
                                return Err(Error::new(
                                    ErrorKind::Unexpected,
                                    "sftp source metadata does not contain file length",
                                ));
                            }
                        },
                        Err(err) => {
                            let _ = src_file.close().await;
                            return Err(err);
                        }
                    },
                };

                if let Some((dir, _)) = to.rsplit_once('/')
                    && let Err(err) =
                        create_dir_all(&client, &backend.core.root, dir.trim_start_matches('/'))
                            .await
                {
                    let _ = src_file.close().await;
                    return Err(err);
                }

                let dst = match fs.canonicalize(&to).await.map_err(parse_sftp_error) {
                    Ok(dst) => dst,
                    Err(err) => {
                        let _ = src_file.close().await;
                        return Err(err);
                    }
                };
                if src == dst {
                    let _ = src_file.close().await;
                    return Err(Error::new(
                        ErrorKind::IsSameFile,
                        "copy source and destination resolve to the same file",
                    ));
                }

                let mut dst_file = match client.create(dst).await.map_err(parse_sftp_error) {
                    Ok(dst_file) => dst_file,
                    Err(err) => {
                        let _ = src_file.close().await;
                        return Err(err);
                    }
                };

                let copy_res = src_file
                    .copy_all_to(&mut dst_file)
                    .await
                    .map_err(parse_sftp_error);
                let dst_close_res = dst_file.close().await.map_err(parse_sftp_error);
                let src_close_res = src_file.close().await.map_err(parse_sftp_error);

                copy_res?;
                dst_close_res?;
                src_close_res?;

                Ok(MetadataBuilder::file(source_size).build())
            }
        }))
    }

    async fn rename(
        &self,
        _ctx: &OperationContext,
        from: &str,
        to: &str,
        _: OpRename,
    ) -> Result<RpRename> {
        let client = self.core.connect().await?;

        let mut fs = client.fs();
        fs.set_cwd(&self.core.root);

        if let Some((dir, _)) = to.rsplit_once('/') {
            // Verify source exists before creating destination parent directories.
            fs.metadata(from).await.map_err(parse_sftp_error)?;
            create_dir_all(&client, &self.core.root, dir.trim_start_matches('/')).await?;
        }
        fs.rename(from, to).await.map_err(parse_sftp_error)?;

        Ok(RpRename::default())
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
