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
use russh_sftp::client::error::Error as SftpClientError;
use russh_sftp::extensions::HardlinkExtension;
use russh_sftp::protocol::{FileAttributes, OpenFlags, Packet, StatusCode};

use super::SFTP_SCHEME;
use super::config::SftpConfig;
use super::core::KnownHostsStrategy;
use super::core::PIPELINE_DEPTH;
use super::core::POSIX_RENAME;
use super::core::SftpCore;
use super::core::is_eof;
use super::core::is_sftp_protocol_error;
use super::core::parse_sftp_error;
use super::core::to_metadata;
use super::deleter::SftpDeleter;
use super::reader::*;
use opendal_core::raw::*;
use opendal_core::*;

/// SFTP services support.
///
/// Warning: Maximum number of file holdings is depending on the remote system configuration.
///
/// For example, the default value is 255 in macOS, and 1024 in linux. If you want to open
/// lots of files, you should pay attention to close the file after using it.
#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct SftpBuilder {
    pub(super) config: SftpConfig,
}

impl SftpBuilder {
    /// set endpoint for sftp backend.
    /// The format is either `[user@]hostname[:port]` or `ssh://[user@]hostname[:port]`, and the port defaults to 22. A username that is specified in the endpoint overrides the one set in the builder (but does not change the builder).
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
                    KnownHostsStrategy::Strict
                } else if v == "accept" {
                    KnownHostsStrategy::Accept
                } else if v == "add" {
                    KnownHostsStrategy::Add
                } else {
                    return Err(Error::new(
                        ErrorKind::ConfigInvalid,
                        format!("unknown known_hosts strategy: {v}").as_str(),
                    ));
                }
            }
            None => KnownHostsStrategy::Strict,
        };

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
            list_with_limit: true,

            copy: true,
            rename: true,

            shared: true,

            ..Default::default()
        };

        let core = Arc::new(SftpCore::new(
            info,
            capability,
            endpoint,
            root,
            user,
            self.config.key.clone(),
            known_hosts_strategy,
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
        let conn = self.core.connect().await?;

        // Create every missing component of the requested directory chain.
        let mut current = self.core.abs_path("");
        for component in path.split('/').filter(|v| !v.is_empty()) {
            if !current.is_empty() && !current.ends_with('/') {
                current.push('/');
            }
            current.push_str(component);

            if let Err(e) = conn
                .session
                .mkdir(current.as_str(), FileAttributes::default())
                .await
                && !is_sftp_protocol_error(&e)
            {
                // ignore error if dir already exists
                return Err(parse_sftp_error(e));
            }
        }

        Ok(RpCreateDir::default())
    }

    async fn stat(&self, _ctx: &OperationContext, path: &str, _: OpStat) -> Result<RpStat> {
        let conn = self.core.connect().await?;

        let attrs = conn
            .session
            .stat(self.core.abs_path(path))
            .await
            .map_err(parse_sftp_error)?;

        Ok(RpStat::new(to_metadata(&attrs.attrs)))
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

    fn write(&self, ctx: &OperationContext, path: &str, op: OpWrite) -> Result<Self::Writer> {
        Ok(SftpLazyWriter::new(self.clone(), ctx.clone(), path, op))
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
        ctx: &OperationContext,
        from: &str,
        to: &str,
        _: OpCopy,
        _opts: OpCopier,
    ) -> Result<Self::Copier> {
        let backend = self.clone();
        let ctx = ctx.clone();
        let from = from.to_string();
        let to = to.to_string();
        Ok(oio::OneShotCopier::new(async move {
            if let Some((dir, _)) = to.rsplit_once('/') {
                backend
                    .create_dir(&ctx, dir, OpCreateDir::default())
                    .await?;
            }

            let core = &backend.core;
            let conn = core.connect().await?;
            let session = &conn.session;

            let src = session
                .open(
                    core.abs_path(&from),
                    OpenFlags::READ,
                    FileAttributes::default(),
                )
                .await
                .map_err(parse_sftp_error)?
                .handle;
            let dst = session
                .open(
                    core.abs_path(&to),
                    OpenFlags::WRITE | OpenFlags::CREATE | OpenFlags::TRUNCATE,
                    FileAttributes::default(),
                )
                .await
                .map_err(parse_sftp_error)?;
            let dst = dst.handle;

            // SFTP has no server-side copy, so stream the payload through the
            // client, keeping several reads and writes in flight.
            let chunk = conn.read_len.min(conn.write_len) as u64;
            let mut offset = 0u64;
            'copy: loop {
                let reads = (0..PIPELINE_DEPTH).map(|i| {
                    let session = session.clone();
                    let src = src.clone();
                    let at = offset + i as u64 * chunk;
                    async move { session.read(src, at, chunk as u32).await }
                });

                let mut writes = Vec::with_capacity(PIPELINE_DEPTH);
                let mut done = false;
                for (i, result) in futures::future::join_all(reads)
                    .await
                    .into_iter()
                    .enumerate()
                {
                    let data = match result {
                        Ok(data) => data.data,
                        Err(e) if is_eof(&e) => {
                            done = true;
                            break;
                        }
                        Err(e) => return Err(parse_sftp_error(e)),
                    };

                    let len = data.len() as u64;
                    if len == 0 {
                        done = true;
                        break;
                    }

                    let session = session.clone();
                    let dst = dst.clone();
                    let at = offset + i as u64 * chunk;
                    writes.push(async move { session.write(dst, at, data).await });

                    if len < chunk {
                        done = true;
                        break;
                    }
                }

                let written = writes.len() as u64;
                futures::future::try_join_all(writes)
                    .await
                    .map_err(parse_sftp_error)?;

                if done || written == 0 {
                    break 'copy;
                }
                offset += written * chunk;
            }

            let _ = session.close(src).await;
            session.close(dst).await.map_err(parse_sftp_error)?;

            Ok(Metadata::default())
        }))
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        _: OpRename,
    ) -> Result<RpRename> {
        if let Some((dir, _)) = to.rsplit_once('/') {
            self.create_dir(ctx, dir, OpCreateDir::default()).await?;
        }

        let conn = self.core.connect().await?;
        let from = self.core.abs_path(from);
        let to = self.core.abs_path(to);

        if conn.posix_rename {
            // `posix-rename@openssh.com` replaces an existing destination
            // atomically, which plain SFTP v3 `rename` refuses to do.
            let payload: Vec<u8> = HardlinkExtension {
                oldpath: from,
                newpath: to,
            }
            .try_into()
            .map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    "sftp failed to encode rename request",
                )
                .set_source(err)
            })?;

            match conn
                .session
                .extended(POSIX_RENAME, payload)
                .await
                .map_err(parse_sftp_error)?
            {
                Packet::Status(status) if status.status_code == StatusCode::Ok => {}
                Packet::Status(status) => {
                    return Err(parse_sftp_error(SftpClientError::Status(status)));
                }
                _ => {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        "sftp rename returned an unexpected packet",
                    ));
                }
            }
        } else {
            // Fall back to removing the destination first, matching the
            // overwrite semantics callers expect from `rename`.
            let _ = conn.session.remove(to.clone()).await;
            conn.session
                .rename(from, to)
                .await
                .map_err(parse_sftp_error)?;
        }

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
