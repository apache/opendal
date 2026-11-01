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

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use log::debug;
use openssh::KnownHosts;

use super::SFTP_SCHEME;
use super::config::SftpConfig;
use super::core::Manager;
use super::core::SftpCore;
use super::core::is_sftp_protocol_error;
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

    /// Sets the TCP connection and initial SSH handshake/key exchange timeout.
    ///
    /// Defaults to 10 seconds. Zero uses OpenSSH's configured timeout.
    /// OpenSSH accepts only whole seconds; a fractional-second value returns
    /// [`ErrorKind::ConfigInvalid`] when building the service.
    /// This does not limit authentication or SFTP
    /// initialization. Use [`opendal_core::layers::TimeoutLayer`] for an overall
    /// operation deadline.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.config.connect_timeout = Some(format!("{timeout:?}"));
        self
    }

    /// Sets the timeout for waiting for an available connection checkout.
    ///
    /// Defaults to 10 seconds. Zero disables this timeout. Subsecond durations
    /// retain their precision. After admission, this timeout does not limit
    /// connection creation or validation.
    pub fn acquire_timeout(mut self, timeout: Duration) -> Self {
        self.config.acquire_timeout = Some(format!("{timeout:?}"));
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

        let connect_timeout = self
            .config
            .connect_timeout
            .as_deref()
            .map(signed_to_duration)
            .transpose()?
            .unwrap_or(Duration::from_secs(10));
        if connect_timeout.subsec_nanos() != 0 {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "connect_timeout must be a whole number of seconds",
            ));
        }
        let connect_timeout = (!connect_timeout.is_zero()).then_some(connect_timeout);

        let acquire_timeout = self
            .config
            .acquire_timeout
            .as_deref()
            .map(signed_to_duration)
            .transpose()?
            .unwrap_or(Duration::from_secs(10));
        let acquire_timeout = (!acquire_timeout.is_zero()).then_some(acquire_timeout);

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
            Manager {
                endpoint,
                root,
                user,
                key: self.config.key.clone(),
                known_hosts_strategy,
                connect_timeout,
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
        args: OpCopy,
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

            let client = backend.core.connect().await?;
            let mut fs = client.fs();
            fs.set_cwd(&backend.core.root);

            let src = fs.canonicalize(&from).await.map_err(parse_sftp_error)?;
            let dst = fs.canonicalize(&to).await.map_err(parse_sftp_error)?;
            let mut src_file = client.open(&src).await.map_err(parse_sftp_error)?;
            let mut dst_file = client.create(dst).await.map_err(parse_sftp_error)?;

            let source_size = match args.source_content_length_hint() {
                Some(size) => size,
                None => src_file
                    .metadata()
                    .await
                    .map_err(parse_sftp_error)?
                    .len()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::Unexpected,
                            "sftp source metadata does not contain file length",
                        )
                    })?,
            };

            src_file
                .copy_all_to(&mut dst_file)
                .await
                .map_err(parse_sftp_error)?;

            Ok(MetadataBuilder::file(source_size).build())
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

        let client = self.core.connect().await?;
        let mut fs = client.fs();
        fs.set_cwd(&self.core.root);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_timeout_builder_preserves_precision() {
        for timeout in [
            Duration::ZERO,
            Duration::from_nanos(1),
            Duration::from_millis(500),
            Duration::from_millis(1500),
        ] {
            let builder = SftpBuilder::default()
                .endpoint("unused.invalid")
                .acquire_timeout(timeout);
            let encoded = builder.config.acquire_timeout.as_deref().unwrap();
            assert_eq!(signed_to_duration(encoded).unwrap(), timeout);
            assert!(builder.build().is_ok());
        }
    }

    #[test]
    fn timeout_options_from_map() {
        for connect in ["0s", "2s", "PT10S"] {
            let config = SftpConfig::from_iter(
                [
                    ("endpoint", "unused.invalid"),
                    ("connect_timeout", connect),
                    ("acquire_timeout", "1500ms"),
                ]
                .map(|(key, value)| (key.to_string(), value.to_string())),
            )
            .unwrap();
            assert!(config.into_builder().build().is_ok());
        }

        assert!(
            SftpBuilder::default()
                .endpoint("unused.invalid")
                .connect_timeout(Duration::ZERO)
                .acquire_timeout(Duration::ZERO)
                .build()
                .is_ok()
        );
    }

    #[test]
    fn fractional_connect_timeout_is_rejected() {
        for timeout in [Duration::from_millis(500), Duration::from_millis(1500)] {
            let error = SftpBuilder::default()
                .endpoint("unused.invalid")
                .connect_timeout(timeout)
                .build()
                .err()
                .expect("fractional OpenSSH timeout must be rejected");
            assert_eq!(error.kind(), ErrorKind::ConfigInvalid);

            let mut config = SftpConfig::default();
            config.endpoint = Some("unused.invalid".to_string());
            config.connect_timeout = Some(format!("{timeout:?}"));
            let error = config.into_builder().build().err().unwrap();
            assert_eq!(error.kind(), ErrorKind::ConfigInvalid);
        }
    }

    #[test]
    fn invalid_timeout_options_are_rejected() {
        for value in ["-1s", "invalid", "999999999999999999999999s"] {
            for option in ["connect_timeout", "acquire_timeout"] {
                let config = SftpConfig::from_iter(
                    [("endpoint", "unused.invalid"), (option, value)]
                        .map(|(key, value)| (key.to_string(), value.to_string())),
                )
                .unwrap();
                let error = config.into_builder().build().err().unwrap();
                assert_eq!(error.kind(), ErrorKind::ConfigInvalid);
            }
        }
    }
}
