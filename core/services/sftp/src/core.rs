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
use std::future::Future;
use std::io;
use std::ops::Deref;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use asyncband::pool::{ManageObject, ObjectStatus, unbounded};
use asyncband::semaphore::{OwnedSemaphorePermit, Semaphore};
use log::debug;
use opendal_core::raw::*;
use opendal_core::*;
use openssh::Error as SshError;
use openssh::{KnownHosts, SessionBuilder};
use openssh_sftp_client::Error as SftpClientError;
use openssh_sftp_client::error::SftpErrorKind;
use openssh_sftp_client::{Sftp, SftpOptions};

const POOL_CAPACITY: usize = 64;

pub struct SftpCore {
    pub info: ServiceInfo,
    pub capability: Capability,
    pub endpoint: String,
    pub root: String,
    client: Arc<unbounded::Pool<Sftp, Manager>>,
    capacity: Arc<Semaphore>,
    acquire_timeout: Option<Duration>,
}

impl Debug for SftpCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SftpCore")
            .field("endpoint", &self.endpoint)
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl SftpCore {
    pub fn new(
        info: ServiceInfo,
        capability: Capability,
        manager: Manager,
        acquire_timeout: Option<Duration>,
    ) -> Self {
        Self {
            info,
            capability,
            endpoint: manager.endpoint.clone(),
            root: manager.root.clone(),
            client: unbounded::Pool::new(unbounded::PoolConfig::default(), manager),
            capacity: Arc::new(Semaphore::new(POOL_CAPACITY)),
            acquire_timeout,
        }
    }

    pub async fn connect(&self) -> Result<SftpConnection> {
        let permit = connection_stage(self.acquire_timeout, "waiting for pool capacity", async {
            Ok(self.capacity.clone().acquire_owned(1).await)
        })
        .await?;
        let object = self.client.get().await?;
        Ok(SftpConnection {
            object,
            _permit: permit,
        })
    }
}

pub struct SftpConnection {
    // Return the connection to the pool before another checkout can take its slot.
    object: unbounded::Object<Sftp, Manager>,
    _permit: OwnedSemaphorePermit,
}

impl Deref for SftpConnection {
    type Target = Sftp;

    fn deref(&self) -> &Self::Target {
        &self.object
    }
}

pub struct Manager {
    pub(super) endpoint: String,
    pub(super) root: String,
    pub(super) user: Option<String>,
    pub(super) key: Option<String>,
    pub(super) known_hosts_strategy: KnownHosts,
    pub(super) connect_timeout: Option<Duration>,
    #[cfg(not(windows))]
    pub(super) control_directory: Option<String>,
}

impl Manager {
    async fn initialize_root(&self, sftp: &Sftp) -> Result<()> {
        create_dir_all(sftp, "", &self.root).await
    }
}

pub(crate) async fn create_dir_all(sftp: &Sftp, base: &str, path: &str) -> Result<()> {
    let mut fs = sftp.fs();
    let mut current = PathBuf::from(base);
    if !base.is_empty() {
        fs.set_cwd(&current);
    }

    for component in Path::new(path).components() {
        let segment = component.as_os_str();
        match component {
            Component::RootDir => {
                if base.is_empty() {
                    current.push(segment);
                    fs.set_cwd(&current);
                }
                continue;
            }
            Component::CurDir => continue,
            _ => {}
        }

        current.push(segment);
        if let Err(error) = fs.create_dir(segment).await {
            // Servers may reject mkdir even when an existing directory can be used.
            if !matches!(
                error,
                SftpClientError::SftpError(SftpErrorKind::Failure | SftpErrorKind::PermDenied, _)
            ) {
                return Err(parse_sftp_error(error));
            }
            match fs.metadata(segment).await {
                Ok(metadata) if metadata.file_type().is_some_and(|kind| kind.is_dir()) => {}
                Ok(_) => {
                    return Err(Error::new(
                        ErrorKind::NotADirectory,
                        "path exists and is not a directory",
                    )
                    .set_source(error));
                }
                Err(err) if classify_sftp_error(&err).1 => return Err(parse_sftp_error(err)),
                _ => return Err(parse_sftp_error(error)),
            }
        }
        fs.set_cwd(&current);
    }

    Ok(())
}

impl ManageObject for Manager {
    type Object = Sftp;
    type Error = Error;

    async fn create(&self) -> Result<Self::Object, Self::Error> {
        let mut session = SessionBuilder::default();

        if let Some(user) = &self.user {
            session.user(user.clone());
        }

        if let Some(key) = &self.key {
            session.keyfile(key);
        }

        session.known_hosts_check(self.known_hosts_strategy.clone());
        #[cfg(not(windows))]
        if let Some(dir) = &self.control_directory {
            session.control_directory(dir);
        }
        if let Some(timeout) = self.connect_timeout {
            // OpenSSH uses whole seconds. Keep a process-side limit even if the Rust
            // deadline expires first and drops the connect future.
            let seconds = timeout
                .as_secs()
                .saturating_add(u64::from(timeout.subsec_nanos() != 0))
                .min(i32::MAX as u64);
            session.connect_timeout(Duration::from_secs(seconds));
        }

        let session = connection_stage(
            self.connect_timeout,
            "SSH connection and authentication",
            async {
                session
                    .connect(&self.endpoint)
                    .await
                    .map_err(parse_ssh_error)
            },
        )
        .await?;

        let sftp = connection_stage(
            self.connect_timeout,
            "SFTP subsystem initialization",
            async {
                Sftp::from_session(session, SftpOptions::default())
                    .await
                    .map_err(parse_sftp_error)
            },
        )
        .await?;

        connection_stage(
            self.connect_timeout,
            "SFTP root initialization",
            self.initialize_root(&sftp),
        )
        .await
        .map_err(|err| err.with_context("root", &self.root))?;

        debug!("sftp connection created at {}", self.root);
        Ok(sftp)
    }

    async fn is_recyclable(
        &self,
        o: &mut Self::Object,
        _: &ObjectStatus,
    ) -> Result<(), Self::Error> {
        let mut fs = o.fs();
        fs.set_cwd(&self.root);
        let result = connection_stage(
            self.connect_timeout,
            "SFTP connection health check",
            async {
                fs.metadata("./").await.map_err(parse_sftp_error)?;
                Ok(())
            },
        )
        .await;
        if let Err(err) = &result {
            // The pool discards the failed connection and tries another checkout.
            debug!("discarding pooled SFTP connection: {err:?}");
        }
        result
    }
}

async fn connection_stage<T>(
    timeout: Option<Duration>,
    stage: &'static str,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    let result = match timeout {
        Some(timeout) => tokio::time::timeout(timeout, future)
            .await
            .unwrap_or_else(|err| {
                Err(
                    Error::new(ErrorKind::Unexpected, "SFTP connection timed out")
                        .set_temporary()
                        .set_source(err),
                )
            }),
        None => future.await,
    };
    result.map_err(|err| err.with_context("stage", stage))
}

pub fn parse_sftp_error(e: SftpClientError) -> Error {
    let (kind, temporary) = classify_sftp_error(&e);
    let error = Error::new(kind, "sftp error").set_source(e);
    if temporary {
        error.set_temporary()
    } else {
        error
    }
}

fn classify_sftp_error(error: &SftpClientError) -> (ErrorKind, bool) {
    match error {
        SftpClientError::UnsupportedSftpProtocol { .. }
        | SftpClientError::SftpError(SftpErrorKind::OpUnsupported, _) => {
            (ErrorKind::Unsupported, false)
        }
        SftpClientError::SftpError(SftpErrorKind::NoSuchFile, _) => (ErrorKind::NotFound, false),
        SftpClientError::SftpError(SftpErrorKind::PermDenied, _) => {
            (ErrorKind::PermissionDenied, false)
        }
        SftpClientError::SftpError(SftpErrorKind::Failure, _) => (ErrorKind::Unexpected, true),
        SftpClientError::IOError(err) => classify_io_error(err),
        SftpClientError::BackgroundTaskFailure(_) => (ErrorKind::Unexpected, true),
        SftpClientError::RemoteChildSpawnError(err) => classify_ssh_error(err),
        SftpClientError::RecursiveErrors(errors) => classify_sftp_error(&errors.original_error),
        SftpClientError::RecursiveErrors3(errors) => classify_sftp_error(&errors.err1),
        _ => (ErrorKind::Unexpected, false),
    }
}

pub fn parse_ssh_error(e: SshError) -> Error {
    let (kind, temporary) = classify_ssh_error(&e);
    let error = Error::new(kind, "failed to establish SSH connection").set_source(e);
    if temporary {
        error.set_temporary()
    } else {
        error
    }
}

fn classify_ssh_error(error: &SshError) -> (ErrorKind, bool) {
    match error {
        SshError::Disconnected | SshError::RemoteProcessTerminated => (ErrorKind::Unexpected, true),
        SshError::Connect(err)
            if matches!(
                err.kind(),
                io::ErrorKind::ConnectionAborted | io::ErrorKind::Other
            ) =>
        {
            // openssh uses ConnectionAborted for unrecognized diagnostics, including
            // host key and authentication failures as well as network failures.
            let diagnostic = err.to_string();
            if diagnostic.lines().any(|line| {
                line == "Host key verification failed." || line.contains(": Permission denied (")
            }) {
                return (ErrorKind::PermissionDenied, false);
            }
            let temporary = diagnostic.lines().any(|raw_line| {
                let line = raw_line
                    .strip_prefix("kex_exchange_identification: ")
                    .or_else(|| raw_line.strip_prefix("ssh_exchange_identification: "))
                    .unwrap_or(raw_line);
                let line = line.strip_prefix("read: ").unwrap_or(line);
                matches!(
                    line,
                    "Connection timed out during banner exchange"
                        | "Connection timed out"
                        | "Operation timed out"
                        | "Broken pipe"
                ) || raw_line.ends_with(" Connection timed out")
                    || raw_line.ends_with(" Operation timed out")
                    || raw_line.ends_with(": Network is unreachable")
                    || raw_line.ends_with(": No route to host")
                    || raw_line.ends_with(" Connection reset by peer")
                    || raw_line.ends_with(" Broken pipe")
                    || line.starts_with("Connection closed by ")
                    || line.starts_with("Connection reset by ")
            });
            (ErrorKind::Unexpected, temporary)
        }
        SshError::Connect(err)
        | SshError::Master(err)
        | SshError::Ssh(err)
        | SshError::Remote(err)
        | SshError::ChildIo(err) => classify_io_error(err),
        _ => (ErrorKind::Unexpected, false),
    }
}

fn classify_io_error(error: &io::Error) -> (ErrorKind, bool) {
    match error.kind() {
        io::ErrorKind::PermissionDenied => (ErrorKind::PermissionDenied, false),
        io::ErrorKind::NotFound => (ErrorKind::NotFound, false),
        io::ErrorKind::TimedOut
        | io::ErrorKind::ConnectionRefused
        | io::ErrorKind::ConnectionReset
        | io::ErrorKind::ConnectionAborted
        | io::ErrorKind::NotConnected
        | io::ErrorKind::NetworkUnreachable
        | io::ErrorKind::HostUnreachable
        | io::ErrorKind::BrokenPipe
        | io::ErrorKind::UnexpectedEof => (ErrorKind::Unexpected, true),
        _ => (ErrorKind::Unexpected, false),
    }
}

pub(crate) fn is_not_found(e: &SftpClientError) -> bool {
    matches!(e, SftpClientError::SftpError(SftpErrorKind::NoSuchFile, _))
}

pub(crate) fn is_sftp_failure(e: &SftpClientError) -> bool {
    matches!(e, SftpClientError::SftpError(SftpErrorKind::Failure, _))
}

mod utils {
    use openssh_sftp_client::metadata::MetaData as SftpMeta;

    use opendal_core::EntryMode;
    use opendal_core::Error;
    use opendal_core::ErrorKind;
    use opendal_core::Metadata;
    use opendal_core::MetadataBuilder;
    use opendal_core::Result;
    use opendal_core::raw::Timestamp;

    pub fn to_metadata(meta: SftpMeta) -> Result<Metadata> {
        let mode = meta
            .file_type()
            .map(|filetype| {
                if filetype.is_file() {
                    EntryMode::FILE
                } else if filetype.is_dir() {
                    EntryMode::DIR
                } else {
                    EntryMode::Unknown
                }
            })
            .unwrap_or(EntryMode::Unknown);

        let mut metadata = match mode {
            EntryMode::FILE => MetadataBuilder::file(meta.len().ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "sftp metadata does not contain file length",
                )
            })?),
            EntryMode::DIR => MetadataBuilder::dir(),
            EntryMode::Unknown => MetadataBuilder::unknown(),
        };

        if let Some(modified) = meta.modified()
            && let Ok(m) = Timestamp::try_from(modified.as_system_time())
        {
            metadata.last_modified(m);
        }

        Ok(metadata.build())
    }
}

pub(super) use utils::*;

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use super::*;

    #[tokio::test]
    async fn checkout_times_out_when_capacity_is_reserved_without_idle_connections() {
        let core = SftpCore::new(
            ServiceInfo::new("sftp", "", ""),
            Capability::default(),
            Manager {
                endpoint: String::new(),
                root: String::new(),
                user: None,
                key: None,
                known_hosts_strategy: KnownHosts::Strict,
                connect_timeout: None,
                #[cfg(not(windows))]
                control_directory: None,
            },
            Some(Duration::from_millis(10)),
        );
        let _reserved = core.capacity.clone().acquire_owned(POOL_CAPACITY).await;

        let error = tokio::time::timeout(Duration::from_secs(3), core.connect())
            .await
            .expect("checkout should respect the pool wait limit")
            .err()
            .expect("all pool slots are reserved");
        assert!(error.is_temporary(), "{error:?}");
        assert!(error.to_string().contains("waiting for pool capacity"));
        assert!(error.source().unwrap().is::<tokio::time::error::Elapsed>());
    }

    #[test]
    fn ssh_error_classification_preserves_original_error() {
        for (source, expected_kind, temporary) in [
            (SshError::Disconnected, ErrorKind::Unexpected, true),
            (
                SshError::Connect(io::ErrorKind::TimedOut.into()),
                ErrorKind::Unexpected,
                true,
            ),
            (
                SshError::Connect(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "Host key verification failed.\nConnection closed by remote host",
                )),
                ErrorKind::PermissionDenied,
                false,
            ),
            (
                SshError::Connect(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "user@host: Permission denied (publickey).\nConnection closed by remote host",
                )),
                ErrorKind::PermissionDenied,
                false,
            ),
        ] {
            let error = parse_ssh_error(source);
            assert_eq!(error.kind(), expected_kind, "{error:?}");
            assert_eq!(error.is_temporary(), temporary, "{error:?}");
            assert!(error.source().unwrap().is::<SshError>());
        }
    }

    #[test]
    fn ssh_transport_diagnostics_are_temporary() {
        for diagnostic in [
            "kex_exchange_identification: Connection closed by remote host",
            "kex_exchange_identification: read: Connection reset by peer",
            "kex_exchange_identification: read: Connection timed out",
            "kex_exchange_identification: Connection timed out",
            "read: Operation timed out",
            "read: Broken pipe",
            "ssh_exchange_identification: Connection closed by remote host",
        ] {
            let error = parse_ssh_error(SshError::Connect(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                diagnostic,
            )));
            assert_eq!(error.kind(), ErrorKind::Unexpected, "{error:?}");
            assert!(error.is_temporary(), "{error:?}");
            assert!(error.source().unwrap().is::<SshError>());
        }
    }

    #[test]
    fn sftp_status_classification_preserves_original_error() {
        use openssh_sftp_client::error::SftpErrMsg;
        use serde::Deserialize;
        use serde::de::value::{
            BorrowedStrDeserializer, Error as DeserializeError, SeqDeserializer,
        };

        let fields = ["server status", ""]
            .into_iter()
            .map(BorrowedStrDeserializer::<DeserializeError>::new);
        let message = SftpErrMsg::deserialize(SeqDeserializer::new(std::iter::once(
            SeqDeserializer::new(fields),
        )))
        .unwrap();

        for (status, kind, temporary) in [
            (SftpErrorKind::Failure, ErrorKind::Unexpected, true),
            (
                SftpErrorKind::PermDenied,
                ErrorKind::PermissionDenied,
                false,
            ),
            (SftpErrorKind::NoSuchFile, ErrorKind::NotFound, false),
            (SftpErrorKind::OpUnsupported, ErrorKind::Unsupported, false),
        ] {
            let error = parse_sftp_error(SftpClientError::SftpError(status, message.clone()));
            assert_eq!(error.kind(), kind, "{error:?}");
            assert_eq!(error.is_temporary(), temporary, "{error:?}");
            assert!(error.source().unwrap().is::<SftpClientError>());
        }
    }
}
