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

use fastpool::{ManageObject, ObjectStatus, bounded};
use log::debug;
use opendal_core::raw::*;
use opendal_core::*;
use russh::client;
use russh::keys::PrivateKeyWithHashAlg;
use russh_sftp::client::RawSftpSession;
use russh_sftp::client::rawsession::Limits;
use russh_sftp::protocol::FileAttributes;

/// Largest payload we ever ask the remote server for in a single SFTP packet.
///
/// The effective value is additionally clamped by the limits advertised through
/// the `limits@openssh.com` extension.
const MAX_CHUNK_SIZE: u32 = 256 * 1024;

/// Conservative payload size used when the server does not advertise limits.
///
/// SFTP v3 has no negotiation for this, and 32 KiB is the value the OpenSSH
/// client itself falls back to.
const DEFAULT_CHUNK_SIZE: u32 = 32 * 1024;

/// Number of SFTP read/write packets kept in flight at once.
///
/// SFTP is a request/response protocol, so throughput on links with non-trivial
/// latency is bound by the number of outstanding requests rather than bandwidth.
pub const PIPELINE_DEPTH: usize = 8;

/// OpenSSH extension providing `rename` with POSIX overwrite semantics.
pub const POSIX_RENAME: &str = "posix-rename@openssh.com";

/// Number of SSH handshakes allowed to run at the same time.
///
/// Servers cap how many connections may be mid-authentication at once
/// (`MaxStartups` defaults to `10:30:100` in OpenSSH, which starts dropping
/// connections past ten), so growing the pool in a burst must be throttled.
const MAX_CONCURRENT_HANDSHAKES: usize = 4;

/// Number of agent identities tried before giving up.
///
/// Each attempt counts against the server's `MaxAuthTries`, which defaults to
/// six in OpenSSH and drops the connection once exceeded.
const MAX_AGENT_IDENTITIES: usize = 4;

/// Specifies how `SftpBackend` validates the remote host key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnownHostsStrategy {
    /// The host key must be present in `known_hosts` and match.
    ///
    /// This mirrors `ssh -o StrictHostKeyChecking=yes`.
    Strict,
    /// Accept a previously unseen host key and record it in `known_hosts`.
    ///
    /// A changed key is still rejected. This mirrors
    /// `ssh -o StrictHostKeyChecking=accept-new`.
    Add,
    /// Accept whatever key the server presents.
    ///
    /// This mirrors `ssh -o StrictHostKeyChecking=no`.
    Accept,
}

/// Validates the remote host key according to [`KnownHostsStrategy`].
struct SshHandler {
    host: String,
    port: u16,
    strategy: KnownHostsStrategy,
}

impl client::Handler for SshHandler {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        server_public_key: &russh::keys::ssh_key::PublicKey,
    ) -> std::result::Result<bool, Self::Error> {
        match self.strategy {
            KnownHostsStrategy::Accept => Ok(true),
            KnownHostsStrategy::Strict => {
                Ok(
                    russh::keys::check_known_hosts(&self.host, self.port, server_public_key)
                        .unwrap_or(false),
                )
            }
            KnownHostsStrategy::Add => {
                match russh::keys::check_known_hosts(&self.host, self.port, server_public_key) {
                    Ok(true) => Ok(true),
                    // The host is unknown: record it and continue.
                    Ok(false) => {
                        if let Err(err) = russh::keys::known_hosts::learn_known_hosts(
                            &self.host,
                            self.port,
                            server_public_key,
                        ) {
                            // Nothing was pinned, so every later connection also
                            // takes this branch and `add` silently behaves like
                            // `accept`. Surface it rather than hiding it.
                            log::warn!(
                                "sftp accepted host key for {}:{} but could not record it in known_hosts, \
                                 so it will not be verified on future connections: {err}",
                                self.host,
                                self.port
                            );
                        }
                        Ok(true)
                    }
                    // The host is known but the key changed: refuse to connect.
                    Err(_) => Ok(false),
                }
            }
        }
    }
}

/// A detachable reference to a live SFTP session.
///
/// Readers, writers, and listers hold one of these instead of the pooled
/// connection itself, so a long-running stream returns its slot to the pool
/// immediately. Remote servers usually cap concurrent SSH connections
/// (`MaxStartups`), so keeping a slot for the lifetime of a stream would
/// otherwise exhaust the server well before the pool.
#[derive(Clone)]
pub struct SftpSessionRef {
    pub session: Arc<RawSftpSession>,
    /// Largest payload accepted by the server for a single read.
    pub read_len: u32,
    /// Largest payload accepted by the server for a single write.
    pub write_len: u32,
    /// Whether the server offers `posix-rename@openssh.com`.
    pub posix_rename: bool,
    /// Keeps the SSH connection open for as long as any reference is alive.
    _ssh: Arc<client::Handle<SshHandler>>,
}

/// A live SFTP session together with the SSH connection that carries it.
pub struct SftpConnection {
    inner: SftpSessionRef,
}

impl SftpConnection {
    /// Returns a reference that keeps the session alive after the pooled
    /// connection is released.
    pub fn session_ref(&self) -> SftpSessionRef {
        self.inner.clone()
    }
}

impl std::ops::Deref for SftpConnection {
    type Target = SftpSessionRef;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub struct SftpCore {
    pub info: ServiceInfo,
    pub capability: Capability,
    pub endpoint: String,
    pub root: String,
    client: Arc<bounded::Pool<Manager>>,
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
        endpoint: String,
        root: String,
        user: Option<String>,
        key: Option<String>,
        known_hosts_strategy: KnownHostsStrategy,
    ) -> Self {
        let client = bounded::Pool::new(
            bounded::PoolConfig::new(64),
            Manager {
                endpoint: endpoint.clone(),
                root: root.clone(),
                user,
                key,
                known_hosts_strategy,
                handshakes: tokio::sync::Semaphore::new(MAX_CONCURRENT_HANDSHAKES),
            },
        );

        SftpCore {
            info,
            capability,
            endpoint,
            root,
            client,
        }
    }

    /// Resolves an OpenDAL path against the configured root.
    ///
    /// An empty root leaves the path relative, so the remote server resolves it
    /// against the login directory.
    pub fn abs_path(&self, path: &str) -> String {
        let path = path.trim_start_matches('/');

        if self.root.is_empty() {
            if path.is_empty() {
                return ".".to_string();
            }
            return path.to_string();
        }

        let mut buf = String::with_capacity(self.root.len() + path.len());
        buf.push_str(&self.root);
        buf.push_str(path);
        buf
    }

    pub async fn connect(&self) -> Result<bounded::Object<Manager>> {
        let fut = self.client.get();

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(10)) => {
                Err(Error::new(ErrorKind::Unexpected, "connection request: timeout").set_temporary())
            }
            result = fut => match result {
                Ok(conn) => Ok(conn),
                Err(err) => Err(err),
            }
        }
    }
}

pub struct Manager {
    endpoint: String,
    root: String,
    user: Option<String>,
    key: Option<String>,
    known_hosts_strategy: KnownHostsStrategy,
    /// Throttles concurrent SSH handshakes, see [`MAX_CONCURRENT_HANDSHAKES`].
    handshakes: tokio::sync::Semaphore,
}

impl Manager {
    /// Authenticates the session, preferring the configured key and otherwise
    /// falling back to an SSH agent and the usual default key locations.
    async fn authenticate(
        &self,
        handle: &mut client::Handle<SshHandler>,
        user: &str,
    ) -> Result<()> {
        if let Some(key) = &self.key {
            let private_key = russh::keys::load_secret_key(key, None).map_err(|err| {
                Error::new(ErrorKind::ConfigInvalid, "sftp failed to load private key")
                    .set_source(err)
            })?;

            if self
                .try_publickey(handle, user, private_key)
                .await?
                .is_some()
            {
                return Ok(());
            }

            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "sftp public key authentication failed",
            ));
        }

        if self.try_agent(handle, user).await? {
            return Ok(());
        }

        for candidate in default_key_paths() {
            let Ok(private_key) = russh::keys::load_secret_key(&candidate, None) else {
                continue;
            };
            if self
                .try_publickey(handle, user, private_key)
                .await?
                .is_some()
            {
                return Ok(());
            }
        }

        Err(Error::new(
            ErrorKind::PermissionDenied,
            "sftp authentication failed: no usable credentials, set `key` to a private key path",
        ))
    }

    async fn try_publickey(
        &self,
        handle: &mut client::Handle<SshHandler>,
        user: &str,
        private_key: russh::keys::PrivateKey,
    ) -> Result<Option<()>> {
        // RSA keys must be signed with the strongest hash the server accepts;
        // servers commonly reject the legacy SHA-1 `ssh-rsa` signatures.
        let hash_alg = handle
            .best_supported_rsa_hash()
            .await
            .map_err(parse_ssh_error)?
            .flatten();

        let result = handle
            .authenticate_publickey(
                user,
                PrivateKeyWithHashAlg::new(Arc::new(private_key), hash_alg),
            )
            .await
            .map_err(parse_ssh_error)?;

        Ok(result.success().then_some(()))
    }

    async fn try_agent(&self, handle: &mut client::Handle<SshHandler>, user: &str) -> Result<bool> {
        #[cfg(unix)]
        {
            use russh::keys::agent::AgentIdentity;

            let Ok(mut agent) = russh::keys::agent::client::AgentClient::connect_env().await else {
                return Ok(false);
            };
            let Ok(identities) = agent.request_identities().await else {
                return Ok(false);
            };

            let hash_alg = handle
                .best_supported_rsa_hash()
                .await
                .map_err(parse_ssh_error)?
                .flatten();

            // Every attempt counts against the server's `MaxAuthTries` (6 by
            // default), and exhausting it disconnects instead of reporting a
            // clean authentication failure.
            for identity in identities.into_iter().take(MAX_AGENT_IDENTITIES) {
                let AgentIdentity::PublicKey { key, .. } = identity else {
                    continue;
                };

                let Ok(result) = handle
                    .authenticate_publickey_with(user, key, hash_alg, &mut agent)
                    .await
                else {
                    continue;
                };
                if result.success() {
                    return Ok(true);
                }
            }
        }

        let _ = (handle, user);
        Ok(false)
    }
}

impl ManageObject for Manager {
    type Object = SftpConnection;
    type Error = Error;

    async fn create(&self) -> Result<Self::Object, Self::Error> {
        let _permit = self.handshakes.acquire().await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "sftp connection limiter closed").set_source(err)
        })?;

        let (endpoint_user, host, port) = parse_endpoint(&self.endpoint)?;

        // A user encoded in the endpoint takes precedence over the builder value.
        let user = endpoint_user
            .or_else(|| self.user.clone())
            .or_else(default_user)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::ConfigInvalid,
                    "sftp user is not set and cannot be inferred from the environment",
                )
            })?;

        let handler = SshHandler {
            host: host.clone(),
            port,
            strategy: self.known_hosts_strategy,
        };

        let mut handle = client::connect(
            Arc::new(client::Config::default()),
            (host.as_str(), port),
            handler,
        )
        .await
        .map_err(parse_ssh_error)?;

        self.authenticate(&mut handle, &user).await?;

        let channel = handle
            .channel_open_session()
            .await
            .map_err(parse_ssh_error)?;
        channel
            .request_subsystem(true, "sftp")
            .await
            .map_err(parse_ssh_error)?;

        let config = russh_sftp::client::Config {
            // Every request is a single round trip, so allow slow servers more
            // room than the crate default of 10 seconds.
            request_timeout_secs: 60,
            ..Default::default()
        };
        let mut session = RawSftpSession::new_with_config(channel.into_stream(), config);

        let version = session.init().await.map_err(parse_sftp_error)?;

        // SFTP v3 `rename` fails when the destination exists; the OpenSSH
        // extension provides POSIX overwrite semantics instead.
        let posix_rename = version.extensions.contains_key(POSIX_RENAME);

        let mut read_len = DEFAULT_CHUNK_SIZE;
        let mut write_len = DEFAULT_CHUNK_SIZE;
        if version
            .extensions
            .get(russh_sftp::extensions::LIMITS)
            .is_some_and(|v| v == "1")
        {
            let limits = Limits::from(session.limits().await.map_err(parse_sftp_error)?);
            session.set_limits(limits);

            if let Some(len) = limits.read_len {
                read_len = clamp_chunk(len);
            }
            if let Some(len) = limits.write_len {
                write_len = clamp_chunk(len);
            }

            // The server rejects a *serialized* packet larger than this, so the
            // payload must leave room for the request header. OpenSSH reserves
            // the same 1 KiB between its packet and read/write limits.
            if let Some(packet_len) = limits.packet_len {
                let budget = clamp_chunk(packet_len.saturating_sub(1024));
                read_len = read_len.min(budget);
                write_len = write_len.min(budget);
            }
        }

        let session = Arc::new(session);

        if !self.root.is_empty() {
            // Create the root directory chain, ignoring components that exist.
            let mut current = String::new();
            for component in self.root.split('/').filter(|v| !v.is_empty()) {
                current.push('/');
                current.push_str(component);

                if let Err(e) = session
                    .mkdir(current.as_str(), FileAttributes::default())
                    .await
                    && !is_sftp_protocol_error(&e)
                {
                    return Err(parse_sftp_error(e));
                }
            }
        }

        debug!("sftp connection created at {}", self.root);
        Ok(SftpConnection {
            inner: SftpSessionRef {
                session,
                read_len,
                write_len,
                posix_rename,
                _ssh: Arc::new(handle),
            },
        })
    }

    // Check if connect valid by checking the root path.
    async fn is_recyclable(
        &self,
        o: &mut Self::Object,
        _: &ObjectStatus,
    ) -> Result<(), Self::Error> {
        match o.session.stat(".").await {
            Ok(_) => Ok(()),
            Err(e) => Err(parse_sftp_error(e)),
        }
    }
}

fn clamp_chunk(len: u64) -> u32 {
    len.min(MAX_CHUNK_SIZE as u64).max(1) as u32
}

/// Releases a server-side handle without awaiting the reply.
///
/// Handles are remote resources, so abandoning a reader, writer, or lister
/// early must not leak them. `Drop` cannot await, so the close is dispatched
/// onto the current runtime. Without a runtime the session is already being
/// torn down, and the server releases the handle along with the channel.
pub fn close_handle_detached(session: Arc<RawSftpSession>, handle: String) {
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        runtime.spawn(async move {
            if let Err(err) = session.close(handle).await {
                debug!("sftp failed to close handle: {err}");
            }
        });
    }
}

fn default_user() -> Option<String> {
    std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .ok()
        .filter(|v| !v.is_empty())
}

fn default_key_paths() -> Vec<std::path::PathBuf> {
    let Some(home) = std::env::home_dir() else {
        return Vec::new();
    };
    let ssh = home.join(".ssh");

    ["id_ed25519", "id_ecdsa", "id_rsa"]
        .iter()
        .map(|name| ssh.join(name))
        .collect()
}

/// Splits an endpoint into its user, host, and port parts.
///
/// Accepts both `[user@]host[:port]` and `ssh://[user@]host[:port]`, and
/// defaults the port to 22.
fn parse_endpoint(endpoint: &str) -> Result<(Option<String>, String, u16)> {
    let invalid = || {
        Error::new(
            ErrorKind::ConfigInvalid,
            "sftp endpoint is invalid, expected `[user@]host[:port]`",
        )
    };

    let raw = endpoint.trim();
    let raw = raw.strip_prefix("ssh://").unwrap_or(raw);
    // Drop any trailing path component; the root is configured separately.
    let raw = raw.split('/').next().unwrap_or(raw);

    let (user, host_port) = match raw.rsplit_once('@') {
        Some((user, host_port)) if !user.is_empty() => (Some(user.to_string()), host_port),
        _ => (None, raw),
    };

    let (host, port) = if let Some(rest) = host_port.strip_prefix('[') {
        // Bracketed IPv6 literal, optionally followed by a port.
        let (host, tail) = rest.split_once(']').ok_or_else(invalid)?;
        let port = match tail.strip_prefix(':') {
            Some(port) => port.parse::<u16>().map_err(|_| invalid())?,
            None => 22,
        };
        (host.to_string(), port)
    } else if host_port.matches(':').count() > 1 {
        // An unbracketed IPv6 literal cannot be told apart from `host:port`.
        return Err(invalid());
    } else {
        match host_port.rsplit_once(':') {
            Some((host, port)) => (
                host.to_string(),
                port.parse::<u16>().map_err(|_| invalid())?,
            ),
            None => (host_port.to_string(), 22),
        }
    };

    if host.is_empty() {
        return Err(invalid());
    }

    Ok((user, host, port))
}

mod error {
    use russh_sftp::client::error::Error as SftpClientError;
    use russh_sftp::protocol::StatusCode;

    use opendal_core::Error;
    use opendal_core::ErrorKind;

    pub fn parse_sftp_error(e: SftpClientError) -> Error {
        let kind = match &e {
            SftpClientError::Status(status) => match status.status_code {
                StatusCode::NoSuchFile => ErrorKind::NotFound,
                StatusCode::PermissionDenied => ErrorKind::PermissionDenied,
                StatusCode::OpUnsupported => ErrorKind::Unsupported,
                _ => ErrorKind::Unexpected,
            },
            _ => ErrorKind::Unexpected,
        };

        let mut err = Error::new(kind, "sftp error").set_source(e);

        // Mark error as temporary if it's unexpected.
        if kind == ErrorKind::Unexpected {
            err = err.set_temporary();
        }

        err
    }

    pub fn parse_ssh_error(e: russh::Error) -> Error {
        Error::new(ErrorKind::Unexpected, "ssh error").set_source(e)
    }

    pub(crate) fn is_not_found(e: &SftpClientError) -> bool {
        matches!(e, SftpClientError::Status(status) if status.status_code == StatusCode::NoSuchFile)
    }

    pub(crate) fn is_sftp_protocol_error(e: &SftpClientError) -> bool {
        matches!(e, SftpClientError::Status(_))
    }

    pub(crate) fn is_sftp_failure(e: &SftpClientError) -> bool {
        matches!(e, SftpClientError::Status(status) if status.status_code == StatusCode::Failure)
    }

    pub(crate) fn is_eof(e: &SftpClientError) -> bool {
        matches!(e, SftpClientError::Status(status) if status.status_code == StatusCode::Eof)
    }
}

pub(super) use error::*;

mod utils {
    use russh_sftp::protocol::{FileAttributes, FileType};

    use opendal_core::EntryMode;
    use opendal_core::Metadata;
    use opendal_core::raw::Timestamp;

    pub fn to_metadata(attrs: &FileAttributes) -> Metadata {
        let mode = match attrs.file_type() {
            FileType::File => EntryMode::FILE,
            FileType::Dir => EntryMode::DIR,
            _ => EntryMode::Unknown,
        };

        let mut metadata = Metadata::new(mode);

        if let Some(size) = attrs.size {
            metadata.set_content_length(size);
        }

        if let Some(mtime) = attrs.mtime
            && let Ok(ts) = Timestamp::from_second(mtime as i64)
        {
            metadata.set_last_modified(ts);
        }

        metadata
    }
}

pub(super) use utils::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_endpoint() {
        let cases = [
            ("127.0.0.1", (None, "127.0.0.1", 22)),
            ("ssh://127.0.0.1", (None, "127.0.0.1", 22)),
            ("ssh://127.0.0.1:2222", (None, "127.0.0.1", 2222)),
            ("ssh://foo@127.0.0.1:2222", (Some("foo"), "127.0.0.1", 2222)),
            ("foo@example.com", (Some("foo"), "example.com", 22)),
            ("example.com:2222", (None, "example.com", 2222)),
            ("[::1]:2222", (None, "::1", 2222)),
            ("[::1]", (None, "::1", 22)),
            ("ssh://127.0.0.1:2222/ignored", (None, "127.0.0.1", 2222)),
        ];

        for (input, (user, host, port)) in cases {
            let actual = parse_endpoint(input).expect("endpoint must parse");
            assert_eq!(
                actual,
                (user.map(str::to_string), host.to_string(), port),
                "endpoint: {input}"
            );
        }
    }

    #[test]
    fn test_parse_endpoint_invalid() {
        // An unbracketed IPv6 literal is ambiguous with `host:port` and must
        // not be parsed as host ":" on port 1.
        for input in ["", "host:port", "[::1", "::1", "fe80::1:22"] {
            assert!(
                parse_endpoint(input).is_err(),
                "endpoint must be rejected: {input}"
            );
        }
    }

    #[test]
    fn test_abs_path() {
        let core = |root: &str| {
            SftpCore::new(
                ServiceInfo::new(crate::SFTP_SCHEME, root, ""),
                Capability::default(),
                "127.0.0.1".to_string(),
                root.to_string(),
                None,
                None,
                KnownHostsStrategy::Strict,
            )
        };

        let rooted = core("/upload/");
        assert_eq!(rooted.abs_path("a/b.txt"), "/upload/a/b.txt");
        assert_eq!(rooted.abs_path("/a/b.txt"), "/upload/a/b.txt");
        assert_eq!(rooted.abs_path(""), "/upload/");

        // An empty root keeps paths relative to the login directory.
        let rootless = core("");
        assert_eq!(rootless.abs_path("a/b.txt"), "a/b.txt");
        assert_eq!(rootless.abs_path(""), ".");
    }
}
