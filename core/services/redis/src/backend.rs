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

use std::path::PathBuf;
use std::sync::Arc;

use http::Uri;
use opendal_core::raw::*;
use opendal_core::*;
use redis::Client;
use redis::ConnectionAddr;
use redis::ConnectionInfo;
use redis::IntoConnectionInfo;
use redis::ProtocolVersion;
use redis::RedisConnectionInfo;
use redis::cluster::ClusterClientBuilder;

use super::REDIS_SCHEME;
use super::config::RedisConfig;
use super::core::*;
use super::delete::RedisDeleter;
use super::writer::RedisWriter;

const DEFAULT_REDIS_ENDPOINT: &str = "tcp://127.0.0.1:6379";
const DEFAULT_REDIS_PORT: u16 = 6379;

/// [Redis](https://redis.io/) services support.
#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct RedisBuilder {
    pub(super) config: RedisConfig,
}

impl RedisBuilder {
    /// set the network address of redis service.
    ///
    /// currently supported schemes:
    /// - no scheme: will be seen as "tcp"
    /// - "tcp" or "redis": unsecured redis connections
    /// - "rediss": secured redis connections
    /// - "unix" or "redis+unix": unix socket connection
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        if !endpoint.is_empty() {
            self.config.endpoint = Some(endpoint.to_owned());
        }
        self
    }

    /// set the network address of redis cluster service.
    /// This parameter is mutually exclusive with the endpoint parameter.
    ///
    /// currently supported schemes:
    /// - no scheme: will be seen as "tcp"
    /// - "tcp" or "redis": unsecured redis connections
    /// - "rediss": secured redis connections
    /// - "unix" or "redis+unix": unix socket connection
    pub fn cluster_endpoints(mut self, cluster_endpoints: &str) -> Self {
        if !cluster_endpoints.is_empty() {
            self.config.cluster_endpoints = Some(cluster_endpoints.to_owned());
        }
        self
    }

    /// set the username for redis
    ///
    /// default: no username
    pub fn username(mut self, username: &str) -> Self {
        if !username.is_empty() {
            self.config.username = Some(username.to_owned());
        }
        self
    }

    /// set the password for redis
    ///
    /// default: no password
    pub fn password(mut self, password: &str) -> Self {
        if !password.is_empty() {
            self.config.password = Some(password.to_owned());
        }
        self
    }

    /// set the db used in redis
    ///
    /// default: 0
    pub fn db(mut self, db: i64) -> Self {
        self.config.db = db;
        self
    }

    /// Set the default ttl for redis services.
    ///
    /// If set, we will specify `EX` for write operations.
    pub fn default_ttl(mut self, ttl: Duration) -> Self {
        self.config.default_ttl = Some(ttl);
        self
    }

    /// set the working directory, all operations will be performed under it.
    ///
    /// default: "/"
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// Sets the maximum number of connections managed by the pool.
    ///
    /// Defaults to 10.
    ///
    /// # Panics
    ///
    /// Will panic if `max_size` is 0.
    #[must_use]
    pub fn connection_pool_max_size(mut self, max_size: usize) -> Self {
        assert!(max_size > 0, "max_size must be greater than zero!");
        self.config.connection_pool_max_size = Some(max_size);
        self
    }
}

impl Builder for RedisBuilder {
    type Config = RedisConfig;

    fn build(self) -> Result<impl Service> {
        let root = normalize_root(
            self.config
                .root
                .clone()
                .unwrap_or_else(|| "/".to_string())
                .as_str(),
        );

        if let Some(endpoints) = self.config.cluster_endpoints.clone() {
            let mut cluster_endpoints: Vec<ConnectionInfo> = Vec::default();
            for endpoint in endpoints.split(',') {
                cluster_endpoints.push(self.get_connection_info(endpoint.to_string())?);
            }
            let mut client_builder = ClusterClientBuilder::new(cluster_endpoints);
            if let Some(username) = &self.config.username {
                client_builder = client_builder.username(username.clone());
            }
            if let Some(password) = &self.config.password {
                client_builder = client_builder.password(password.clone());
            }
            let client = client_builder.build().map_err(format_redis_error)?;

            Ok(RedisBackend::new(RedisCore::new(
                endpoints,
                None,
                Some(client),
                self.config.default_ttl,
                self.config.connection_pool_max_size,
            ))
            .with_normalized_root(root))
        } else {
            let endpoint = self
                .config
                .endpoint
                .clone()
                .unwrap_or_else(|| DEFAULT_REDIS_ENDPOINT.to_string());

            let client =
                Client::open(self.get_connection_info(endpoint.clone())?).map_err(|e| {
                    Error::new(ErrorKind::ConfigInvalid, "invalid or unsupported scheme")
                        .with_context("service", REDIS_SCHEME)
                        .with_context("endpoint", self.config.endpoint.as_ref().unwrap())
                        .with_context("db", self.config.db.to_string())
                        .set_source(e)
                })?;

            Ok(RedisBackend::new(RedisCore::new(
                endpoint,
                Some(client),
                None,
                self.config.default_ttl,
                self.config.connection_pool_max_size,
            ))
            .with_normalized_root(root))
        }
    }
}

impl RedisBuilder {
    fn get_connection_info(&self, endpoint: String) -> Result<ConnectionInfo> {
        let ep_url = endpoint.parse::<Uri>().map_err(|e| {
            Error::new(ErrorKind::ConfigInvalid, "endpoint is invalid")
                .with_context("service", REDIS_SCHEME)
                .with_context("endpoint", endpoint)
                .set_source(e)
        })?;

        let con_addr = match ep_url.scheme_str() {
            Some("tcp") | Some("redis") | None => {
                let host = ep_url
                    .host()
                    .map(|h| h.to_string())
                    .unwrap_or_else(|| "127.0.0.1".to_string());
                let port = ep_url.port_u16().unwrap_or(DEFAULT_REDIS_PORT);
                ConnectionAddr::Tcp(host, port)
            }
            Some("rediss") => {
                let host = ep_url
                    .host()
                    .map(|h| h.to_string())
                    .unwrap_or_else(|| "127.0.0.1".to_string());
                let port = ep_url.port_u16().unwrap_or(DEFAULT_REDIS_PORT);
                ConnectionAddr::TcpTls {
                    host,
                    port,
                    insecure: false,
                    tls_params: None,
                }
            }
            Some("unix") | Some("redis+unix") => {
                let path = PathBuf::from(ep_url.path());
                ConnectionAddr::Unix(path)
            }
            Some(s) => {
                return Err(
                    Error::new(ErrorKind::ConfigInvalid, "invalid or unsupported scheme")
                        .with_context("service", REDIS_SCHEME)
                        .with_context("scheme", s),
                );
            }
        };

        let mut redis_info = RedisConnectionInfo::default()
            .set_db(self.config.db)
            .set_protocol(ProtocolVersion::RESP2);
        if let Some(username) = &self.config.username {
            redis_info = redis_info.set_username(username);
        }
        if let Some(password) = &self.config.password {
            redis_info = redis_info.set_password(password);
        }
        let connection_info = con_addr
            .clone()
            .into_connection_info()
            .map_err(|err| {
                Error::new(ErrorKind::ConfigInvalid, "invalid connection address")
                    .with_context("service", REDIS_SCHEME)
                    .with_context("address", con_addr)
                    .with_context("error", err)
            })?
            .set_redis_settings(redis_info);

        Ok(connection_info)
    }
}

/// RedisBackend implements [`Service`] for Redis-compatible key-value stores.
#[derive(Debug, Clone)]
pub struct RedisBackend {
    core: Arc<RedisCore>,
    root: String,
    info: ServiceInfo,
    capability: Capability,
}

impl RedisBackend {
    fn new(core: RedisCore) -> Self {
        let info = ServiceInfo::new(REDIS_SCHEME, "/", core.addr());
        let capability = Capability {
            read: true,
            write: true,
            delete: true,
            stat: true,
            write_can_empty: true,
            shared: true,
            ..Default::default()
        };

        Self {
            core: Arc::new(core),
            root: "/".to_string(),
            info,
            capability,
        }
    }

    fn with_normalized_root(mut self, root: String) -> Self {
        self.info = self.info.with_root(&root);
        self.root = root;
        self
    }
}

/// Reader returned by this backend.
pub struct RedisReader {
    backend: RedisBackend,
    path: String,
}

impl RedisReader {
    fn new(backend: RedisBackend, path: &str, _: OpRead) -> Self {
        Self {
            backend,
            path: path.to_string(),
        }
    }
}

impl oio::StreamRead for RedisReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let p = build_abs_path(&backend.root, path);

        let (buffer, metadata) = if range.is_full() {
            // Full read - use GET
            match backend.core.get(&p).await? {
                Some(bs) => {
                    let metadata =
                        Metadata::new(EntryMode::FILE).with_content_length(bs.len() as u64);
                    (bs, Some(metadata))
                }
                None => return Err(Error::new(ErrorKind::NotFound, "key not found in redis")),
            }
        } else {
            // Range read - use GETRANGE
            let content_length = match backend.core.len(&p).await? {
                Some(v) => v,
                None => return Err(Error::new(ErrorKind::NotFound, "key not found in redis")),
            };
            let content_range = range.to_content_range(content_length)?;

            let buffer = if content_range.is_empty() {
                Buffer::new()
            } else {
                let start: isize = content_range.start.try_into().map_err(|err| {
                    Error::new(ErrorKind::Unexpected, "range start exceeds isize::MAX")
                        .set_source(err)
                })?;
                let end: isize = (content_range.end - 1).try_into().map_err(|err| {
                    Error::new(ErrorKind::Unexpected, "range end exceeds isize::MAX")
                        .set_source(err)
                })?;
                match backend.core.get_range(&p, start, end).await? {
                    Some(bs) => bs,
                    None => {
                        return Err(Error::new(ErrorKind::NotFound, "key not found in redis"));
                    }
                }
            };
            let metadata =
                Metadata::new(EntryMode::FILE).with_content_length(content_length as u64);
            (buffer, Some(metadata))
        };

        let rp = metadata.map_or_else(RpRead::default, RpRead::new);
        Ok((rp, Box::new(buffer) as Box<dyn oio::ReadStreamDyn>))
    }
}

impl Service for RedisBackend {
    type Reader = oio::StreamReader<RedisReader>;
    type Writer = RedisWriter;
    type Lister = ();
    type Deleter = oio::OneShotDeleter<RedisDeleter>;
    type Copier = ();

    fn info(&self) -> ServiceInfo {
        self.info.clone()
    }

    fn capability(&self) -> Capability {
        self.capability
    }

    async fn create_dir(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn stat(&self, _ctx: &OperationContext, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_abs_path(&self.root, path);

        if p == build_abs_path(&self.root, "") {
            Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
        } else {
            let bs = self.core.get(&p).await?;
            match bs {
                Some(bs) => Ok(RpStat::new(
                    Metadata::new(EntryMode::FILE).with_content_length(bs.len() as u64),
                )),
                None => Err(Error::new(ErrorKind::NotFound, "key not found in redis")),
            }
        }
    }
    async fn read(
        &self,
        _ctx: &OperationContext,
        path: &str,
        args: OpRead,
    ) -> Result<(RpRead, Self::Reader)> {
        let (rp, output): (_, oio::StreamReader<RedisReader>) = {
            Ok((
                RpRead::default(),
                oio::StreamReader::new(RedisReader::new(self.clone(), path, args)),
            ))
        }?;

        Ok((rp, output))
    }

    async fn write(
        &self,
        _ctx: &OperationContext,
        path: &str,
        _: OpWrite,
    ) -> Result<(RpWrite, Self::Writer)> {
        let (rp, output): (_, RedisWriter) = {
            let p = build_abs_path(&self.root, path);
            Ok((RpWrite::new(), RedisWriter::new(self.core.clone(), p)))
        }?;

        Ok((rp, output))
    }

    async fn delete(&self, _ctx: &OperationContext) -> Result<(RpDelete, Self::Deleter)> {
        let (rp, output): (_, oio::OneShotDeleter<RedisDeleter>) = {
            Ok((
                RpDelete::default(),
                oio::OneShotDeleter::new(RedisDeleter::new(self.core.clone(), self.root.clone())),
            ))
        }?;

        Ok((rp, output))
    }

    async fn list(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpList,
    ) -> Result<(RpList, Self::Lister)> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn copy(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpCopy,
        _opts: OpCopier,
    ) -> Result<(RpCopy, Self::Copier)> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn rename(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpRename,
    ) -> Result<RpRename> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
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
