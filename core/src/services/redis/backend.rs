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
use std::path::PathBuf;
use std::time::Duration;

use http::Uri;
use redis::cluster::ClusterClientBuilder;
use redis::Client;
use redis::ConnectionAddr;
use redis::ConnectionInfo;
use redis::ProtocolVersion;
use redis::RedisConnectionInfo;
use tokio::sync::OnceCell;

use super::core::*;
use super::delete::RedisDeleter;
use super::writer::RedisWriter;
use super::DEFAULT_SCHEME;
use crate::raw::oio;
use crate::raw::*;
use crate::services::RedisConfig;
use crate::*;
const DEFAULT_REDIS_ENDPOINT: &str = "tcp://127.0.0.1:6379";
const DEFAULT_REDIS_PORT: u16 = 6379;

impl Configurator for RedisConfig {
    type Builder = RedisBuilder;
    fn into_builder(self) -> Self::Builder {
        RedisBuilder { config: self }
    }
}

/// [Redis](https://redis.io/) services support.
#[doc = include_str!("docs.md")]
#[derive(Clone, Default)]
pub struct RedisBuilder {
    config: RedisConfig,
}

impl Debug for RedisBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("RedisBuilder");

        d.field("config", &self.config);
        d.finish_non_exhaustive()
    }
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
}

impl Builder for RedisBuilder {
    type Config = RedisConfig;

    fn build(self) -> Result<impl Access> {
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

            let conn = OnceCell::new();

            Ok(RedisAccessor::new(RedisCore {
                addr: endpoints,
                client: None,
                cluster_client: Some(client),
                conn,
                default_ttl: self.config.default_ttl,
            })
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
                        .with_context("service", Scheme::Redis)
                        .with_context("endpoint", self.config.endpoint.as_ref().unwrap())
                        .with_context("db", self.config.db.to_string())
                        .set_source(e)
                })?;

            let conn = OnceCell::new();
            Ok(RedisAccessor::new(RedisCore {
                addr: endpoint,
                client: Some(client),
                cluster_client: None,
                conn,
                default_ttl: self.config.default_ttl,
            })
            .with_normalized_root(root))
        }
    }
}

impl RedisBuilder {
    fn get_connection_info(&self, endpoint: String) -> Result<ConnectionInfo> {
        let ep_url = endpoint.parse::<Uri>().map_err(|e| {
            Error::new(ErrorKind::ConfigInvalid, "endpoint is invalid")
                .with_context("service", Scheme::Redis)
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
                        .with_context("service", Scheme::Redis)
                        .with_context("scheme", s),
                )
            }
        };

        let redis_info = RedisConnectionInfo {
            db: self.config.db,
            username: self.config.username.clone(),
            password: self.config.password.clone(),
            protocol: ProtocolVersion::RESP2,
        };

        Ok(ConnectionInfo {
            addr: con_addr,
            redis: redis_info,
        })
    }
}

/// RedisAccessor implements Access trait directly
#[derive(Debug, Clone)]
pub struct RedisAccessor {
    core: std::sync::Arc<RedisCore>,
    root: String,
    info: std::sync::Arc<AccessorInfo>,
}

impl RedisAccessor {
    fn new(core: RedisCore) -> Self {
        let info = AccessorInfo::default();
        info.set_scheme(DEFAULT_SCHEME);
        info.set_name(&core.addr);
        info.set_root("/");
        info.set_native_capability(Capability {
            read: true,
            write: true,
            delete: true,
            stat: true,
            write_can_empty: true,
            shared: true,
            ..Default::default()
        });

        Self {
            core: std::sync::Arc::new(core),
            root: "/".to_string(),
            info: std::sync::Arc::new(info),
        }
    }

    fn with_normalized_root(mut self, root: String) -> Self {
        self.info.set_root(&root);
        self.root = root;
        self
    }
}

impl Access for RedisAccessor {
    type Reader = Buffer;
    type Writer = RedisWriter;
    type Lister = ();
    type Deleter = oio::OneShotDeleter<RedisDeleter>;

    fn info(&self) -> std::sync::Arc<AccessorInfo> {
        self.info.clone()
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
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

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let p = build_abs_path(&self.root, path);

        let range = args.range();
        let buffer = if range.is_full() {
            // Full read - use GET
            match self.core.get(&p).await? {
                Some(bs) => bs,
                None => return Err(Error::new(ErrorKind::NotFound, "key not found in redis")),
            }
        } else {
            // Range read - use GETRANGE
            let start = range.offset() as isize;
            let end = match range.size() {
                Some(size) => (range.offset() + size - 1) as isize,
                None => -1, // Redis uses -1 for end of string
            };

            match self.core.get_range(&p, start, end).await? {
                Some(bs) => bs,
                None => return Err(Error::new(ErrorKind::NotFound, "key not found in redis")),
            }
        };

        Ok((RpRead::new(), buffer))
    }

    async fn write(&self, path: &str, _: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let p = build_abs_path(&self.root, path);
        Ok((RpWrite::new(), RedisWriter::new(self.core.clone(), p)))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(RedisDeleter::new(self.core.clone(), self.root.clone())),
        ))
    }

    async fn list(&self, path: &str, _: OpList) -> Result<(RpList, Self::Lister)> {
        let _ = build_abs_path(&self.root, path);
        // Redis doesn't support listing keys, return empty list
        Ok((RpList::default(), ()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_redis_accessor_creation() {
        let core = RedisCore {
            addr: "redis://127.0.0.1:6379".to_string(),
            client: None,
            cluster_client: None,
            conn: OnceCell::new(),
            default_ttl: Some(Duration::from_secs(60)),
        };

        let accessor = RedisAccessor::new(core);

        // Verify basic properties
        assert_eq!(accessor.root, "/");
        assert_eq!(accessor.info.scheme(), "redis");
        assert!(accessor.info.native_capability().read);
        assert!(accessor.info.native_capability().write);
        assert!(accessor.info.native_capability().delete);
        assert!(accessor.info.native_capability().stat);
    }

    #[test]
    fn test_redis_accessor_with_root() {
        let core = RedisCore {
            addr: "redis://127.0.0.1:6379".to_string(),
            client: None,
            cluster_client: None,
            conn: OnceCell::new(),
            default_ttl: None,
        };

        let accessor = RedisAccessor::new(core).with_normalized_root("/test/".to_string());

        assert_eq!(accessor.root, "/test/");
        assert_eq!(accessor.info.root(), "/test/".into());
    }

    #[test]
    fn test_redis_builder_interface() {
        // Test that RedisBuilder still works with the new implementation
        let builder = RedisBuilder::default()
            .endpoint("redis://localhost:6379")
            .username("testuser")
            .password("testpass")
            .db(1)
            .root("/test");

        // The builder should be able to create configuration
        assert!(builder.config.endpoint.is_some());
        assert_eq!(
            builder.config.endpoint.as_ref().unwrap(),
            "redis://localhost:6379"
        );
        assert_eq!(builder.config.username.as_ref().unwrap(), "testuser");
        assert_eq!(builder.config.password.as_ref().unwrap(), "testpass");
        assert_eq!(builder.config.db, 1);
        assert_eq!(builder.config.root.as_ref().unwrap(), "/test");
    }
}
