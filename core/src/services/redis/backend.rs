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

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use http::Uri;
use redis::aio::ConnectionManager;
use redis::cluster::ClusterClient;
use redis::cluster::ClusterClientBuilder;
use redis::cluster_async::ClusterConnection;
use redis::AsyncCommands;
use redis::Client;
use redis::ConnectionAddr;
use redis::ConnectionInfo;
use redis::RedisConnectionInfo;
use redis::RedisError;
use serde::Deserialize;
use tokio::sync::OnceCell;

use crate::raw::adapters::kv;
use crate::raw::*;
use crate::*;

const DEFAULT_REDIS_ENDPOINT: &str = "tcp://127.0.0.1:6379";
const DEFAULT_REDIS_PORT: u16 = 6379;

/// Config for Redis services support.
#[derive(Default, Deserialize, Clone)]
#[serde(default)]
#[non_exhaustive]
pub struct RedisConfig {
    /// network address of the Redis service. Can be "tcp://127.0.0.1:6379", e.g.
    ///
    /// default is "tcp://127.0.0.1:6379"
    endpoint: Option<String>,
    /// network address of the Redis cluster service. Can be "tcp://127.0.0.1:6379,tcp://127.0.0.1:6380,tcp://127.0.0.1:6381", e.g.
    ///
    /// default is None
    cluster_endpoints: Option<String>,
    /// the username to connect redis service.
    ///
    /// default is None
    username: Option<String>,
    /// the password for authentication
    ///
    /// default is None
    password: Option<String>,
    /// the working directory of the Redis service. Can be "/path/to/dir"
    ///
    /// default is "/"
    root: Option<String>,
    /// the number of DBs redis can take is unlimited
    ///
    /// default is db 0
    db: i64,
    /// The default ttl for put operations.
    default_ttl: Option<Duration>,
}

impl Debug for RedisConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("RedisConfig");

        d.field("db", &self.db.to_string());
        d.field("root", &self.root);
        if let Some(endpoint) = self.endpoint.clone() {
            d.field("endpoint", &endpoint);
        }
        if let Some(cluster_endpoints) = self.cluster_endpoints.clone() {
            d.field("cluster_endpoints", &cluster_endpoints);
        }
        if let Some(username) = self.username.clone() {
            d.field("username", &username);
        }
        if self.password.is_some() {
            d.field("password", &"<redacted>");
        }

        d.finish_non_exhaustive()
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
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
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
    pub fn cluster_endpoints(&mut self, cluster_endpoints: &str) -> &mut Self {
        if !cluster_endpoints.is_empty() {
            self.config.cluster_endpoints = Some(cluster_endpoints.to_owned());
        }
        self
    }

    /// set the username for redis
    ///
    /// default: no username
    pub fn username(&mut self, username: &str) -> &mut Self {
        if !username.is_empty() {
            self.config.username = Some(username.to_owned());
        }
        self
    }

    /// set the password for redis
    ///
    /// default: no password
    pub fn password(&mut self, password: &str) -> &mut Self {
        if !password.is_empty() {
            self.config.password = Some(password.to_owned());
        }
        self
    }

    /// set the db used in redis
    ///
    /// default: 0
    pub fn db(&mut self, db: i64) -> &mut Self {
        self.config.db = db;
        self
    }

    /// Set the default ttl for redis services.
    ///
    /// If set, we will specify `EX` for write operations.
    pub fn default_ttl(&mut self, ttl: Duration) -> &mut Self {
        self.config.default_ttl = Some(ttl);
        self
    }

    /// set the working directory, all operations will be performed under it.
    ///
    /// default: "/"
    pub fn root(&mut self, root: &str) -> &mut Self {
        if !root.is_empty() {
            self.config.root = Some(root.to_owned());
        }
        self
    }
}

impl Builder for RedisBuilder {
    const SCHEME: Scheme = Scheme::Redis;
    type Accessor = RedisBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let config = RedisConfig::deserialize(ConfigDeserializer::new(map))
            .expect("config deserialize must succeed");

        RedisBuilder { config }
    }

    fn build(&mut self) -> Result<Self::Accessor> {
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

            Ok(RedisBackend::new(Adapter {
                addr: endpoints,
                client: None,
                cluster_client: Some(client),
                conn,
                default_ttl: self.config.default_ttl,
            })
            .with_root(&root))
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
            Ok(RedisBackend::new(Adapter {
                addr: endpoint,
                client: Some(client),
                cluster_client: None,
                conn,
                default_ttl: self.config.default_ttl,
            })
            .with_root(&root))
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
        };

        Ok(ConnectionInfo {
            addr: con_addr,
            redis: redis_info,
        })
    }
}

/// Backend for redis services.
pub type RedisBackend = kv::Backend<Adapter>;

#[derive(Clone)]
enum RedisConnection {
    Normal(ConnectionManager),
    Cluster(ClusterConnection),
}

#[derive(Clone)]
pub struct Adapter {
    addr: String,
    client: Option<Client>,
    cluster_client: Option<ClusterClient>,
    conn: OnceCell<RedisConnection>,

    default_ttl: Option<Duration>,
}

// implement `Debug` manually, or password may be leaked.
impl Debug for Adapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Adapter");

        ds.field("addr", &self.addr);
        ds.finish()
    }
}

impl Adapter {
    async fn conn(&self) -> Result<RedisConnection> {
        Ok(self
            .conn
            .get_or_try_init(|| async {
                if let Some(client) = self.client.clone() {
                    ConnectionManager::new(client.clone())
                        .await
                        .map(RedisConnection::Normal)
                } else {
                    self.cluster_client
                        .clone()
                        .unwrap()
                        .get_async_connection()
                        .await
                        .map(RedisConnection::Cluster)
                }
            })
            .await
            .map_err(format_redis_error)?
            .clone())
    }
}

#[async_trait]
impl kv::Adapter for Adapter {
    fn metadata(&self) -> kv::Metadata {
        kv::Metadata::new(
            Scheme::Redis,
            self.addr.as_str(),
            Capability {
                read: true,
                write: true,

                ..Default::default()
            },
        )
    }

    async fn get(&self, key: &str) -> Result<Option<Buffer>> {
        let conn = self.conn().await?;
        let result: Option<bytes::Bytes> = match conn {
            RedisConnection::Normal(mut conn) => conn.get(key).await.map_err(format_redis_error),
            RedisConnection::Cluster(mut conn) => conn.get(key).await.map_err(format_redis_error),
        }?;
        Ok(result.map(Buffer::from))
    }

    async fn set(&self, key: &str, value: Buffer) -> Result<()> {
        let conn = self.conn().await?;
        let value = value.as_ref();
        match self.default_ttl {
            Some(ttl) => match conn {
                RedisConnection::Normal(mut conn) => conn
                    .set_ex(key, value, ttl.as_secs() as usize)
                    .await
                    .map_err(format_redis_error)?,
                RedisConnection::Cluster(mut conn) => conn
                    .set_ex(key, value, ttl.as_secs() as usize)
                    .await
                    .map_err(format_redis_error)?,
            },
            None => match conn {
                RedisConnection::Normal(mut conn) => {
                    conn.set(key, value).await.map_err(format_redis_error)?
                }
                RedisConnection::Cluster(mut conn) => {
                    conn.set(key, value).await.map_err(format_redis_error)?
                }
            },
        }
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let conn = self.conn().await?;
        match conn {
            RedisConnection::Normal(mut conn) => {
                let _: () = conn.del(key).await.map_err(format_redis_error)?;
            }
            RedisConnection::Cluster(mut conn) => {
                let _: () = conn.del(key).await.map_err(format_redis_error)?;
            }
        }
        Ok(())
    }

    async fn append(&self, key: &str, value: &[u8]) -> Result<()> {
        let conn = self.conn().await?;
        match conn {
            RedisConnection::Normal(mut conn) => {
                conn.append(key, value).await.map_err(format_redis_error)?;
            }
            RedisConnection::Cluster(mut conn) => {
                conn.append(key, value).await.map_err(format_redis_error)?;
            }
        }
        Ok(())
    }
}

pub fn format_redis_error(e: RedisError) -> Error {
    Error::new(ErrorKind::Unexpected, e.category())
        .set_source(e)
        .set_temporary()
}
