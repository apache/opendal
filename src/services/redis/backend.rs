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
use redis::AsyncCommands;
use redis::Client;
use redis::ConnectionAddr;
use redis::ConnectionInfo;
use redis::RedisConnectionInfo;
use redis::RedisError;
use tokio::sync::OnceCell;

use crate::raw::adapters::kv;
use crate::raw::*;
use crate::*;

const DEFAULT_REDIS_ENDPOINT: &str = "tcp://127.0.0.1:6379";
const DEFAULT_REDIS_PORT: u16 = 6379;

/// [Redis](https://redis.io/) services support.
///
/// # Capabilities
///
/// This service can be used to:
///
/// - [x] read
/// - [x] write
/// - [ ] ~~list~~
/// - [ ] scan
/// - [ ] ~~presign~~
/// - [ ] blocking
///
/// # Configuration
///
/// - `root`: Set the working directory of `OpenDAL`
/// - `endpoint`: Set the network address of redis server
/// - `username`: Set the username of Redis
/// - `password`: Set the password for authentication
/// - `db`: Set the DB of redis
///
/// You can refer to [`RedisBuilder`]'s docs for more information
///
/// # Example
///
/// ## Via Builder
///
/// ```no_run
/// use anyhow::Result;
/// use opendal::services::Redis;
/// use opendal::Object;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let mut builder = Redis::default();
///
///     // this will build a Operator accessing Redis which runs on tcp://localhost:6379
///     let op: Operator = Operator::new(builder)?.finish();
///     let _: Object = op.object("test_file");
///     Ok(())
/// }
/// ```
#[derive(Clone, Default)]
pub struct RedisBuilder {
    /// network address of the Redis service. Can be "tcp://127.0.0.1:6379", e.g.
    ///
    /// default is "tcp://127.0.0.1:6379"
    endpoint: Option<String>,
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

impl Debug for RedisBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Builder");
        ds.field("db", &self.db.to_string());
        ds.field("root", &self.root);
        if let Some(endpoint) = self.endpoint.clone() {
            ds.field("endpoint", &endpoint);
        }
        if let Some(username) = self.username.clone() {
            ds.field("username", &username);
        }
        if self.password.is_some() {
            ds.field("password", &"<redacted>");
        }
        ds.finish()
    }
}

impl RedisBuilder {
    /// set the network address of redis service.
    ///
    /// currently supported schemes:
    /// - no scheme: will be seen as "tcp"
    /// - "tcp" or "redis": unsecured redis connections
    /// - "unix" or "redis+unix": unix socket connection
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        if !endpoint.is_empty() {
            self.endpoint = Some(endpoint.to_owned());
        }
        self
    }

    /// set the username for redis
    ///
    /// default: no username
    pub fn username(&mut self, username: &str) -> &mut Self {
        if !username.is_empty() {
            self.username = Some(username.to_owned());
        }
        self
    }

    /// set the password for redis
    ///
    /// default: no password
    pub fn password(&mut self, password: &str) -> &mut Self {
        if !password.is_empty() {
            self.password = Some(password.to_owned());
        }
        self
    }

    /// set the db used in redis
    ///
    /// default: 0
    pub fn db(&mut self, db: i64) -> &mut Self {
        self.db = db;
        self
    }

    /// Set the default ttl for redis services.
    ///
    /// If set, we will specify `EX` for write operations.
    pub fn default_ttl(&mut self, ttl: Duration) -> &mut Self {
        self.default_ttl = Some(ttl);
        self
    }

    /// set the working directory, all operations will be performed under it.
    ///
    /// default: "/"
    pub fn root(&mut self, root: &str) -> &mut Self {
        if !root.is_empty() {
            self.root = Some(root.to_owned());
        }
        self
    }
}

impl Builder for RedisBuilder {
    const SCHEME: Scheme = Scheme::Redis;
    type Accessor = RedisBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = RedisBuilder::default();

        map.get("root").map(|v| builder.root(v));
        map.get("endpoint").map(|v| builder.endpoint(v));
        map.get("username").map(|v| builder.username(v));
        map.get("password").map(|v| builder.password(v));
        map.get("db")
            .map(|v| v.parse::<i64>().map(|v| builder.db(v)));

        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        let endpoint = self
            .endpoint
            .clone()
            .unwrap_or_else(|| DEFAULT_REDIS_ENDPOINT.to_string());

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
            // TODO: wait for upstream to support `rustls` based TLS connection.
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
            db: self.db,
            username: self.username.clone(),
            password: self.password.clone(),
        };

        let con_info = ConnectionInfo {
            addr: con_addr,
            redis: redis_info,
        };

        let client = Client::open(con_info).map_err(|e| {
            Error::new(ErrorKind::ConfigInvalid, "invalid or unsupported scheme")
                .with_context("service", Scheme::Redis)
                .with_context("endpoint", self.endpoint.as_ref().unwrap())
                .with_context("db", self.db.to_string())
                .set_source(e)
        })?;

        let root = normalize_root(
            self.root
                .clone()
                .unwrap_or_else(|| "/".to_string())
                .as_str(),
        );

        let conn = OnceCell::new();
        Ok(RedisBackend::new(Adapter {
            client,
            conn,
            default_ttl: self.default_ttl,
        })
        .with_root(&root))
    }
}

/// Backend for redis services.
pub type RedisBackend = kv::Backend<Adapter>;

#[derive(Clone)]
pub struct Adapter {
    client: Client,
    conn: OnceCell<ConnectionManager>,

    default_ttl: Option<Duration>,
}

// implement `Debug` manually, or password may be leaked.
impl Debug for Adapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Adapter");

        let info = self.client.get_connection_info();
        ds.field("addr", &info.addr);
        ds.field("db", &info.redis.db);
        ds.field("user", &info.redis.username);
        ds.finish()
    }
}

impl Adapter {
    async fn conn(&self) -> Result<ConnectionManager> {
        Ok(self
            .conn
            .get_or_try_init(|| async { ConnectionManager::new(self.client.clone()).await })
            .await?
            .clone())
    }
}

#[async_trait]
impl kv::Adapter for Adapter {
    fn metadata(&self) -> kv::Metadata {
        kv::Metadata::new(
            Scheme::Redis,
            &self.client.get_connection_info().addr.to_string(),
            AccessorCapability::Read | AccessorCapability::Write,
        )
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let mut conn = self.conn().await?;
        let bs: Option<Vec<u8>> = conn.get(key).await?;
        Ok(bs)
    }

    async fn set(&self, key: &str, value: &[u8]) -> Result<()> {
        let mut conn = self.conn().await?;
        match self.default_ttl {
            Some(ttl) => conn.set_ex(key, value, ttl.as_secs() as usize).await?,
            None => conn.set(key, value).await?,
        }
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let mut conn = self.conn().await?;
        let _: () = conn.del(key).await?;
        Ok(())
    }

    async fn append(&self, key: &str, value: &[u8]) -> Result<()> {
        let mut conn = self.conn().await?;
        conn.append(key, value).await?;
        Ok(())
    }
}

impl From<RedisError> for Error {
    fn from(e: RedisError) -> Self {
        Error::new(ErrorKind::Unexpected, e.category()).set_source(e)
    }
}
