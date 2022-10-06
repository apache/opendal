// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use futures::Future;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::io::Result;
use std::io::{Error, ErrorKind};
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::vec::IntoIter;

use anyhow::anyhow;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::{ready, Stream};
use http::Uri;
use pin_project::pin_project;
use redis::aio::ConnectionManager;
use redis::Client;
use redis::ConnectionAddr;
use redis::ConnectionInfo;
use redis::RedisConnectionInfo;
use redis::{AsyncCommands, RedisError};
use tokio::sync::OnceCell;

use crate::adapters::kv;
use crate::error::new_other_backend_error;
use crate::path::normalize_root;
use crate::Scheme;

const DEFAULT_REDIS_ENDPOINT: &str = "tcp://127.0.0.1:6379";
const DEFAULT_REDIS_PORT: u16 = 6379;

/// Redis backend builder
#[derive(Clone, Default)]
pub struct Builder {
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
}

impl Builder {
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

    /// set the working directory, all operations will be performed under it.
    ///
    /// default: "/"
    pub fn root(&mut self, root: &str) -> &mut Self {
        if !root.is_empty() {
            self.root = Some(root.to_owned());
        }
        self
    }

    /// Establish connection to Redis and finish making Redis endpoint
    pub fn build(&mut self) -> Result<Backend> {
        let endpoint = self
            .endpoint
            .clone()
            .unwrap_or_else(|| DEFAULT_REDIS_ENDPOINT.to_string());

        let ep_url = endpoint.parse::<Uri>().map_err(|e| {
            new_other_backend_error(
                HashMap::from([("endpoint".to_string(), endpoint.clone())]),
                anyhow!("endpoint is invalid: {:?}", e),
            )
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
                return Err(new_other_backend_error(
                    HashMap::from([("endpoint".to_string(), endpoint)]),
                    anyhow!("invalid or unsupported URL scheme: {}", s),
                ))
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
            new_other_backend_error(
                HashMap::from([
                    ("endpoint".to_string(), endpoint),
                    ("db".to_string(), self.db.to_string()),
                    (
                        "username".to_string(),
                        self.username
                            .clone()
                            .unwrap_or_else(|| "<None>".to_string()),
                    ),
                ]),
                anyhow!("establish redis client error: {:?}", e),
            )
        })?;

        let root = normalize_root(
            self.root
                .clone()
                .unwrap_or_else(|| "/".to_string())
                .as_str(),
        );

        let conn = OnceCell::new();
        Ok(Backend::new(Adapter { client, conn }).with_root(&root))
    }
}

impl Debug for Builder {
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

/// Backend for redis services.
pub type Backend = kv::Backend<Adapter>;

impl Backend {
    /// build from iterator
    pub(crate) fn from_iter(it: impl Iterator<Item = (String, String)>) -> Result<Self> {
        let mut builder = Builder::default();
        for (k, v) in it {
            let v = v.as_str();
            match k.as_ref() {
                "root" => builder.root(v),
                "endpoint" => builder.endpoint(v),
                "username" => builder.username(v),
                "password" => builder.password(v),
                "db" => match v.parse::<i64>() {
                    Ok(num) => builder.db(num),
                    _ => continue,
                },
                _ => continue,
            };
        }
        builder.build()
    }
}

#[derive(Clone)]
pub struct Adapter {
    client: Client,
    conn: OnceCell<ConnectionManager>,
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
        match self
            .conn
            .get_or_try_init(|| async { ConnectionManager::new(self.client.clone()).await })
            .await
        {
            Ok(v) => Ok(v.clone()),
            Err(err) => Err(new_redis_error(err)),
        }
    }
}

#[async_trait]
impl kv::Adapter for Adapter {
    fn metadata(&self) -> kv::Metadata {
        kv::Metadata::new(
            Scheme::Redis,
            &self.client.get_connection_info().addr.to_string(),
        )
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut conn = self.conn().await?;
        let bs: Option<Vec<u8>> = conn.get(key).await.map_err(new_redis_error)?;
        Ok(bs)
    }

    async fn set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut conn = self.conn().await?;
        let _: () = conn.set(key, value).await.map_err(new_redis_error)?;
        Ok(())
    }

    async fn scan(&self, prefix: &[u8]) -> Result<kv::KeyStreamer> {
        let conn = self.conn().await?;
        Ok(Box::new(KeyStream::new(conn, prefix)))
    }

    async fn delete(&self, key: &[u8]) -> Result<()> {
        let mut conn = self.conn().await?;
        let _: () = conn.del(key).await.map_err(new_redis_error)?;
        Ok(())
    }
}

#[pin_project]
#[allow(clippy::type_complexity)]
struct KeyStream {
    conn: ConnectionManager,
    arg: Vec<u8>,

    done: bool,
    cursor: u64,
    keys: IntoIter<Vec<u8>>,
    /// Yep, this type is complex. But they are simple to understand and
    /// already implemented `FromRedisValue`.
    ///
    /// (cursor, Vec<keys in vec>)
    fut: Option<BoxFuture<'static, Result<(u64, Vec<Vec<u8>>)>>>,
}

impl KeyStream {
    fn new(conn: ConnectionManager, arg: &[u8]) -> Self {
        Self {
            conn,
            arg: arg.to_vec(),

            done: false,
            cursor: 0,
            keys: vec![].into_iter(),
            fut: None,
        }
    }
}

impl Stream for KeyStream {
    type Item = Result<Vec<u8>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        loop {
            if let Some(key) = this.keys.next() {
                return Poll::Ready(Some(Ok(key)));
            }

            match this.fut {
                None => {
                    if *this.done {
                        return Poll::Ready(None);
                    }

                    let arg = this.arg.to_vec();
                    let cursor = *this.cursor;
                    let mut conn = this.conn.clone();
                    let fut = async move {
                        let (cursor, keys) = redis::cmd("scan")
                            .cursor_arg(cursor)
                            .arg("MATCH")
                            .arg(arg.into_iter().chain(vec![b'*']).collect::<Vec<_>>())
                            .query_async(&mut conn)
                            .await
                            .map_err(new_redis_error)?;

                        Ok((cursor, keys))
                    };
                    *this.fut = Some(Box::pin(fut));
                    continue;
                }
                Some(fut) => {
                    let (cursor, keys) = ready!(Pin::new(fut).poll(cx))?;

                    *this.fut = None;

                    if cursor == 0 {
                        *this.done = true;
                    }
                    *this.cursor = cursor;
                    *this.keys = keys.into_iter();
                    continue;
                }
            }
        }
    }
}

fn new_redis_error(err: RedisError) -> Error {
    Error::new(ErrorKind::Other, anyhow!("redis: {err:?}"))
}
