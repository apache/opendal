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
use std::time::Duration;

use bb8::RunError;
use bytes::Bytes;
use redis::AsyncCommands;
use redis::Client;
use redis::Cmd;
use redis::Pipeline;
use redis::RedisError;
use redis::RedisFuture;
use redis::Value;
use redis::aio::ConnectionLike;
use redis::aio::ConnectionManager;
use redis::cluster::ClusterClient;
use redis::cluster_async::ClusterConnection;
use tokio::sync::OnceCell;

use crate::*;

#[derive(Clone)]
pub enum RedisConnection {
    Normal(ConnectionManager),
    Cluster(ClusterConnection),
}

impl ConnectionLike for RedisConnection {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        match self {
            RedisConnection::Normal(conn) => conn.req_packed_command(cmd),
            RedisConnection::Cluster(conn) => conn.req_packed_command(cmd),
        }
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        match self {
            RedisConnection::Normal(conn) => conn.req_packed_commands(cmd, offset, count),
            RedisConnection::Cluster(conn) => conn.req_packed_commands(cmd, offset, count),
        }
    }

    fn get_db(&self) -> i64 {
        match self {
            RedisConnection::Normal(conn) => conn.get_db(),
            RedisConnection::Cluster(conn) => conn.get_db(),
        }
    }
}

#[derive(Clone)]
pub struct RedisConnectionManager {
    pub client: Option<Client>,
    pub cluster_client: Option<ClusterClient>,
}

impl bb8::ManageConnection for RedisConnectionManager {
    type Connection = RedisConnection;
    type Error = Error;

    async fn connect(&self) -> Result<RedisConnection, Self::Error> {
        if let Some(client) = self.client.clone() {
            ConnectionManager::new(client.clone())
                .await
                .map_err(format_redis_error)
                .map(RedisConnection::Normal)
        } else {
            self.cluster_client
                .clone()
                .unwrap()
                .get_async_connection()
                .await
                .map_err(format_redis_error)
                .map(RedisConnection::Cluster)
        }
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        let pong: String = conn.ping().await.map_err(format_redis_error)?;

        if pong == "PONG" {
            Ok(())
        } else {
            Err(Error::new(ErrorKind::Unexpected, "PING ERROR"))
        }
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

/// RedisCore holds the Redis connection and configuration
#[derive(Clone)]
pub struct RedisCore {
    pub addr: String,
    pub client: Option<Client>,
    pub cluster_client: Option<ClusterClient>,
    pub conn: OnceCell<bb8::Pool<RedisConnectionManager>>,
    pub default_ttl: Option<Duration>,
}

impl Debug for RedisCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisCore")
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}

impl RedisCore {
    pub async fn conn(&self) -> Result<bb8::PooledConnection<'_, RedisConnectionManager>> {
        let pool = self
            .conn
            .get_or_try_init(|| async {
                bb8::Pool::builder()
                    .build(self.get_redis_connection_manager())
                    .await
                    .map_err(|err| {
                        Error::new(ErrorKind::ConfigInvalid, "connect to redis failed")
                            .set_source(err)
                    })
            })
            .await?;
        pool.get().await.map_err(|err| match err {
            RunError::TimedOut => {
                Error::new(ErrorKind::Unexpected, "get connection from pool failed").set_temporary()
            }
            RunError::User(err) => err,
        })
    }

    pub fn get_redis_connection_manager(&self) -> RedisConnectionManager {
        if let Some(_client) = self.client.clone() {
            RedisConnectionManager {
                client: self.client.clone(),
                cluster_client: None,
            }
        } else {
            RedisConnectionManager {
                client: None,
                cluster_client: self.cluster_client.clone(),
            }
        }
    }

    pub async fn get(&self, key: &str) -> Result<Option<Buffer>> {
        let mut conn = self.conn().await?;
        let result: Option<Bytes> = conn.get(key).await.map_err(format_redis_error)?;
        Ok(result.map(Buffer::from))
    }

    pub async fn get_range(&self, key: &str, start: isize, end: isize) -> Result<Option<Buffer>> {
        let mut conn = self.conn().await?;
        let result: Option<Bytes> = conn
            .getrange(key, start, end)
            .await
            .map_err(format_redis_error)?;
        Ok(result.map(Buffer::from))
    }

    pub async fn set(&self, key: &str, value: Buffer) -> Result<()> {
        let mut conn = self.conn().await?;
        let value = value.to_vec();
        if let Some(dur) = self.default_ttl {
            let _: () = conn
                .set_ex(key, value, dur.as_secs())
                .await
                .map_err(format_redis_error)?;
        } else {
            let _: () = conn.set(key, value).await.map_err(format_redis_error)?;
        }
        Ok(())
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        let mut conn = self.conn().await?;
        let _: () = conn.del(key).await.map_err(format_redis_error)?;
        Ok(())
    }
}

pub fn format_redis_error(e: RedisError) -> Error {
    Error::new(ErrorKind::Unexpected, e.category())
        .set_source(e)
        .set_temporary()
}
