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

use crate::{Buffer, Error, ErrorKind};
use redis::aio::{ConnectionLike, ConnectionManager};
use redis::cluster::ClusterClient;
use redis::cluster_async::ClusterConnection;
use redis::{from_redis_value, AsyncCommands, Client, RedisError};
use std::time::Duration;

#[derive(Clone)]
pub enum RedisConnection {
    Normal(ConnectionManager),
    Cluster(ClusterConnection),
}
impl RedisConnection {
    pub async fn get(&self, key: &str) -> crate::Result<Option<Buffer>> {
        let result: Option<bytes::Bytes> = match self.clone() {
            RedisConnection::Normal(mut conn) => conn.get(key).await.map_err(format_redis_error),
            RedisConnection::Cluster(mut conn) => conn.get(key).await.map_err(format_redis_error),
        }?;
        Ok(result.map(Buffer::from))
    }

    pub async fn set(&self, key: &str, value: Vec<u8>, ttl: Option<Duration>) -> crate::Result<()> {
        let value = value.to_vec();
        if let Some(ttl) = ttl {
            match self.clone() {
                RedisConnection::Normal(mut conn) => conn
                    .set_ex(key, value, ttl.as_secs())
                    .await
                    .map_err(format_redis_error)?,
                RedisConnection::Cluster(mut conn) => conn
                    .set_ex(key, value, ttl.as_secs())
                    .await
                    .map_err(format_redis_error)?,
            }
        } else {
            match self.clone() {
                RedisConnection::Normal(mut conn) => {
                    conn.set(key, value).await.map_err(format_redis_error)?
                }
                RedisConnection::Cluster(mut conn) => {
                    conn.set(key, value).await.map_err(format_redis_error)?
                }
            }
        }

        Ok(())
    }

    pub async fn delete(&self, key: &str) -> crate::Result<()> {
        match self.clone() {
            RedisConnection::Normal(mut conn) => {
                let _: () = conn.del(key).await.map_err(format_redis_error)?;
            }
            RedisConnection::Cluster(mut conn) => {
                let _: () = conn.del(key).await.map_err(format_redis_error)?;
            }
        }

        Ok(())
    }

    pub async fn append(&self, key: &str, value: &[u8]) -> crate::Result<()> {
        match self.clone() {
            RedisConnection::Normal(mut conn) => {
                () = conn.append(key, value).await.map_err(format_redis_error)?;
            }
            RedisConnection::Cluster(mut conn) => {
                () = conn.append(key, value).await.map_err(format_redis_error)?;
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct RedisConnectionManager {
    pub client: Option<Client>,
    pub cluster_client: Option<ClusterClient>,
}

#[async_trait::async_trait]
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
        let pong_value = match conn.clone() {
            RedisConnection::Normal(mut conn) => conn
                .send_packed_command(&redis::cmd("PING"))
                .await
                .map_err(format_redis_error)?,

            RedisConnection::Cluster(mut conn) => conn
                .req_packed_command(&redis::cmd("PING"))
                .await
                .map_err(format_redis_error)?,
        };
        let pong: String = from_redis_value(&pong_value).map_err(format_redis_error)?;

        if pong == String::from("PONG") {
            Ok(())
        } else {
            Err(Error::new(ErrorKind::Unexpected, "PING ERROR"))
        }
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

pub fn format_redis_error(e: RedisError) -> Error {
    Error::new(ErrorKind::Unexpected, e.category())
        .set_source(e)
        .set_temporary()
}
