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

use crate::Error;
use crate::ErrorKind;

use redis::aio::ConnectionLike;
use redis::aio::ConnectionManager;
use redis::cluster::ClusterClient;
use redis::cluster_async::ClusterConnection;
use redis::AsyncCommands;
use redis::Client;
use redis::RedisError;
use redis::{Cmd, Pipeline, RedisFuture, Value};

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

pub fn format_redis_error(e: RedisError) -> Error {
    Error::new(ErrorKind::Unexpected, e.category())
        .set_source(e)
        .set_temporary()
}
