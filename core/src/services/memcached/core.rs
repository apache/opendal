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

use std::time::Duration;

use bb8::RunError;
use tokio::net::TcpStream;
use tokio::sync::OnceCell;

use super::binary;
use crate::raw::*;
use crate::*;

/// A `bb8::ManageConnection` for `memcache_async::ascii::Protocol`.
#[derive(Clone, Debug)]
pub struct MemcacheConnectionManager {
    address: String,
    username: Option<String>,
    password: Option<String>,
}

impl MemcacheConnectionManager {
    fn new(address: &str, username: Option<String>, password: Option<String>) -> Self {
        Self {
            address: address.to_string(),
            username,
            password,
        }
    }
}

impl bb8::ManageConnection for MemcacheConnectionManager {
    type Connection = binary::Connection;
    type Error = Error;

    /// TODO: Implement unix stream support.
    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let conn = TcpStream::connect(&self.address)
            .await
            .map_err(new_std_io_error)?;
        let mut conn = binary::Connection::new(conn);

        if let (Some(username), Some(password)) = (self.username.as_ref(), self.password.as_ref()) {
            conn.auth(username, password).await?;
        }
        Ok(conn)
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.version().await.map(|_| ())
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

#[derive(Clone, Debug)]
pub struct MemcachedCore {
    pub conn: OnceCell<bb8::Pool<MemcacheConnectionManager>>,
    pub endpoint: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub default_ttl: Option<Duration>,
    pub connection_pool_max_size: Option<u32>,
}

impl MemcachedCore {
    async fn conn(&self) -> Result<bb8::PooledConnection<'_, MemcacheConnectionManager>> {
        let pool = self
            .conn
            .get_or_try_init(|| async {
                let mgr = MemcacheConnectionManager::new(
                    &self.endpoint,
                    self.username.clone(),
                    self.password.clone(),
                );

                bb8::Pool::builder()
                    .max_size(self.connection_pool_max_size.unwrap_or(10))
                    .build(mgr)
                    .await
                    .map_err(|err| {
                        Error::new(ErrorKind::ConfigInvalid, "connect to memecached failed")
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

    pub async fn get(&self, key: &str) -> Result<Option<Buffer>> {
        let mut conn = self.conn().await?;
        let result = conn.get(&percent_encode_path(key)).await?;
        Ok(result.map(Buffer::from))
    }

    pub async fn set(&self, key: &str, value: Buffer) -> Result<()> {
        let mut conn = self.conn().await?;

        conn.set(
            &percent_encode_path(key),
            &value.to_vec(),
            // Set expiration to 0 if ttl not set.
            self.default_ttl
                .map(|v| v.as_secs() as u32)
                .unwrap_or_default(),
        )
        .await
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        let mut conn = self.conn().await?;

        conn.delete(&percent_encode_path(key)).await
    }
}
