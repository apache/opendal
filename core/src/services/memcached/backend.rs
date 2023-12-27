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
use std::time::Duration;

use super::ascii;
use crate::raw::adapters::kv;
use crate::raw::*;
use crate::*;
use async_trait::async_trait;
use bb8::RunError;
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio::sync::OnceCell;

/// Config for MemCached services support
#[derive(Default, Deserialize, Clone)]
#[serde(default)]
#[non_exhaustive]
pub struct MemcachedConfig {
    /// network address of the memcached service.
    ///
    /// For example: "tcp://localhost:11211"
    endpoint: Option<String>,
    /// the working directory of the service. Can be "/path/to/dir"
    ///
    /// default is "/"
    root: Option<String>,
    /// The default ttl for put operations.
    default_ttl: Option<Duration>,
}

/// [Memcached](https://memcached.org/) service support.
#[doc = include_str!("docs.md")]
#[derive(Clone, Default)]
pub struct MemcachedBuilder {
    config: MemcachedConfig,
}

impl MemcachedBuilder {
    /// set the network address of memcached service.
    ///
    /// For example: "tcp://localhost:11211"
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        if !endpoint.is_empty() {
            self.config.endpoint = Some(endpoint.to_owned());
        }
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

    /// Set the default ttl for memcached services.
    pub fn default_ttl(&mut self, ttl: Duration) -> &mut Self {
        self.config.default_ttl = Some(ttl);
        self
    }
}

impl Builder for MemcachedBuilder {
    const SCHEME: Scheme = Scheme::Memcached;
    type Accessor = MemcachedBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let config = MemcachedConfig::deserialize(ConfigDeserializer::new(map))
            .expect("config deserialize must succeed");
        MemcachedBuilder { config }
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        let endpoint = self.config.endpoint.clone().ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                .with_context("service", Scheme::Memcached)
        })?;
        let uri = http::Uri::try_from(&endpoint).map_err(|err| {
            Error::new(ErrorKind::ConfigInvalid, "endpoint is invalid")
                .with_context("service", Scheme::Memcached)
                .with_context("endpoint", &endpoint)
                .set_source(err)
        })?;

        match uri.scheme_str() {
            // If scheme is none, we will use tcp by default.
            None => (),
            Some(scheme) => {
                // We only support tcp by now.
                if scheme != "tcp" {
                    return Err(Error::new(
                        ErrorKind::ConfigInvalid,
                        "endpoint is using invalid scheme",
                    )
                    .with_context("service", Scheme::Memcached)
                    .with_context("endpoint", &endpoint)
                    .with_context("scheme", scheme.to_string()));
                }
            }
        };

        let host = if let Some(host) = uri.host() {
            host.to_string()
        } else {
            return Err(
                Error::new(ErrorKind::ConfigInvalid, "endpoint doesn't have host")
                    .with_context("service", Scheme::Memcached)
                    .with_context("endpoint", &endpoint),
            );
        };
        let port = if let Some(port) = uri.port_u16() {
            port
        } else {
            return Err(
                Error::new(ErrorKind::ConfigInvalid, "endpoint doesn't have port")
                    .with_context("service", Scheme::Memcached)
                    .with_context("endpoint", &endpoint),
            );
        };
        let endpoint = format!("{host}:{port}",);

        let root = normalize_root(
            self.config
                .root
                .clone()
                .unwrap_or_else(|| "/".to_string())
                .as_str(),
        );

        let conn = OnceCell::new();
        Ok(MemcachedBackend::new(Adapter {
            endpoint,
            conn,
            default_ttl: self.config.default_ttl,
        })
        .with_root(&root))
    }
}

/// Backend for memcached services.
pub type MemcachedBackend = kv::Backend<Adapter>;

#[derive(Clone, Debug)]
pub struct Adapter {
    endpoint: String,
    default_ttl: Option<Duration>,
    conn: OnceCell<bb8::Pool<MemcacheConnectionManager>>,
}

impl Adapter {
    async fn conn(&self) -> Result<bb8::PooledConnection<'_, MemcacheConnectionManager>> {
        let pool = self
            .conn
            .get_or_try_init(|| async {
                let mgr = MemcacheConnectionManager::new(&self.endpoint);

                bb8::Pool::builder().build(mgr).await.map_err(|err| {
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
}

#[async_trait]
impl kv::Adapter for Adapter {
    fn metadata(&self) -> kv::Metadata {
        kv::Metadata::new(
            Scheme::Memcached,
            "memcached",
            Capability {
                read: true,
                write: true,

                ..Default::default()
            },
        )
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let mut conn = self.conn().await?;

        conn.get(&percent_encode_path(key)).await
    }

    async fn set(&self, key: &str, value: &[u8]) -> Result<()> {
        let mut conn = self.conn().await?;

        conn.set(
            &percent_encode_path(key),
            value,
            // Set expiration to 0 if ttl not set.
            self.default_ttl
                .map(|v| v.as_secs() as u32)
                .unwrap_or_default(),
        )
        .await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let mut conn = self.conn().await?;

        conn.delete(&percent_encode_path(key)).await
    }
}

/// A `bb8::ManageConnection` for `memcache_async::ascii::Protocol`.
#[derive(Clone, Debug)]
struct MemcacheConnectionManager {
    address: String,
}

impl MemcacheConnectionManager {
    fn new(address: &str) -> Self {
        Self {
            address: address.to_string(),
        }
    }
}

#[async_trait]
impl bb8::ManageConnection for MemcacheConnectionManager {
    type Connection = ascii::Connection;
    type Error = Error;

    /// TODO: Implement unix stream support.
    async fn connect(&self) -> std::result::Result<Self::Connection, Self::Error> {
        let conn = TcpStream::connect(&self.address)
            .await
            .map_err(new_std_io_error)?;
        Ok(ascii::Connection::new(conn))
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> std::result::Result<(), Self::Error> {
        conn.version().await.map(|_| ())
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}
