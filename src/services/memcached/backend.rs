// Copyright 2023 Datafuse Labs.
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

use std::time::Duration;

use async_trait::async_trait;
use bb8::RunError;
use bb8_memcached::{bb8, MemcacheConnectionManager};
use tokio::sync::OnceCell;

use crate::raw::adapters::kv;
use crate::raw::*;
use crate::Error;
use crate::ErrorKind;
use crate::Result;
use crate::Scheme;

/// Memcached backend builder
#[derive(Clone, Default)]
pub struct Builder {
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

impl Builder {
    pub(crate) fn from_iter(it: impl Iterator<Item = (String, String)>) -> Self {
        let mut builder = Builder::default();
        for (k, v) in it {
            let v = v.as_str();
            match k.as_ref() {
                "root" => builder.root(v),
                "endpoint" => builder.endpoint(v),
                _ => continue,
            };
        }
        builder
    }

    /// set the network address of memcached service.
    ///
    /// For example: "tcp://localhost:11211"
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        if !endpoint.is_empty() {
            self.endpoint = Some(endpoint.to_owned());
        }
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

    /// Set the default ttl for memcached services.
    pub fn default_ttl(&mut self, ttl: Duration) -> &mut Self {
        self.default_ttl = Some(ttl);
        self
    }

    /// Establish connection to memcached.
    pub fn build(&mut self) -> Result<impl Accessor> {
        let endpoint = self.endpoint.clone().ok_or_else(|| {
            Error::new(ErrorKind::BackendConfigInvalid, "endpoint is empty")
                .with_context("service", Scheme::Memcached)
        })?;

        let root = normalize_root(
            self.root
                .clone()
                .unwrap_or_else(|| "/".to_string())
                .as_str(),
        );

        let conn = OnceCell::new();
        Ok(apply_wrapper(
            Backend::new(Adapter {
                endpoint,
                conn,
                default_ttl: self.default_ttl,
            })
            .with_root(&root),
        ))
    }
}

/// Backend for memcached services.
pub type Backend = kv::Backend<Adapter>;

#[derive(Clone, Debug)]
pub struct Adapter {
    endpoint: String,
    default_ttl: Option<Duration>,
    conn: OnceCell<bb8::Pool<MemcacheConnectionManager>>,
}

impl Adapter {
    async fn conn(&self) -> Result<bb8::PooledConnection<'_, MemcacheConnectionManager>> {
        let endpint = self.endpoint.as_str();

        let pool = self
            .conn
            .get_or_try_init(|| async {
                let mgr = MemcacheConnectionManager::new(endpint).map_err(|err| {
                    Error::new(
                        ErrorKind::BackendConfigInvalid,
                        "connect to memecached failed",
                    )
                    .set_source(err)
                })?;

                bb8::Pool::builder().build(mgr).await.map_err(|err| {
                    Error::new(
                        ErrorKind::BackendConfigInvalid,
                        "connect to memecached failed",
                    )
                    .set_source(err)
                })
            })
            .await?;

        pool.get().await.map_err(|err| match err {
            RunError::TimedOut => {
                Error::new(ErrorKind::Unexpected, "get connection from pool failed").set_temporary()
            }
            RunError::User(err) => parse_io_error(err),
        })
    }
}

#[async_trait]
impl kv::Adapter for Adapter {
    fn metadata(&self) -> kv::Metadata {
        kv::Metadata::new(
            Scheme::Memcached,
            "memcached",
            AccessorCapability::Read | AccessorCapability::Write,
        )
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let mut conn = self.conn().await?;
        // TODO: memcache-async have `Sized` limit on key, can we remove it?
        match conn.get(&percent_encode_path(key)).await {
            Ok(bs) => Ok(Some(bs)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(parse_io_error(err)),
        }
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
        .map_err(parse_io_error)?;

        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let mut conn = self.conn().await?;

        let _: () = conn
            .delete(&percent_encode_path(key))
            .await
            .map_err(parse_io_error)?;
        Ok(())
    }
}

fn parse_io_error(err: std::io::Error) -> Error {
    use std::io::ErrorKind::*;

    let (kind, retryable) = match err.kind() {
        NotFound => (ErrorKind::ObjectNotFound, false),
        AlreadyExists => (ErrorKind::ObjectNotFound, false),
        PermissionDenied => (ErrorKind::ObjectPermissionDenied, false),
        Interrupted | UnexpectedEof | TimedOut | WouldBlock => (ErrorKind::Unexpected, true),
        _ => (ErrorKind::Unexpected, true),
    };

    let mut err = Error::new(kind, &err.kind().to_string()).set_source(err);

    if retryable {
        err = err.set_temporary();
    }

    err
}
