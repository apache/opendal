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

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::OnceCell;

use super::config::MemcachedConfig;
use super::core::*;
use super::deleter::MemcachedDeleter;
use super::writer::MemcachedWriter;
use crate::raw::*;
use crate::*;

/// [Memcached](https://memcached.org/) service support.
#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct MemcachedBuilder {
    pub(super) config: MemcachedConfig,
}

impl MemcachedBuilder {
    /// set the network address of memcached service.
    ///
    /// For example: "tcp://localhost:11211"
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        if !endpoint.is_empty() {
            self.config.endpoint = Some(endpoint.to_owned());
        }
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

    /// set the username.
    pub fn username(mut self, username: &str) -> Self {
        self.config.username = Some(username.to_string());
        self
    }

    /// set the password.
    pub fn password(mut self, password: &str) -> Self {
        self.config.password = Some(password.to_string());
        self
    }

    /// Set the default ttl for memcached services.
    pub fn default_ttl(mut self, ttl: Duration) -> Self {
        self.config.default_ttl = Some(ttl);
        self
    }
}

impl Builder for MemcachedBuilder {
    type Config = MemcachedConfig;

    fn build(self) -> Result<impl Access> {
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
        Ok(MemcachedBackend::new(MemcachedCore {
            conn,
            endpoint,
            username: self.config.username.clone(),
            password: self.config.password.clone(),
            default_ttl: self.config.default_ttl,
        })
        .with_normalized_root(root))
    }
}

/// Backend for memcached services.
#[derive(Clone, Debug)]
pub struct MemcachedBackend {
    core: Arc<MemcachedCore>,
    root: String,
    info: Arc<AccessorInfo>,
}

impl MemcachedBackend {
    pub fn new(core: MemcachedCore) -> Self {
        let info = AccessorInfo::default();
        info.set_scheme(Scheme::Memcached.into_static());
        info.set_name("memcached");
        info.set_root("/");
        info.set_native_capability(Capability {
            read: true,
            stat: true,
            write: true,
            write_can_empty: true,
            delete: true,
            shared: true,
            ..Default::default()
        });

        Self {
            core: Arc::new(core),
            root: "/".to_string(),
            info: Arc::new(info),
        }
    }

    fn with_normalized_root(mut self, root: String) -> Self {
        self.info.set_root(&root);
        self.root = root;
        self
    }
}

impl Access for MemcachedBackend {
    type Reader = Buffer;
    type Writer = MemcachedWriter;
    type Lister = ();
    type Deleter = oio::OneShotDeleter<MemcachedDeleter>;

    fn info(&self) -> Arc<AccessorInfo> {
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
                None => Err(Error::new(ErrorKind::NotFound, "kv not found in memcached")),
            }
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let p = build_abs_path(&self.root, path);
        let bs = match self.core.get(&p).await? {
            Some(bs) => bs,
            None => return Err(Error::new(ErrorKind::NotFound, "kv not found in memcached")),
        };
        Ok((RpRead::new(), bs.slice(args.range().to_range_as_usize())))
    }

    async fn write(&self, path: &str, _: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let p = build_abs_path(&self.root, path);
        Ok((RpWrite::new(), MemcachedWriter::new(self.core.clone(), p)))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(MemcachedDeleter::new(self.core.clone(), self.root.clone())),
        ))
    }

    async fn list(&self, path: &str, _: OpList) -> Result<(RpList, Self::Lister)> {
        let _ = build_abs_path(&self.root, path);
        Ok((RpList::default(), ()))
    }
}
