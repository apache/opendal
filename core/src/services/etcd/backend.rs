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
use std::fmt::Formatter;
use std::sync::Arc;

use etcd_client::Certificate;
use etcd_client::ConnectOptions;
use etcd_client::Identity;
use etcd_client::TlsOptions;
use tokio::sync::OnceCell;

use crate::raw::*;
use crate::services::etcd::core::constants::DEFAULT_ETCD_ENDPOINTS;
use crate::services::etcd::core::EtcdCore;
use crate::services::etcd::deleter::EtcdDeleter;
use crate::services::etcd::lister::EtcdLister;
use crate::services::etcd::writer::EtcdWriter;
use crate::services::EtcdConfig;
use crate::*;

impl Configurator for EtcdConfig {
    type Builder = EtcdBuilder;
    fn into_builder(self) -> Self::Builder {
        EtcdBuilder { config: self }
    }
}

/// [Etcd](https://etcd.io/) services support.
#[doc = include_str!("docs.md")]
#[derive(Clone, Default)]
pub struct EtcdBuilder {
    config: EtcdConfig,
}

impl Debug for EtcdBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Builder");

        ds.field("config", &self.config);
        ds.finish()
    }
}

impl EtcdBuilder {
    /// set the network address of etcd service.
    ///
    /// default: "http://127.0.0.1:2379"
    pub fn endpoints(mut self, endpoints: &str) -> Self {
        if !endpoints.is_empty() {
            self.config.endpoints = Some(endpoints.to_owned());
        }
        self
    }

    /// set the username for etcd
    ///
    /// default: no username
    pub fn username(mut self, username: &str) -> Self {
        if !username.is_empty() {
            self.config.username = Some(username.to_owned());
        }
        self
    }

    /// set the password for etcd
    ///
    /// default: no password
    pub fn password(mut self, password: &str) -> Self {
        if !password.is_empty() {
            self.config.password = Some(password.to_owned());
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

    /// Set the certificate authority file path.
    ///
    /// default is None
    pub fn ca_path(mut self, ca_path: &str) -> Self {
        if !ca_path.is_empty() {
            self.config.ca_path = Some(ca_path.to_string())
        }
        self
    }

    /// Set the certificate file path.
    ///
    /// default is None
    pub fn cert_path(mut self, cert_path: &str) -> Self {
        if !cert_path.is_empty() {
            self.config.cert_path = Some(cert_path.to_string())
        }
        self
    }

    /// Set the key file path.
    ///
    /// default is None
    pub fn key_path(mut self, key_path: &str) -> Self {
        if !key_path.is_empty() {
            self.config.key_path = Some(key_path.to_string())
        }
        self
    }
}

impl Builder for EtcdBuilder {
    type Config = EtcdConfig;

    fn build(self) -> Result<impl Access> {
        let endpoints = self
            .config
            .endpoints
            .clone()
            .unwrap_or_else(|| DEFAULT_ETCD_ENDPOINTS.to_string());

        let endpoints: Vec<String> = endpoints.split(',').map(|s| s.to_string()).collect();

        let mut options = ConnectOptions::new();

        if self.config.ca_path.is_some()
            && self.config.cert_path.is_some()
            && self.config.key_path.is_some()
        {
            let ca = self.load_pem(self.config.ca_path.clone().unwrap().as_str())?;
            let key = self.load_pem(self.config.key_path.clone().unwrap().as_str())?;
            let cert = self.load_pem(self.config.cert_path.clone().unwrap().as_str())?;

            let tls_options = TlsOptions::default()
                .ca_certificate(Certificate::from_pem(ca))
                .identity(Identity::from_pem(cert, key));
            options = options.with_tls(tls_options);
        }

        if let Some(username) = self.config.username.clone() {
            options = options.with_user(
                username,
                self.config.password.clone().unwrap_or("".to_string()),
            );
        }

        let root = normalize_root(
            self.config
                .root
                .clone()
                .unwrap_or_else(|| "/".to_string())
                .as_str(),
        );

        let client = OnceCell::new();

        let core = EtcdCore {
            endpoints,
            client,
            options,
        };

        Ok(EtcdAccessor::new(core, &root))
    }
}

impl EtcdBuilder {
    fn load_pem(&self, path: &str) -> Result<String> {
        std::fs::read_to_string(path)
            .map_err(|err| Error::new(ErrorKind::Unexpected, "invalid file path").set_source(err))
    }
}

#[derive(Debug, Clone)]
pub struct EtcdAccessor {
    core: Arc<EtcdCore>,
    info: Arc<AccessorInfo>,
}

impl EtcdAccessor {
    fn new(core: EtcdCore, root: &str) -> Self {
        let info = AccessorInfo::default();
        info.set_scheme(Scheme::Etcd);
        info.set_name("etcd");
        info.set_root(root);
        info.set_native_capability(Capability {
            read: true,

            write: true,
            write_can_empty: true,

            delete: true,
            stat: true,
            list: true,

            shared: true,

            ..Default::default()
        });

        Self {
            core: Arc::new(core),
            info: Arc::new(info),
        }
    }
}

impl Access for EtcdAccessor {
    type Reader = Buffer;
    type Writer = EtcdWriter;
    type Lister = oio::HierarchyLister<EtcdLister>;
    type Deleter = oio::OneShotDeleter<EtcdDeleter>;

    fn info(&self) -> Arc<AccessorInfo> {
        self.info.clone()
    }

    async fn create_dir(&self, path: &str, _args: OpCreateDir) -> Result<RpCreateDir> {
        let abs_path = build_abs_path(&self.info.root(), path);

        // In etcd, we simulate directory creation by storing an empty value
        // with the directory path (ensuring it ends with '/')
        let dir_path = if abs_path.ends_with('/') {
            abs_path
        } else {
            format!("{abs_path}/")
        };

        // Store an empty buffer to represent the directory
        self.core.set(&dir_path, Buffer::new()).await?;

        Ok(RpCreateDir::default())
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let abs_path = build_abs_path(&self.info.root(), path);

        // First check if it's a direct key
        match self.core.get(&abs_path).await? {
            Some(buffer) => {
                let mut metadata = Metadata::new(EntryMode::from_path(&abs_path));
                metadata.set_content_length(buffer.len() as u64);
                Ok(RpStat::new(metadata))
            }
            None => {
                // Check if it's a directory by looking for keys with this prefix
                let prefix = if abs_path.ends_with('/') {
                    abs_path
                } else {
                    format!("{abs_path}/")
                };

                // Use etcd prefix query to check if any keys exist with this prefix
                let has_children = self.core.has_prefix(&prefix).await?;
                if has_children {
                    // Has children, it's a directory
                    let metadata = Metadata::new(EntryMode::DIR);
                    Ok(RpStat::new(metadata))
                } else {
                    Err(Error::new(ErrorKind::NotFound, "path not found"))
                }
            }
        }
    }

    async fn read(&self, path: &str, op: OpRead) -> Result<(RpRead, Self::Reader)> {
        let abs_path = build_abs_path(&self.info.root(), path);

        match self.core.get(&abs_path).await? {
            Some(buffer) => {
                let range = op.range();

                // If range is full, return the buffer directly
                if range.is_full() {
                    return Ok((RpRead::new(), buffer));
                }

                // Handle range requests
                let offset = range.offset() as usize;
                if offset >= buffer.len() {
                    return Err(Error::new(
                        ErrorKind::RangeNotSatisfied,
                        "range start offset exceeds content length",
                    ));
                }

                let size = range.size().map(|s| s as usize);
                let end = size.map_or(buffer.len(), |s| (offset + s).min(buffer.len()));
                let sliced_buffer = buffer.slice(offset..end);

                Ok((RpRead::new(), sliced_buffer))
            }
            None => Err(Error::new(ErrorKind::NotFound, "path not found")),
        }
    }

    async fn write(&self, path: &str, _op: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let abs_path = build_abs_path(&self.info.root(), path);
        let writer = EtcdWriter::new(self.core.clone(), abs_path);
        Ok((RpWrite::new(), writer))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        let deleter = oio::OneShotDeleter::new(EtcdDeleter::new(
            self.core.clone(),
            self.info.root().to_string(),
        ));
        Ok((RpDelete::default(), deleter))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let lister = EtcdLister::new(
            self.core.clone(),
            self.info.root().to_string(),
            path.to_string(),
        )
        .await?;
        let lister = oio::HierarchyLister::new(lister, path, args.recursive());
        Ok((RpList::default(), lister))
    }
}
