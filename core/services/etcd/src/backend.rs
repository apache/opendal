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

use etcd_client::Certificate;
use etcd_client::ConnectOptions;
use etcd_client::Identity;
use etcd_client::TlsOptions;

use super::ETCD_SCHEME;
use super::config::EtcdConfig;
use super::core::EtcdCore;
use super::core::constants::DEFAULT_ETCD_ENDPOINTS;
use super::deleter::EtcdDeleter;
use super::lister::EtcdLazyLister;
use super::reader::*;
use super::writer::EtcdWriter;
use opendal_core::raw::*;
use opendal_core::*;

/// [Etcd](https://etcd.io/) services support.
#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct EtcdBuilder {
    pub(super) config: EtcdConfig,
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

    fn build(self) -> Result<impl Service> {
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

        let core = EtcdCore::new(endpoints, options);
        Ok(EtcdBackend::new(core, &root))
    }
}

impl EtcdBuilder {
    fn load_pem(&self, path: &str) -> Result<String> {
        std::fs::read_to_string(path)
            .map_err(|err| Error::new(ErrorKind::Unexpected, "invalid file path").set_source(err))
    }
}

#[derive(Debug, Clone)]
pub struct EtcdBackend {
    pub(crate) core: Arc<EtcdCore>,
    pub(crate) info: ServiceInfo,
    pub(crate) capability: Capability,
}

impl EtcdBackend {
    fn new(core: EtcdCore, root: &str) -> Self {
        let info = ServiceInfo::new(ETCD_SCHEME, root, "etcd");
        let capability = Capability {
            read: true,

            write: true,
            write_can_empty: true,

            delete: true,
            stat: true,
            list: true,

            shared: true,

            ..Default::default()
        };

        Self {
            core: Arc::new(core),
            info,
            capability,
        }
    }
}

impl Service for EtcdBackend {
    type Reader = oio::StreamReader<EtcdReader>;
    type Writer = EtcdWriter;
    type Lister = oio::HierarchyLister<EtcdLazyLister>;
    type Deleter = oio::OneShotDeleter<EtcdDeleter>;
    type Copier = ();

    fn info(&self) -> ServiceInfo {
        self.info.clone()
    }

    fn capability(&self) -> Capability {
        self.capability
    }

    async fn create_dir(
        &self,
        _ctx: &OperationContext,
        path: &str,
        _args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        let abs_path = build_absolute_path(&self.info.root(), path);

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

    async fn stat(&self, _ctx: &OperationContext, path: &str, _: OpStat) -> Result<RpStat> {
        let abs_path = build_absolute_path(&self.info.root(), path);

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
    fn read(&self, _ctx: &OperationContext, path: &str, op: OpRead) -> Result<Self::Reader> {
        let output: oio::StreamReader<EtcdReader> = {
            Ok(oio::StreamReader::new(EtcdReader::new(
                self.clone(),
                path,
                op,
            )))
        }?;

        Ok(output)
    }

    fn write(&self, _ctx: &OperationContext, path: &str, _op: OpWrite) -> Result<Self::Writer> {
        let output: EtcdWriter = {
            let abs_path = build_absolute_path(&self.info.root(), path);
            let writer = EtcdWriter::new(self.core.clone(), abs_path);
            Ok(writer)
        }?;

        Ok(output)
    }

    fn delete(&self, _ctx: &OperationContext) -> Result<Self::Deleter> {
        let output: oio::OneShotDeleter<EtcdDeleter> = {
            let deleter = oio::OneShotDeleter::new(EtcdDeleter::new(
                self.core.clone(),
                self.info.root().to_string(),
            ));
            Ok(deleter)
        }?;

        Ok(output)
    }

    fn list(&self, _ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        let output: oio::HierarchyLister<EtcdLazyLister> = {
            let lister = EtcdLazyLister::new(
                self.core.clone(),
                self.info.root().to_string(),
                path.to_string(),
            );
            let lister = oio::HierarchyLister::new(lister, path, args.recursive());
            Ok(lister)
        }?;

        Ok(output)
    }

    fn copy(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpCopy,
        _opts: OpCopier,
    ) -> Result<Self::Copier> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn rename(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpRename,
    ) -> Result<RpRename> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn presign(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpPresign,
    ) -> Result<RpPresign> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }
}
