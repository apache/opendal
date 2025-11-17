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
use std::sync::Arc;

use tokio::sync::OnceCell;

use super::SURREALDB_SCHEME;
use super::config::SurrealdbConfig;
use super::core::*;
use super::deleter::SurrealdbDeleter;
use super::writer::SurrealdbWriter;
use crate::raw::*;
use crate::*;

#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct SurrealdbBuilder {
    pub(super) config: SurrealdbConfig,
}

impl SurrealdbBuilder {
    /// Set the connection_string of the surrealdb service.
    ///
    /// This connection string is used to connect to the surrealdb service. There are url based formats:
    ///
    /// ## Url
    ///
    /// - `ws://ip:port`
    /// - `wss://ip:port`
    /// - `http://ip:port`
    /// - `https://ip:port`
    pub fn connection_string(mut self, connection_string: &str) -> Self {
        if !connection_string.is_empty() {
            self.config.connection_string = Some(connection_string.to_string());
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

    /// Set the table name of the surrealdb service for read/write.
    pub fn table(mut self, table: &str) -> Self {
        if !table.is_empty() {
            self.config.table = Some(table.to_string());
        }
        self
    }

    /// Set the username of the surrealdb service for signin.
    pub fn username(mut self, username: &str) -> Self {
        if !username.is_empty() {
            self.config.username = Some(username.to_string());
        }
        self
    }

    /// Set the password of the surrealdb service for signin.
    pub fn password(mut self, password: &str) -> Self {
        if !password.is_empty() {
            self.config.password = Some(password.to_string());
        }
        self
    }

    /// Set the namespace of the surrealdb service for read/write.
    pub fn namespace(mut self, namespace: &str) -> Self {
        if !namespace.is_empty() {
            self.config.namespace = Some(namespace.to_string());
        }
        self
    }

    /// Set the database of the surrealdb service for read/write.
    pub fn database(mut self, database: &str) -> Self {
        if !database.is_empty() {
            self.config.database = Some(database.to_string());
        }
        self
    }

    /// Set the key field name of the surrealdb service for read/write.
    ///
    /// Default to `key` if not specified.
    pub fn key_field(mut self, key_field: &str) -> Self {
        if !key_field.is_empty() {
            self.config.key_field = Some(key_field.to_string());
        }
        self
    }

    /// Set the value field name of the surrealdb service for read/write.
    ///
    /// Default to `value` if not specified.
    pub fn value_field(mut self, value_field: &str) -> Self {
        if !value_field.is_empty() {
            self.config.value_field = Some(value_field.to_string());
        }
        self
    }
}

impl Builder for SurrealdbBuilder {
    type Config = SurrealdbConfig;

    fn build(self) -> Result<impl Access> {
        let connection_string = match self.config.connection_string.clone() {
            Some(v) => v,
            None => {
                return Err(
                    Error::new(ErrorKind::ConfigInvalid, "connection_string is empty")
                        .with_context("service", SURREALDB_SCHEME),
                );
            }
        };

        let namespace = match self.config.namespace.clone() {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "namespace is empty")
                    .with_context("service", SURREALDB_SCHEME));
            }
        };
        let database = match self.config.database.clone() {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "database is empty")
                    .with_context("service", SURREALDB_SCHEME));
            }
        };
        let table = match self.config.table.clone() {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "table is empty")
                    .with_context("service", SURREALDB_SCHEME));
            }
        };

        let username = self.config.username.clone().unwrap_or_default();
        let password = self.config.password.clone().unwrap_or_default();
        let key_field = self
            .config
            .key_field
            .clone()
            .unwrap_or_else(|| "key".to_string());
        let value_field = self
            .config
            .value_field
            .clone()
            .unwrap_or_else(|| "value".to_string());
        let root = normalize_root(
            self.config
                .root
                .clone()
                .unwrap_or_else(|| "/".to_string())
                .as_str(),
        );

        Ok(SurrealdbBackend::new(SurrealdbCore {
            db: OnceCell::new(),
            connection_string,
            username,
            password,
            namespace,
            database,
            table,
            key_field,
            value_field,
        })
        .with_normalized_root(root))
    }
}

/// Backend for Surrealdb service
#[derive(Clone, Debug)]
pub struct SurrealdbBackend {
    core: Arc<SurrealdbCore>,
    root: String,
    info: Arc<AccessorInfo>,
}

impl SurrealdbBackend {
    pub fn new(core: SurrealdbCore) -> Self {
        let info = AccessorInfo::default();
        info.set_scheme(SURREALDB_SCHEME);
        info.set_name(&core.table);
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

impl Access for SurrealdbBackend {
    type Reader = Buffer;
    type Writer = SurrealdbWriter;
    type Lister = ();
    type Deleter = oio::OneShotDeleter<SurrealdbDeleter>;

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
                None => Err(Error::new(ErrorKind::NotFound, "kv not found in surrealdb")),
            }
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let p = build_abs_path(&self.root, path);
        let bs = match self.core.get(&p).await? {
            Some(bs) => bs,
            None => {
                return Err(Error::new(ErrorKind::NotFound, "kv not found in surrealdb"));
            }
        };
        Ok((RpRead::new(), bs.slice(args.range().to_range_as_usize())))
    }

    async fn write(&self, path: &str, _: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let p = build_abs_path(&self.root, path);
        Ok((RpWrite::new(), SurrealdbWriter::new(self.core.clone(), p)))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(SurrealdbDeleter::new(self.core.clone(), self.root.clone())),
        ))
    }
}
