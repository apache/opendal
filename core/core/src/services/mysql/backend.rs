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

use mea::once::OnceCell;
use sqlx::mysql::MySqlConnectOptions;

use super::MYSQL_SCHEME;
use super::config::MysqlConfig;
use super::core::*;
use super::deleter::MysqlDeleter;
use super::writer::MysqlWriter;
use crate::raw::oio;
use crate::raw::*;
use crate::*;

#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct MysqlBuilder {
    pub(super) config: MysqlConfig,
}

impl MysqlBuilder {
    /// Set the connection_string of the mysql service.
    ///
    /// This connection string is used to connect to the mysql service. There are url based formats:
    ///
    /// ## Url
    ///
    /// This format resembles the url format of the mysql client. The format is: `[scheme://][user[:[password]]@]host[:port][/schema][?attribute1=value1&attribute2=value2...`
    ///
    /// - `mysql://user@localhost`
    /// - `mysql://user:password@localhost`
    /// - `mysql://user:password@localhost:3306`
    /// - `mysql://user:password@localhost:3306/db`
    ///
    /// For more information, please refer to <https://docs.rs/sqlx/latest/sqlx/mysql/struct.MySqlConnectOptions.html>.
    pub fn connection_string(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.connection_string = Some(v.to_string());
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

    /// Set the table name of the mysql service to read/write.
    pub fn table(mut self, table: &str) -> Self {
        if !table.is_empty() {
            self.config.table = Some(table.to_string());
        }
        self
    }

    /// Set the key field name of the mysql service to read/write.
    ///
    /// Default to `key` if not specified.
    pub fn key_field(mut self, key_field: &str) -> Self {
        if !key_field.is_empty() {
            self.config.key_field = Some(key_field.to_string());
        }
        self
    }

    /// Set the value field name of the mysql service to read/write.
    ///
    /// Default to `value` if not specified.
    pub fn value_field(mut self, value_field: &str) -> Self {
        if !value_field.is_empty() {
            self.config.value_field = Some(value_field.to_string());
        }
        self
    }
}

impl Builder for MysqlBuilder {
    type Config = MysqlConfig;

    fn build(self) -> Result<impl Access> {
        let conn = match self.config.connection_string {
            Some(v) => v,
            None => {
                return Err(
                    Error::new(ErrorKind::ConfigInvalid, "connection_string is empty")
                        .with_context("service", MYSQL_SCHEME),
                );
            }
        };

        let config = conn.parse::<MySqlConnectOptions>().map_err(|err| {
            Error::new(ErrorKind::ConfigInvalid, "connection_string is invalid")
                .with_context("service", MYSQL_SCHEME)
                .set_source(err)
        })?;

        let table = match self.config.table {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "table is empty")
                    .with_context("service", MYSQL_SCHEME));
            }
        };

        let key_field = self.config.key_field.unwrap_or_else(|| "key".to_string());

        let value_field = self
            .config
            .value_field
            .unwrap_or_else(|| "value".to_string());

        let root = normalize_root(self.config.root.unwrap_or_else(|| "/".to_string()).as_str());

        Ok(MysqlBackend::new(MysqlCore {
            pool: OnceCell::new(),
            config,
            table,
            key_field,
            value_field,
        })
        .with_normalized_root(root))
    }
}

/// Backend for mysql service
#[derive(Clone, Debug)]
pub struct MysqlBackend {
    core: Arc<MysqlCore>,
    root: String,
    info: Arc<AccessorInfo>,
}

impl MysqlBackend {
    pub fn new(core: MysqlCore) -> Self {
        let info = AccessorInfo::default();
        info.set_scheme(MYSQL_SCHEME);
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

impl Access for MysqlBackend {
    type Reader = Buffer;
    type Writer = MysqlWriter;
    type Lister = ();
    type Deleter = oio::OneShotDeleter<MysqlDeleter>;

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
                None => Err(Error::new(ErrorKind::NotFound, "kv not found in mysql")),
            }
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let p = build_abs_path(&self.root, path);
        let bs = match self.core.get(&p).await? {
            Some(bs) => bs,
            None => return Err(Error::new(ErrorKind::NotFound, "kv not found in mysql")),
        };
        Ok((RpRead::new(), bs.slice(args.range().to_range_as_usize())))
    }

    async fn write(&self, path: &str, _: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let p = build_abs_path(&self.root, path);
        Ok((RpWrite::new(), MysqlWriter::new(self.core.clone(), p)))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(MysqlDeleter::new(self.core.clone(), self.root.clone())),
        ))
    }
}
