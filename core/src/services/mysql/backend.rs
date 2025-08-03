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
use std::str::FromStr;

use sqlx::mysql::MySqlConnectOptions;
use sqlx::MySqlPool;
use tokio::sync::OnceCell;

use crate::raw::adapters::kv;
use crate::raw::*;
use crate::services::MysqlConfig;
use crate::*;

impl Configurator for MysqlConfig {
    type Builder = MysqlBuilder;
    fn into_builder(self) -> Self::Builder {
        MysqlBuilder { config: self }
    }
}

#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct MysqlBuilder {
    config: MysqlConfig,
}

impl Debug for MysqlBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("MysqlBuilder");

        d.field("config", &self.config).finish()
    }
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
                        .with_context("service", Scheme::Mysql),
                )
            }
        };

        let config = MySqlConnectOptions::from_str(&conn).map_err(|err| {
            Error::new(ErrorKind::ConfigInvalid, "connection_string is invalid")
                .with_context("service", Scheme::Mysql)
                .set_source(err)
        })?;

        let table = match self.config.table {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "table is empty")
                    .with_context("service", Scheme::Mysql))
            }
        };

        let key_field = self.config.key_field.unwrap_or_else(|| "key".to_string());

        let value_field = self
            .config
            .value_field
            .unwrap_or_else(|| "value".to_string());

        let root = normalize_root(self.config.root.unwrap_or_else(|| "/".to_string()).as_str());

        Ok(MySqlBackend::new(Adapter {
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
pub type MySqlBackend = kv::Backend<Adapter>;

#[derive(Debug, Clone)]
pub struct Adapter {
    pool: OnceCell<MySqlPool>,
    config: MySqlConnectOptions,

    table: String,
    key_field: String,
    value_field: String,
}

impl Adapter {
    async fn get_client(&self) -> Result<&MySqlPool> {
        self.pool
            .get_or_try_init(|| async {
                let pool = MySqlPool::connect_with(self.config.clone())
                    .await
                    .map_err(parse_mysql_error)?;
                Ok(pool)
            })
            .await
    }
}

impl kv::Adapter for Adapter {
    type Scanner = ();

    fn info(&self) -> kv::Info {
        kv::Info::new(
            Scheme::Mysql,
            &self.table,
            Capability {
                read: true,
                write: true,
                delete: true,
                shared: true,
                ..Default::default()
            },
        )
    }

    async fn get(&self, path: &str) -> Result<Option<Buffer>> {
        let pool = self.get_client().await?;

        let value: Option<Vec<u8>> = sqlx::query_scalar(&format!(
            "SELECT `{}` FROM `{}` WHERE `{}` = ? LIMIT 1",
            self.value_field, self.table, self.key_field
        ))
        .bind(path)
        .fetch_optional(pool)
        .await
        .map_err(parse_mysql_error)?;

        Ok(value.map(Buffer::from))
    }

    async fn set(&self, path: &str, value: Buffer) -> Result<()> {
        let pool = self.get_client().await?;

        sqlx::query(&format!(
            r#"INSERT INTO `{}` (`{}`, `{}`) VALUES (?, ?)
            ON DUPLICATE KEY UPDATE `{}` = VALUES({})"#,
            self.table, self.key_field, self.value_field, self.value_field, self.value_field
        ))
        .bind(path)
        .bind(value.to_vec())
        .execute(pool)
        .await
        .map_err(parse_mysql_error)?;

        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let pool = self.get_client().await?;

        sqlx::query(&format!(
            "DELETE FROM `{}` WHERE `{}` = ?",
            self.table, self.key_field
        ))
        .bind(path)
        .execute(pool)
        .await
        .map_err(parse_mysql_error)?;

        Ok(())
    }
}

fn parse_mysql_error(err: sqlx::Error) -> Error {
    Error::new(ErrorKind::Unexpected, "unhandled error from mysql").set_source(err)
}
