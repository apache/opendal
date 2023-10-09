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
use std::fmt::Debug;

use async_trait::async_trait;
use mysql_async::prelude::*;
use mysql_async::Opts;
use mysql_async::Pool;

use crate::raw::adapters::kv;
use crate::raw::*;
use crate::*;

#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct MysqlBuilder {
    connection_string: Option<String>,

    table: Option<String>,
    key_field: Option<String>,
    value_field: Option<String>,
    root: Option<String>,
}

impl Debug for MysqlBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MysqlBuilder")
            .field("connection_string", &self.connection_string)
            .field("table", &self.table)
            .field("key_field", &self.key_field)
            .field("value_field", &self.value_field)
            .field("root", &self.root)
            .finish()
    }
}

impl MysqlBuilder {
    /// Set the connection_string of the mysql service.
    ///
    /// This connection string is used to connect to the mysql service. There are url based formats:
    ///
    /// ## Url
    ///
    /// This format resembles the url format of the mysql client. The format is: [scheme://][user[:[password]]@]host[:port][/schema][?attribute1=value1&attribute2=value2...
    ///
    /// - `mysql://user@localhost`
    /// - `mysql://user:password@localhost`
    /// - `mysql://user:password@localhost:3306`
    /// - `mysql://user:password@localhost:3306/db`
    ///
    /// For more information, please refer to [mysql client](https://dev.mysql.com/doc/refman/8.0/en/connecting-using-uri-or-key-value-pairs.html)
    ///
    pub fn connection_string(&mut self, v: &str) -> &mut Self {
        if !v.is_empty() {
            self.connection_string = Some(v.to_string());
        }
        self
    }

    /// set the working directory, all operations will be performed under it.
    ///
    /// default: "/"
    pub fn root(&mut self, root: &str) -> &mut Self {
        if !root.is_empty() {
            self.root = Some(root.to_string());
        }
        self
    }

    /// Set the table name of the mysql service to read/write.
    pub fn table(&mut self, table: &str) -> &mut Self {
        if !table.is_empty() {
            self.table = Some(table.to_string());
        }
        self
    }

    /// Set the key field name of the mysql service to read/write.
    ///
    /// Default to `key` if not specified.
    pub fn key_field(&mut self, key_field: &str) -> &mut Self {
        if !key_field.is_empty() {
            self.key_field = Some(key_field.to_string());
        }
        self
    }

    /// Set the value field name of the mysql service to read/write.
    ///
    /// Default to `value` if not specified.
    pub fn value_field(&mut self, value_field: &str) -> &mut Self {
        if !value_field.is_empty() {
            self.value_field = Some(value_field.to_string());
        }
        self
    }
}

impl Builder for MysqlBuilder {
    const SCHEME: Scheme = Scheme::Mysql;
    type Accessor = MySqlBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = MysqlBuilder::default();
        map.get("connection_string")
            .map(|v| builder.connection_string(v));
        map.get("table").map(|v| builder.table(v));
        map.get("key_field").map(|v| builder.key_field(v));
        map.get("value_field").map(|v| builder.value_field(v));
        map.get("root").map(|v| builder.root(v));
        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        let conn = match self.connection_string.clone() {
            Some(v) => v,
            None => {
                return Err(
                    Error::new(ErrorKind::ConfigInvalid, "connection_string is empty")
                        .with_context("service", Scheme::Mysql),
                )
            }
        };

        let config = Opts::from_url(&conn).map_err(|err| {
            Error::new(ErrorKind::ConfigInvalid, "connection_string is invalid")
                .with_context("service", Scheme::Mysql)
                .set_source(err)
        })?;

        let table = match self.table.clone() {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "table is empty")
                    .with_context("service", Scheme::Mysql))
            }
        };
        let key_field = match self.key_field.clone() {
            Some(v) => v,
            None => "key".to_string(),
        };
        let value_field = match self.value_field.clone() {
            Some(v) => v,
            None => "value".to_string(),
        };
        let root = normalize_root(
            self.root
                .clone()
                .unwrap_or_else(|| "/".to_string())
                .as_str(),
        );
        let pool = Pool::new(config.clone());

        Ok(MySqlBackend::new(Adapter {
            connection_pool: pool,
            config,
            table,
            key_field,
            value_field,
        })
        .with_root(&root))
    }
}

/// Backend for mysql service
pub type MySqlBackend = kv::Backend<Adapter>;

#[derive(Clone)]
pub struct Adapter {
    connection_pool: Pool,
    config: Opts,

    table: String,
    key_field: String,
    value_field: String,
}

impl Debug for Adapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Adapter")
            .field("connection_pool", &self.connection_pool)
            .field("config", &self.config)
            .field("table", &self.table)
            .field("key_field", &self.key_field)
            .field("value_field", &self.value_field)
            .finish()
    }
}

#[async_trait]
impl kv::Adapter for Adapter {
    fn metadata(&self) -> kv::Metadata {
        kv::Metadata::new(
            Scheme::Mysql,
            &self.table,
            Capability {
                read: true,
                write: true,
                ..Default::default()
            },
        )
    }

    async fn get(&self, path: &str) -> Result<Option<Vec<u8>>> {
        let query = format!(
            "SELECT `{}` FROM `{}` WHERE `{}` = :path LIMIT 1",
            self.value_field, self.table, self.key_field
        );
        let mut conn = self.connection_pool.get_conn().await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "connection failed").set_source(err)
        })?;
        let statement = conn.prep(query).await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "prepare statement failed").set_source(err)
        })?;
        let result: Option<Vec<u8>> = conn
            .exec_first(
                statement,
                params! {
                    "path" => path,
                },
            )
            .await
            .map_err(|err| Error::new(ErrorKind::Unexpected, "delete failed").set_source(err))?;
        match result {
            Some(v) => Ok(Some(v)),
            None => Ok(None),
        }
    }

    async fn set(&self, path: &str, value: &[u8]) -> Result<()> {
        let query = format!(
            "INSERT INTO `{}` (`{}`, `{}`) 
            VALUES (:path, :value) 
            ON DUPLICATE KEY UPDATE `{}` = VALUES({})",
            self.table, self.key_field, self.value_field, self.value_field, self.value_field
        );
        let mut conn = self.connection_pool.get_conn().await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "connection failed").set_source(err)
        })?;
        let statement = conn.prep(query).await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "prepare statement failed").set_source(err)
        })?;

        conn.exec_drop(
            statement,
            params! {
                "path" => path,
                "value" => value,
            },
        )
        .await
        .map_err(|err| Error::new(ErrorKind::Unexpected, "set failed").set_source(err))?;
        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let query = format!(
            "DELETE FROM `{}` WHERE `{}` = :path",
            self.table, self.key_field
        );
        let mut conn = self.connection_pool.get_conn().await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "connection failed").set_source(err)
        })?;
        let statement = conn.prep(query).await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "prepare statement failed").set_source(err)
        })?;

        conn.exec_drop(
            statement,
            params! {
                "path" => path,
            },
        )
        .await
        .map_err(|err| Error::new(ErrorKind::Unexpected, "delete failed").set_source(err))?;
        Ok(())
    }
}
