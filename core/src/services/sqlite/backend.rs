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
use std::fmt::Formatter;

use async_trait::async_trait;
use rusqlite::{params, Connection};
use tokio::task;

use crate::raw::adapters::kv;
use crate::raw::*;
use crate::*;

#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct SqliteBuilder {
    connection_string: Option<String>,

    table: Option<String>,
    key_field: Option<String>,
    value_field: Option<String>,
    root: Option<String>,
}

impl Debug for SqliteBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("SqliteBuilder");
        ds.field("connection_string", &self.connection_string);
        ds.field("table", &self.table);
        ds.field("key_field", &self.key_field);
        ds.field("value_field", &self.value_field);
        ds.field("root", &self.root);
        ds.finish()
    }
}

impl SqliteBuilder {
    /// Set the connection_string of the sqlite service.
    ///
    /// This connection string is used to connect to the sqlite service. There are url based formats:
    ///
    /// ## Url
    ///
    /// This format resembles the url format of the sqlite client. The format is: file://[path]?flag
    ///
    /// - `file://data.db`
    ///
    /// For more information, please refer to [Opening A New Database Connection](http://www.sqlite.org/c3ref/open.html)
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
            self.root = Some(root.to_owned());
        }
        self
    }

    /// Set the table name of the sqlite service to read/write.
    pub fn table(&mut self, table: &str) -> &mut Self {
        if !table.is_empty() {
            self.table = Some(table.to_string());
        }
        self
    }

    /// Set the key field name of the sqlite service to read/write.
    ///
    /// Default to `key` if not specified.
    pub fn key_field(&mut self, key_field: &str) -> &mut Self {
        if !key_field.is_empty() {
            self.key_field = Some(key_field.to_string());
        }
        self
    }

    /// Set the value field name of the sqlite service to read/write.
    ///
    /// Default to `value` if not specified.
    pub fn value_field(&mut self, value_field: &str) -> &mut Self {
        if !value_field.is_empty() {
            self.value_field = Some(value_field.to_string());
        }
        self
    }
}

impl Builder for SqliteBuilder {
    const SCHEME: Scheme = Scheme::Sqlite;
    type Accessor = SqliteBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = SqliteBuilder::default();
        map.get("connection_string")
            .map(|v| builder.connection_string(v));
        map.get("table").map(|v| builder.table(v));
        map.get("key_field").map(|v| builder.key_field(v));
        map.get("value_field").map(|v| builder.value_field(v));
        map.get("root").map(|v| builder.root(v));
        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        let connection_string = match self.connection_string.clone() {
            Some(v) => v,
            None => {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "connection_string is required but not set",
                )
                .with_context("service", Scheme::Sqlite))
            }
        };
        let table = match self.table.clone() {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "table is empty")
                    .with_context("service", Scheme::Postgresql))
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
        Ok(SqliteBackend::new(Adapter {
            connection_string,
            table,
            key_field,
            value_field,
        })
        .with_root(&root))
    }
}

pub type SqliteBackend = kv::Backend<Adapter>;

#[derive(Clone)]
pub struct Adapter {
    connection_string: String,

    table: String,
    key_field: String,
    value_field: String,
}

impl Debug for Adapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("SqliteAdapter");
        ds.field("connection_string", &self.connection_string);
        ds.field("table", &self.table);
        ds.field("key_field", &self.key_field);
        ds.field("value_field", &self.value_field);
        ds.finish()
    }
}

#[async_trait]
impl kv::Adapter for Adapter {
    fn metadata(&self) -> kv::Metadata {
        kv::Metadata::new(
            Scheme::Sqlite,
            &self.table,
            Capability {
                read: true,
                write: true,
                create_dir: true,
                delete: true,
                ..Default::default()
            },
        )
    }

    async fn get(&self, path: &str) -> Result<Option<Vec<u8>>> {
        let cloned_path = path.to_string();
        let cloned_self = self.clone();

        task::spawn_blocking(move || cloned_self.blocking_get(cloned_path.as_str()))
            .await
            .map_err(Error::from)
            .and_then(|inner_result| inner_result)
    }

    fn blocking_get(&self, path: &str) -> Result<Option<Vec<u8>>> {
        let query = format!(
            "SELECT {} FROM {} WHERE `{}` = $1 LIMIT 1",
            self.value_field, self.table, self.key_field
        );
        let conn = Connection::open(self.connection_string.clone()).map_err(Error::from)?;
        let mut statement = conn.prepare(&query).map_err(Error::from)?;
        let result = statement.query_row([path], |row| row.get(0));
        match result {
            Ok(v) => Ok(Some(v)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(err) => Err(Error::from(err)),
        }
    }

    async fn set(&self, path: &str, value: &[u8]) -> Result<()> {
        let cloned_path = path.to_string();
        let cloned_value = value.to_vec();
        let cloned_self = self.clone();

        task::spawn_blocking(move || cloned_self.blocking_set(cloned_path.as_str(), &cloned_value))
            .await
            .map_err(Error::from)
            .and_then(|inner_result| inner_result)
    }

    fn blocking_set(&self, path: &str, value: &[u8]) -> Result<()> {
        let query = format!(
            "INSERT OR REPLACE INTO `{}` (`{}`, `{}`) VALUES ($1, $2)",
            self.table, self.key_field, self.value_field
        );
        let conn = Connection::open(self.connection_string.clone()).map_err(Error::from)?;
        let mut statement = conn.prepare(&query).map_err(Error::from)?;
        statement
            .execute(params![path, value])
            .map_err(Error::from)?;
        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let cloned_path = path.to_string();
        let cloned_self = self.clone();

        task::spawn_blocking(move || cloned_self.blocking_delete(cloned_path.as_str()))
            .await
            .map_err(Error::from)
            .and_then(|inner_result| inner_result)
    }

    fn blocking_delete(&self, path: &str) -> Result<()> {
        let conn = Connection::open(self.connection_string.clone()).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "Sqlite open error").set_source(err)
        })?;
        let query = format!("DELETE FROM {} WHERE `{}` = $1", self.table, self.key_field);
        let mut statement = conn.prepare(&query).map_err(Error::from)?;
        statement.execute([path]).map_err(Error::from)?;
        Ok(())
    }
}

impl From<rusqlite::Error> for Error {
    fn from(value: rusqlite::Error) -> Error {
        Error::new(ErrorKind::Unexpected, "unhandled error from sqlite").set_source(value)
    }
}

impl From<task::JoinError> for Error {
    fn from(value: task::JoinError) -> Error {
        Error::new(
            ErrorKind::Unexpected,
            "unhandled error from sqlite when spawning task",
        )
        .set_source(value)
    }
}
