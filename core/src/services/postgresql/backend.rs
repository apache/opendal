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
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use tokio::sync::OnceCell;
use tokio_postgres::Config;

use crate::raw::adapters::kv;
use crate::raw::*;
use crate::*;

/// [Postgresql](https://www.postgresql.org/) services support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct PostgresqlBuilder {
    connection_string: Option<String>,

    table: Option<String>,
    key_field: Option<String>,
    value_field: Option<String>,
    root: Option<String>,
}

impl Debug for PostgresqlBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Builder");
        ds.field("table", &self.table);
        ds.finish()
    }
}

impl PostgresqlBuilder {
    /// Set the connection_string of the postgresql service.
    ///
    /// This connection string is `libpq-style connection strings`. There are two formats:
    ///
    /// ## Key Value
    ///
    /// This format consists of space-separated key-value pairs. Values which are either the empty
    /// string or contain whitespace should be wrapped in '. ' and \ characters should be
    /// backslash-escaped.
    ///
    /// - `host=localhost user=postgres connect_timeout=10 keepalives=0`
    /// - `host=/var/lib/postgresql,localhost port=1234 user=postgres password='password with spaces'`
    /// - `host=host1,host2,host3 port=1234,,5678 user=postgres target_session_attrs=read-write`
    ///
    /// Available keys could found at: <https://docs.rs/postgres/latest/postgres/config/struct.Config.html>
    ///
    /// ## Url
    ///
    /// This format resembles a URL with a scheme of either `postgres://` or `postgresql://`.
    ///
    /// - `postgresql://user@localhost`
    /// - `postgresql://user:password@%2Fvar%2Flib%2Fpostgresql/mydb?connect_timeout=10`
    /// - `postgresql://user@host1:1234,host2,host3:5678?target_session_attrs=read-write`
    /// - `postgresql:///mydb?user=user&host=/var/lib/postgresql`
    ///
    /// # Notes
    ///
    /// If connection_string has been specified, other parameters will be ignored.
    ///
    /// For more information, please visit <https://docs.rs/postgres/latest/postgres/config/struct.Config.html>
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

    /// Set the table name of the postgresql service to read/write.
    pub fn table(&mut self, table: &str) -> &mut Self {
        if !table.is_empty() {
            self.table = Some(table.to_string());
        }
        self
    }

    /// Set the key field name of the postgresql service to read/write.
    ///
    /// Default to `key` if not specified.
    pub fn key_field(&mut self, key_field: &str) -> &mut Self {
        if !key_field.is_empty() {
            self.key_field = Some(key_field.to_string());
        }
        self
    }

    /// Set the value field name of the postgresql service to read/write.
    ///
    /// Default to `value` if not specified.
    pub fn value_field(&mut self, value_field: &str) -> &mut Self {
        if !value_field.is_empty() {
            self.value_field = Some(value_field.to_string());
        }
        self
    }
}

impl Builder for PostgresqlBuilder {
    const SCHEME: Scheme = Scheme::Postgresql;
    type Accessor = PostgresqlBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = PostgresqlBuilder::default();

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
                        .with_context("service", Scheme::Postgresql),
                )
            }
        };

        let config = Config::from_str(&conn).map_err(|err| {
            Error::new(ErrorKind::ConfigInvalid, "connection_string is invalid")
                .with_context("service", Scheme::Postgresql)
                .set_source(err)
        })?;

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

        Ok(PostgresqlBackend::new(Adapter {
            pool: OnceCell::new(),
            config,
            table,
            key_field,
            value_field,
        })
        .with_root(&root))
    }
}

/// Backend for Postgresql service
pub type PostgresqlBackend = kv::Backend<Adapter>;

#[derive(Clone)]
pub struct Adapter {
    pool: OnceCell<Arc<Pool<PostgresConnectionManager<tokio_postgres::NoTls>>>>,
    config: Config,

    table: String,
    key_field: String,
    value_field: String,
}

impl Debug for Adapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Adapter");
        ds.finish_non_exhaustive()
    }
}

impl Adapter {
    async fn get_client(&self) -> Result<&Pool<PostgresConnectionManager<tokio_postgres::NoTls>>> {
        self.pool
            .get_or_try_init(|| async {
                // TODO: add tls support.
                let manager =
                    PostgresConnectionManager::new(self.config.clone(), tokio_postgres::NoTls);
                let pool = Pool::builder().build(manager).await?;
                Ok(Arc::new(pool))
            })
            .await
            .map(|v| v.as_ref())
    }
}

#[async_trait]
impl kv::Adapter for Adapter {
    fn metadata(&self) -> kv::Metadata {
        kv::Metadata::new(
            Scheme::Postgresql,
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
            "SELECT {} FROM {} WHERE {} = $1 LIMIT 1",
            self.value_field, self.table, self.key_field
        );
        let connection = self.get_client().await?.get().await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "unhandled error from postgresql").set_source(err)
        })?;
        let statement = connection.prepare(&query).await?;
        let rows = connection.query(&statement, &[&path]).await?;
        if rows.is_empty() {
            return Ok(None);
        }
        let value: Vec<u8> = rows[0].get(0);
        Ok(Some(value))
    }

    async fn set(&self, path: &str, value: &[u8]) -> Result<()> {
        let table = &self.table;
        let key_field = &self.key_field;
        let value_field = &self.value_field;
        let query = format!(
            "INSERT INTO {table} ({key_field}, {value_field}) \
                VALUES ($1, $2) \
                ON CONFLICT ({key_field}) \
                    DO UPDATE SET {value_field} = EXCLUDED.{value_field}",
        );
        let connection = self.get_client().await?.get().await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "unhandled error from postgresql").set_source(err)
        })?;
        let statement = connection.prepare(&query).await?;
        let _ = connection.query(&statement, &[&path, &value]).await?;
        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let query = format!("DELETE FROM {} WHERE {} = $1", self.table, self.key_field);
        let connection = self.get_client().await?.get().await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "unhandled error from postgresql").set_source(err)
        })?;
        let statement = connection.prepare(&query).await?;

        let _ = connection.query(&statement, &[&path]).await?;
        Ok(())
    }
}

impl From<tokio_postgres::Error> for Error {
    fn from(value: tokio_postgres::Error) -> Error {
        Error::new(ErrorKind::Unexpected, "unhandled error from postgresql").set_source(value)
    }
}
