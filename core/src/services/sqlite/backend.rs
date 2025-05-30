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
use std::pin::Pin;
use std::str::FromStr;
use std::task::Context;
use std::task::Poll;

use futures::stream::BoxStream;
use futures::Stream;
use futures::StreamExt;
use ouroboros::self_referencing;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::SqlitePool;
use tokio::sync::OnceCell;

use crate::raw::adapters::kv;
use crate::raw::*;
use crate::services::SqliteConfig;
use crate::*;

impl Configurator for SqliteConfig {
    type Builder = SqliteBuilder;
    fn into_builder(self) -> Self::Builder {
        SqliteBuilder { config: self }
    }
}

#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct SqliteBuilder {
    config: SqliteConfig,
}

impl Debug for SqliteBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("SqliteBuilder");

        ds.field("config", &self.config);
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
    /// This format resembles the url format of the sqlite client:
    ///
    /// - `sqlite::memory:`
    /// - `sqlite:data.db`
    /// - `sqlite://data.db`
    ///
    /// For more information, please visit <https://docs.rs/sqlx/latest/sqlx/sqlite/struct.SqliteConnectOptions.html>.
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

    /// Set the table name of the sqlite service to read/write.
    pub fn table(mut self, table: &str) -> Self {
        if !table.is_empty() {
            self.config.table = Some(table.to_string());
        }
        self
    }

    /// Set the key field name of the sqlite service to read/write.
    ///
    /// Default to `key` if not specified.
    pub fn key_field(mut self, key_field: &str) -> Self {
        if !key_field.is_empty() {
            self.config.key_field = Some(key_field.to_string());
        }
        self
    }

    /// Set the value field name of the sqlite service to read/write.
    ///
    /// Default to `value` if not specified.
    pub fn value_field(mut self, value_field: &str) -> Self {
        if !value_field.is_empty() {
            self.config.value_field = Some(value_field.to_string());
        }
        self
    }
}

impl Builder for SqliteBuilder {
    const SCHEME: Scheme = Scheme::Sqlite;
    type Config = SqliteConfig;

    fn build(self) -> Result<impl Access> {
        let conn = match self.config.connection_string {
            Some(v) => v,
            None => {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "connection_string is required but not set",
                )
                .with_context("service", Scheme::Sqlite));
            }
        };

        let config = SqliteConnectOptions::from_str(&conn).map_err(|err| {
            Error::new(ErrorKind::ConfigInvalid, "connection_string is invalid")
                .with_context("service", Scheme::Sqlite)
                .set_source(err)
        })?;

        let table = match self.config.table {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "table is empty")
                    .with_context("service", Scheme::Sqlite));
            }
        };

        let key_field = self.config.key_field.unwrap_or_else(|| "key".to_string());

        let value_field = self
            .config
            .value_field
            .unwrap_or_else(|| "value".to_string());

        let root = normalize_root(self.config.root.as_deref().unwrap_or("/"));

        Ok(SqliteBackend::new(Adapter {
            pool: OnceCell::new(),
            config,
            table,
            key_field,
            value_field,
        })
        .with_normalized_root(root))
    }
}

pub type SqliteBackend = kv::Backend<Adapter>;

#[derive(Debug, Clone)]
pub struct Adapter {
    pool: OnceCell<SqlitePool>,
    config: SqliteConnectOptions,

    table: String,
    key_field: String,
    value_field: String,
}

impl Adapter {
    async fn get_client(&self) -> Result<&SqlitePool> {
        self.pool
            .get_or_try_init(|| async {
                let pool = SqlitePool::connect_with(self.config.clone())
                    .await
                    .map_err(parse_sqlite_error)?;
                Ok(pool)
            })
            .await
    }
}

#[self_referencing]
pub struct SqliteScanner {
    pool: SqlitePool,
    query: String,

    #[borrows(pool, query)]
    #[covariant]
    stream: BoxStream<'this, Result<String>>,
}

impl Stream for SqliteScanner {
    type Item = Result<String>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.with_stream_mut(|s| s.poll_next_unpin(cx))
    }
}

unsafe impl Sync for SqliteScanner {}

impl kv::Scan for SqliteScanner {
    async fn next(&mut self) -> Result<Option<String>> {
        <Self as StreamExt>::next(self).await.transpose()
    }
}

impl kv::Adapter for Adapter {
    type Scanner = SqliteScanner;

    fn info(&self) -> kv::Info {
        kv::Info::new(
            Scheme::Sqlite,
            &self.table,
            Capability {
                read: true,
                write: true,
                delete: true,
                list: true,
                shared: false,
                ..Default::default()
            },
        )
    }

    async fn get(&self, path: &str) -> Result<Option<Buffer>> {
        let pool = self.get_client().await?;

        let value: Option<Vec<u8>> = sqlx::query_scalar(&format!(
            "SELECT `{}` FROM `{}` WHERE `{}` = $1 LIMIT 1",
            self.value_field, self.table, self.key_field
        ))
        .bind(path)
        .fetch_optional(pool)
        .await
        .map_err(parse_sqlite_error)?;

        Ok(value.map(Buffer::from))
    }

    async fn set(&self, path: &str, value: Buffer) -> Result<()> {
        let pool = self.get_client().await?;

        sqlx::query(&format!(
            "INSERT OR REPLACE INTO `{}` (`{}`, `{}`) VALUES ($1, $2)",
            self.table, self.key_field, self.value_field,
        ))
        .bind(path)
        .bind(value.to_vec())
        .execute(pool)
        .await
        .map_err(parse_sqlite_error)?;

        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let pool = self.get_client().await?;

        sqlx::query(&format!(
            "DELETE FROM `{}` WHERE `{}` = $1",
            self.table, self.key_field
        ))
        .bind(path)
        .execute(pool)
        .await
        .map_err(parse_sqlite_error)?;

        Ok(())
    }

    async fn scan(&self, path: &str) -> Result<Self::Scanner> {
        let pool = self.get_client().await?;
        let stream = SqliteScannerBuilder {
            pool: pool.clone(),
            query: format!(
                "SELECT `{}` FROM `{}` WHERE `{}` LIKE $1",
                self.key_field, self.table, self.key_field
            ),
            stream_builder: |pool, query| {
                sqlx::query_scalar(query)
                    .bind(format!("{path}%"))
                    .fetch(pool)
                    .map(|v| v.map_err(parse_sqlite_error))
                    .boxed()
            },
        }
        .build();

        Ok(stream)
    }
}

fn parse_sqlite_error(err: sqlx::Error) -> Error {
    let is_temporary = matches!(
        &err,
        sqlx::Error::Database(db_err) if db_err.code().is_some_and(|c| c == "5" || c == "6")
    );

    let message = if is_temporary {
        "database is locked or busy"
    } else {
        "unhandled error from sqlite"
    };

    let mut error = Error::new(ErrorKind::Unexpected, message).set_source(err);
    if is_temporary {
        error = error.set_temporary();
    }
    error
}
