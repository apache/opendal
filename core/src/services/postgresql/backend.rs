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
use std::str::FromStr;

use sqlx::postgres::PgConnectOptions;
use sqlx::PgPool;
use tokio::sync::OnceCell;

use crate::raw::adapters::kv;
use crate::raw::*;
use crate::services::PostgresqlConfig;
use crate::*;

impl Configurator for PostgresqlConfig {
    type Builder = PostgresqlBuilder;
    fn into_builder(self) -> Self::Builder {
        PostgresqlBuilder { config: self }
    }
}

/// [PostgreSQL](https://www.postgresql.org/) services support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct PostgresqlBuilder {
    config: PostgresqlConfig,
}

impl Debug for PostgresqlBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("PostgresqlBuilder");

        d.field("config", &self.config);
        d.finish()
    }
}

impl PostgresqlBuilder {
    /// Set the connection url string of the postgresql service.
    ///
    /// The URL should be with a scheme of either `postgres://` or `postgresql://`.
    ///
    /// - `postgresql://user@localhost`
    /// - `postgresql://user:password@%2Fvar%2Flib%2Fpostgresql/mydb?connect_timeout=10`
    /// - `postgresql://user@host1:1234,host2,host3:5678?target_session_attrs=read-write`
    /// - `postgresql:///mydb?user=user&host=/var/lib/postgresql`
    ///
    /// For more information, please visit <https://docs.rs/sqlx/latest/sqlx/postgres/struct.PgConnectOptions.html>.
    pub fn connection_string(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.connection_string = Some(v.to_string());
        }
        self
    }

    /// Set the working directory, all operations will be performed under it.
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

    /// Set the table name of the postgresql service to read/write.
    pub fn table(mut self, table: &str) -> Self {
        if !table.is_empty() {
            self.config.table = Some(table.to_string());
        }
        self
    }

    /// Set the key field name of the postgresql service to read/write.
    ///
    /// Default to `key` if not specified.
    pub fn key_field(mut self, key_field: &str) -> Self {
        if !key_field.is_empty() {
            self.config.key_field = Some(key_field.to_string());
        }
        self
    }

    /// Set the value field name of the postgresql service to read/write.
    ///
    /// Default to `value` if not specified.
    pub fn value_field(mut self, value_field: &str) -> Self {
        if !value_field.is_empty() {
            self.config.value_field = Some(value_field.to_string());
        }
        self
    }
}

impl Builder for PostgresqlBuilder {
    const SCHEME: Scheme = Scheme::Postgresql;
    type Config = PostgresqlConfig;

    fn build(self) -> Result<impl Access> {
        let conn = match self.config.connection_string {
            Some(v) => v,
            None => {
                return Err(
                    Error::new(ErrorKind::ConfigInvalid, "connection_string is empty")
                        .with_context("service", Scheme::Postgresql),
                )
            }
        };

        let config = PgConnectOptions::from_str(&conn).map_err(|err| {
            Error::new(ErrorKind::ConfigInvalid, "connection_string is invalid")
                .with_context("service", Scheme::Postgresql)
                .set_source(err)
        })?;

        let table = match self.config.table {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "table is empty")
                    .with_context("service", Scheme::Postgresql))
            }
        };

        let key_field = self.config.key_field.unwrap_or_else(|| "key".to_string());

        let value_field = self
            .config
            .value_field
            .unwrap_or_else(|| "value".to_string());

        let root = normalize_root(self.config.root.unwrap_or_else(|| "/".to_string()).as_str());

        Ok(PostgresqlBackend::new(Adapter {
            pool: OnceCell::new(),
            config,
            table,
            key_field,
            value_field,
        })
        .with_normalized_root(root))
    }
}

/// Backend for Postgresql service
pub type PostgresqlBackend = kv::Backend<Adapter>;

#[derive(Debug, Clone)]
pub struct Adapter {
    pool: OnceCell<PgPool>,
    config: PgConnectOptions,

    table: String,
    key_field: String,
    value_field: String,
}

impl Adapter {
    async fn get_client(&self) -> Result<&PgPool> {
        self.pool
            .get_or_try_init(|| async {
                let pool = PgPool::connect_with(self.config.clone())
                    .await
                    .map_err(parse_postgres_error)?;
                Ok(pool)
            })
            .await
    }
}

impl kv::Adapter for Adapter {
    type Scanner = ();

    fn info(&self) -> kv::Info {
        kv::Info::new(
            Scheme::Postgresql,
            &self.table,
            Capability {
                read: true,
                write: true,
                shared: true,
                ..Default::default()
            },
        )
    }

    async fn get(&self, path: &str) -> Result<Option<Buffer>> {
        let pool = self.get_client().await?;

        let value: Option<Vec<u8>> = sqlx::query_scalar(&format!(
            r#"SELECT "{}" FROM "{}" WHERE "{}" = $1 LIMIT 1"#,
            self.value_field, self.table, self.key_field
        ))
        .bind(path)
        .fetch_optional(pool)
        .await
        .map_err(parse_postgres_error)?;

        Ok(value.map(Buffer::from))
    }

    async fn set(&self, path: &str, value: Buffer) -> Result<()> {
        let pool = self.get_client().await?;

        let table = &self.table;
        let key_field = &self.key_field;
        let value_field = &self.value_field;
        sqlx::query(&format!(
            r#"INSERT INTO "{table}" ("{key_field}", "{value_field}")
                VALUES ($1, $2)
                ON CONFLICT ("{key_field}")
                    DO UPDATE SET "{value_field}" = EXCLUDED."{value_field}""#,
        ))
        .bind(path)
        .bind(value.to_vec())
        .execute(pool)
        .await
        .map_err(parse_postgres_error)?;

        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let pool = self.get_client().await?;

        sqlx::query(&format!(
            "DELETE FROM {} WHERE {} = $1",
            self.table, self.key_field
        ))
        .bind(path)
        .execute(pool)
        .await
        .map_err(parse_postgres_error)?;

        Ok(())
    }
}

fn parse_postgres_error(err: sqlx::Error) -> Error {
    Error::new(ErrorKind::Unexpected, "unhandled error from postgresql").set_source(err)
}
