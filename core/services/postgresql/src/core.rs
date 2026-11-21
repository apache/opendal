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

use asyncband::once::OnceCell;
use sqlx::PgPool;
use sqlx::postgres::PgConnectOptions;

use opendal_core::*;

#[derive(Clone, Debug)]
pub struct PostgresqlCore {
    pool: OnceCell<PgPool>,
    config: PgConnectOptions,
    table: String,
    key_field: String,
    value_field: String,
}

impl PostgresqlCore {
    pub fn new(config: PgConnectOptions, table: &str, key_field: &str, value_field: &str) -> Self {
        Self {
            pool: OnceCell::new(),
            config,
            table: quote_identifier(table),
            key_field: quote_identifier(key_field),
            value_field: quote_identifier(value_field),
        }
    }

    async fn get_client(&self) -> Result<&PgPool> {
        self.pool
            .get_or_try_init(|| async {
                PgPool::connect_with(self.config.clone())
                    .await
                    .map_err(parse_postgres_error)
            })
            .await
    }

    pub async fn get(&self, path: &str) -> Result<Option<Buffer>> {
        let pool = self.get_client().await?;

        let table = &self.table;
        let key_field = &self.key_field;
        let value_field = &self.value_field;
        let value: Option<Vec<u8>> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT {value_field} FROM {table} WHERE {key_field} = $1 LIMIT 1",
        )))
        .bind(path)
        .fetch_optional(pool)
        .await
        .map_err(parse_postgres_error)?;

        Ok(value.map(Buffer::from))
    }

    pub async fn get_length(&self, path: &str) -> Result<Option<usize>> {
        let pool = self.get_client().await?;

        let table = &self.table;
        let key_field = &self.key_field;
        let value_field = &self.value_field;
        let value: Option<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT OCTET_LENGTH({value_field})::BIGINT FROM {table} WHERE {key_field} = $1 LIMIT 1",
        )))
        .bind(path)
        .fetch_optional(pool)
        .await
        .map_err(parse_postgres_error)?;

        value
            .map(|v| {
                v.try_into().map_err(|err| {
                    Error::new(ErrorKind::Unexpected, "postgresql value length is invalid")
                        .set_source(err)
                })
            })
            .transpose()
    }

    pub async fn set(&self, path: &str, value: Buffer) -> Result<()> {
        let pool = self.get_client().await?;

        let table = &self.table;
        let key_field = &self.key_field;
        let value_field = &self.value_field;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} AS destination ({key_field}, {value_field})
                VALUES ($1, $2)
                ON CONFLICT ({key_field})
                    DO UPDATE SET {value_field} = EXCLUDED.{value_field}",
        )))
        .bind(path)
        .bind(value.to_vec())
        .execute(pool)
        .await
        .map_err(parse_postgres_error)?;

        Ok(())
    }

    pub async fn delete(&self, path: &str) -> Result<()> {
        let pool = self.get_client().await?;

        let table = &self.table;
        let key_field = &self.key_field;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DELETE FROM {table} WHERE {key_field} = $1",
        )))
        .bind(path)
        .execute(pool)
        .await
        .map_err(parse_postgres_error)?;

        Ok(())
    }

    pub async fn copy(&self, from: &str, to: &str) -> Result<Metadata> {
        let pool = self.get_client().await?;

        let table = &self.table;
        let key_field = &self.key_field;
        let value_field = &self.value_field;
        let length: Option<i32> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} AS destination ({key_field}, {value_field})
                SELECT $2, {value_field} FROM {table} WHERE {key_field} = $1
                ON CONFLICT ({key_field})
                    DO UPDATE SET {value_field} = EXCLUDED.{value_field}
                RETURNING OCTET_LENGTH({value_field})",
        )))
        .bind(from)
        .bind(to)
        .fetch_optional(pool)
        .await
        .map_err(parse_postgres_error)?;

        let length =
            length.ok_or_else(|| Error::new(ErrorKind::NotFound, "kv not found in postgresql"))?;
        Ok(MetadataBuilder::file(length as u64).build())
    }

    pub async fn rename(&self, from: &str, to: &str) -> Result<()> {
        let pool = self.get_client().await?;
        let mut tx = pool.begin().await.map_err(parse_postgres_error)?;

        let table = &self.table;
        let key_field = &self.key_field;
        let value_field = &self.value_field;
        let (source_deleted, destination_written): (bool, bool) =
            sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "WITH moved AS (
                    DELETE FROM {table} WHERE {key_field} = $1 RETURNING {value_field}
                ), written AS (
                    INSERT INTO {table} AS destination ({key_field}, {value_field})
                        SELECT $2, {value_field} FROM moved
                        ON CONFLICT ({key_field})
                            DO UPDATE SET {value_field} = EXCLUDED.{value_field}
                        RETURNING 1
                )
                SELECT EXISTS (SELECT 1 FROM moved), EXISTS (SELECT 1 FROM written)",
            )))
            .bind(from)
            .bind(to)
            .fetch_one(&mut *tx)
            .await
            .map_err(parse_postgres_error)?;

        if !source_deleted {
            return Err(Error::new(
                ErrorKind::NotFound,
                "kv not found in postgresql",
            ));
        }
        // A trigger can skip the destination write without raising an error.
        // Commit the source deletion only after confirming the write took effect.
        if !destination_written {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "postgresql rename did not write the destination",
            ));
        }
        tx.commit().await.map_err(parse_postgres_error)
    }
}

// PostgreSQL escapes double quotes in identifiers by doubling them. Queries
// interpolate only these quoted identifiers and bind paths and values.
fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn parse_postgres_error(err: sqlx::Error) -> Error {
    Error::new(ErrorKind::Unexpected, "unhandled error from postgresql").set_source(err)
}
