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

use asyncband::once::OnceCell;
use sqlx::SqlitePool;
use sqlx::sqlite::SqliteConnectOptions;

use super::backend::parse_sqlite_error;
use opendal_core::*;

#[derive(Debug, Clone)]
pub struct SqliteCore {
    pub pool: OnceCell<SqlitePool>,
    pub config: SqliteConnectOptions,

    pub table: String,
    pub key_field: String,
    pub value_field: String,
}

impl SqliteCore {
    pub async fn get_client(&self) -> Result<&SqlitePool> {
        self.pool
            .get_or_try_init(|| async {
                SqlitePool::connect_with(self.config.clone())
                    .await
                    .map_err(parse_sqlite_error)
            })
            .await
    }

    pub async fn get(&self, path: &str) -> Result<Option<Buffer>> {
        let pool = self.get_client().await?;

        // SQLite prefers using standard `"` for identifier quotes instead of
        // MySQL-compatible backtick ` or [] quotes.
        // Identifier quotes are case-sensitive and allow special characters.
        // Read more https://www.sqlite.org/quirks.html#double_quoted_string_literals_are_accepted
        //
        // We use:
        // - formatted, quoted identifiers for trusted table and field configuration
        // - bind parameters for values to avoid malformed SQL and SQL injection
        // to ensure correctness. `AssertSqlSafe` records this audit for sqlx.
        let value: Option<Vec<u8>> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            r#"SELECT "{}" FROM "{}" WHERE "{}" = $1 LIMIT 1"#,
            self.value_field, self.table, self.key_field
        )))
        .bind(path)
        .fetch_optional(pool)
        .await
        .map_err(parse_sqlite_error)?;

        Ok(value.map(Buffer::from))
    }

    pub async fn get_length(&self, path: &str) -> Result<Option<u64>> {
        let pool = self.get_client().await?;

        let value: Option<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            r#"SELECT LENGTH(CAST("{}" AS BLOB)) FROM "{}" WHERE "{}" = $1 LIMIT 1"#,
            self.value_field, self.table, self.key_field
        )))
        .bind(path)
        .fetch_optional(pool)
        .await
        .map_err(parse_sqlite_error)?;

        value
            .map(|v| {
                u64::try_from(v).map_err(|err| {
                    Error::new(ErrorKind::Unexpected, "sqlite value length is invalid")
                        .set_source(err)
                })
            })
            .transpose()
    }

    pub async fn count_under(&self, path: &str) -> Result<i64> {
        let pool = self.get_client().await?;

        sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            r#"SELECT COUNT(*) FROM "{}" WHERE instr("{}", $1) = 1 LIMIT 1"#,
            self.table, self.key_field
        )))
        .bind(path.to_string())
        .fetch_one(pool)
        .await
        .map_err(parse_sqlite_error)
    }

    pub async fn get_range(
        &self,
        path: &str,
        offset: u64,
        limit: Option<u64>,
    ) -> Result<Option<(Buffer, u64)>> {
        // SQLite BLOB/TEXT values are at most `i32::MAX` bytes and `SUBSTR`
        // converts its start and length arguments to signed 32-bit integers.
        // An offset >= `i32::MAX` is therefore always at or past EOF.
        let Some(start) = offset.checked_add(1).and_then(|v| i32::try_from(v).ok()) else {
            return Ok(self
                .get_length(path)
                .await?
                .map(|length| (Buffer::new(), length)));
        };

        let pool = self.get_client().await?;
        // Any requested length above `i32::MAX` exceeds SQLite's maximum value
        // size, so omit the length argument to read through EOF without 32-bit
        // truncation in `SUBSTR`.
        let value = match limit.and_then(|v| i32::try_from(v).ok()) {
            Some(limit) => {
                sqlx::query_as(sqlx::AssertSqlSafe(format!(
                    r#"SELECT SUBSTR(CAST("{}" AS BLOB), $1, $2), LENGTH(CAST("{}" AS BLOB)) FROM "{}" WHERE "{}" = $3 LIMIT 1"#,
                    self.value_field, self.value_field, self.table, self.key_field
                )))
                .bind(start)
                .bind(limit)
                .bind(path)
                .fetch_optional(pool)
                .await
            }
            None => {
                sqlx::query_as(sqlx::AssertSqlSafe(format!(
                    r#"SELECT SUBSTR(CAST("{}" AS BLOB), $1), LENGTH(CAST("{}" AS BLOB)) FROM "{}" WHERE "{}" = $2 LIMIT 1"#,
                    self.value_field, self.value_field, self.table, self.key_field
                )))
                .bind(start)
                .bind(path)
                .fetch_optional(pool)
                .await
            }
        };
        let value: Option<(Vec<u8>, i64)> = value.map_err(parse_sqlite_error)?;

        value
            .map(|(bs, size)| {
                let size = u64::try_from(size).map_err(|err| {
                    Error::new(ErrorKind::Unexpected, "sqlite value length is invalid")
                        .set_source(err)
                })?;
                Ok((Buffer::from(bs), size))
            })
            .transpose()
    }

    pub async fn get_suffix(&self, path: &str, size: u64) -> Result<Option<(Buffer, u64)>> {
        if size == 0 {
            return Ok(self
                .get_length(path)
                .await?
                .map(|length| (Buffer::new(), length)));
        }
        // SQLite BLOB/TEXT values are at most `i32::MAX` bytes, so any suffix
        // of `i32::MAX` or more bytes returns the full value.
        if size >= i32::MAX as u64 {
            return Ok(self.get(path).await?.map(|bs| {
                let length = bs.len() as u64;
                (bs, length)
            }));
        }

        let pool = self.get_client().await?;
        let size = size as i64;
        // Compute a 1-based start index in `1..=i32::MAX` using 64-bit SQL
        // arithmetic so `SUBSTR` never receives a negative or out-of-range
        // 32-bit index.
        let value: Option<(Vec<u8>, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            r#"SELECT SUBSTR(CAST("{}" AS BLOB), MAX(1, LENGTH(CAST("{}" AS BLOB)) - $1 + 1)), LENGTH(CAST("{}" AS BLOB)) FROM "{}" WHERE "{}" = $2 LIMIT 1"#,
            self.value_field, self.value_field, self.value_field, self.table, self.key_field
        )))
        .bind(size)
        .bind(path)
        .fetch_optional(pool)
        .await
        .map_err(parse_sqlite_error)?;

        value
            .map(|(bs, total_size)| {
                let total_size = u64::try_from(total_size).map_err(|err| {
                    Error::new(ErrorKind::Unexpected, "sqlite value length is invalid")
                        .set_source(err)
                })?;
                Ok((Buffer::from(bs), total_size))
            })
            .transpose()
    }

    pub async fn set(&self, path: &str, value: Buffer) -> Result<()> {
        let pool = self.get_client().await?;

        // `INSERT OR REPLACE` deletes the conflicting row, which resets
        // unrelated columns and can delete a different row when another unique
        // constraint conflicts. Update only the value column on a key conflict.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            r#"INSERT INTO "{}" ("{}", "{}") VALUES ($1, $2)
            ON CONFLICT ("{}") DO UPDATE SET "{}" = excluded."{}""#,
            self.table,
            self.key_field,
            self.value_field,
            self.key_field,
            self.value_field,
            self.value_field,
        )))
        .bind(path)
        .bind(value.to_vec())
        .execute(pool)
        .await
        .map_err(parse_sqlite_error)?;

        Ok(())
    }

    pub async fn rename(&self, from: &str, to: &str, if_not_exists: bool) -> Result<()> {
        let pool = self.get_client().await?;
        // Serialize competing writers before checking the destination.
        let mut tx = pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(parse_sqlite_error)?;
        let table = &self.table;
        let key = &self.key_field;

        let (source_exists, destination_exists): (bool, bool) =
            sqlx::query_as(sqlx::AssertSqlSafe(format!(
                r#"SELECT EXISTS (SELECT 1 FROM "{table}" WHERE "{key}" = $1),
                          EXISTS (SELECT 1 FROM "{table}" WHERE "{key}" = $2)"#,
            )))
            .bind(from)
            .bind(to)
            .fetch_one(&mut *tx)
            .await
            .map_err(parse_sqlite_error)?;

        if !source_exists {
            return Err(Error::new(ErrorKind::NotFound, "key not found in sqlite"));
        }
        if destination_exists {
            if if_not_exists {
                return Err(Error::new(
                    ErrorKind::ConditionNotMatch,
                    "destination key already exists in sqlite",
                ));
            }

            // The key collation can make two different paths match the source row.
            sqlx::query(sqlx::AssertSqlSafe(format!(
                r#"DELETE FROM "{table}" WHERE "{key}" = $2 AND "{key}" <> $1"#,
            )))
            .bind(from)
            .bind(to)
            .execute(&mut *tx)
            .await
            .map_err(parse_sqlite_error)?;
        }

        // ABORT preserves rows that conflict through other constraints, even if
        // the schema configures a different default conflict policy.
        let result = sqlx::query(sqlx::AssertSqlSafe(format!(
            r#"UPDATE OR ABORT "{table}" SET "{key}" = $2 WHERE "{key}" = $1"#,
        )))
        .bind(from)
        .bind(to)
        .execute(&mut *tx)
        .await
        .map_err(|err| {
            if err
                .as_database_error()
                .is_some_and(|err| err.is_unique_violation())
            {
                Error::new(
                    ErrorKind::Conflict,
                    "sqlite rename violates a unique constraint",
                )
                .set_source(err)
            } else {
                parse_sqlite_error(err)
            }
        })?;
        if result.rows_affected() == 0 {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "sqlite rename did not update the source key",
            ));
        }

        tx.commit().await.map_err(parse_sqlite_error)
    }

    pub async fn delete(&self, path: &str) -> Result<()> {
        let pool = self.get_client().await?;

        sqlx::query(sqlx::AssertSqlSafe(format!(
            r#"DELETE FROM "{}" WHERE "{}" = $1"#,
            self.table, self.key_field
        )))
        .bind(path)
        .execute(pool)
        .await
        .map_err(parse_sqlite_error)?;

        Ok(())
    }
}
