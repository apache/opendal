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

use mea::once::OnceCell;
use sqlx::SqlitePool;
use sqlx::sqlite::SqliteConnectOptions;
use std::fmt::Debug;

use crate::services::sqlite::backend::parse_sqlite_error;
use crate::*;

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

    pub async fn get_range(
        &self,
        path: &str,
        start: isize,
        limit: isize,
    ) -> Result<Option<Buffer>> {
        let pool = self.get_client().await?;
        let value: Option<Vec<u8>> = sqlx::query_scalar(&format!(
            "SELECT SUBSTR(`{}`, {}, {}) FROM `{}` WHERE `{}` = $1 LIMIT 1",
            self.value_field,
            start + 1,
            limit,
            self.table,
            self.key_field
        ))
        .bind(path)
        .fetch_optional(pool)
        .await
        .map_err(parse_sqlite_error)?;

        Ok(value.map(Buffer::from))
    }

    pub async fn set(&self, path: &str, value: Buffer) -> Result<()> {
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

    pub async fn delete(&self, path: &str) -> Result<()> {
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
}
