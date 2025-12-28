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
use sqlx::MySqlPool;
use sqlx::mysql::MySqlConnectOptions;

use opendal_core::*;

#[derive(Clone, Debug)]
pub struct MysqlCore {
    pub pool: OnceCell<MySqlPool>,
    pub config: MySqlConnectOptions,

    pub table: String,
    pub key_field: String,
    pub value_field: String,
}

impl MysqlCore {
    async fn get_client(&self) -> Result<&MySqlPool> {
        self.pool
            .get_or_try_init(|| async {
                MySqlPool::connect_with(self.config.clone())
                    .await
                    .map_err(parse_mysql_error)
            })
            .await
    }

    pub async fn get(&self, path: &str) -> Result<Option<Buffer>> {
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

    pub async fn set(&self, path: &str, value: Buffer) -> Result<()> {
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

    pub async fn delete(&self, path: &str) -> Result<()> {
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
