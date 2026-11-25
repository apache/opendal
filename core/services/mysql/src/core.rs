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
use sqlx::MySqlPool;
use sqlx::mysql::MySqlConnectOptions;

use opendal_core::*;

#[derive(Clone, Debug)]
pub struct MysqlCore {
    pool: OnceCell<MySqlPool>,
    config: MySqlConnectOptions,

    table: String,
    key_field: String,
    value_field: String,
}

impl MysqlCore {
    pub fn new(
        config: MySqlConnectOptions,
        table: &str,
        key_field: &str,
        value_field: &str,
    ) -> Self {
        Self {
            pool: OnceCell::new(),
            config,
            table: quote_identifier(table),
            key_field: quote_identifier(key_field),
            value_field: quote_identifier(value_field),
        }
    }

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

        let table = &self.table;
        let key_field = &self.key_field;
        let value_field = &self.value_field;
        let value: Option<Vec<u8>> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT {value_field} FROM {table} WHERE {key_field} = ? LIMIT 1",
        )))
        .bind(path)
        .fetch_optional(pool)
        .await
        .map_err(parse_mysql_error)?;

        Ok(value.map(Buffer::from))
    }

    pub async fn get_length(&self, path: &str) -> Result<Option<usize>> {
        let pool = self.get_client().await?;

        let table = &self.table;
        let key_field = &self.key_field;
        let value_field = &self.value_field;
        let value: Option<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT OCTET_LENGTH({value_field}) FROM {table} WHERE {key_field} = ? LIMIT 1",
        )))
        .bind(path)
        .fetch_optional(pool)
        .await
        .map_err(parse_mysql_error)?;

        value
            .map(|v| {
                v.try_into().map_err(|err| {
                    Error::new(ErrorKind::Unexpected, "mysql value length is invalid")
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
            "INSERT INTO {table} ({key_field}, {value_field}) VALUES (?, ?)
            ON DUPLICATE KEY UPDATE {value_field} = VALUES({value_field})",
        )))
        .bind(path)
        .bind(value.to_vec())
        .execute(pool)
        .await
        .map_err(parse_mysql_error)?;

        Ok(())
    }

    pub async fn delete(&self, path: &str) -> Result<()> {
        let pool = self.get_client().await?;

        let table = &self.table;
        let key_field = &self.key_field;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DELETE FROM {table} WHERE {key_field} = ?",
        )))
        .bind(path)
        .execute(pool)
        .await
        .map_err(parse_mysql_error)?;

        Ok(())
    }

    pub async fn list(&self, path: &str) -> Result<Vec<String>> {
        let pool = self.get_client().await?;

        let table = &self.table;
        let key_field = &self.key_field;
        // Keep the raw LIKE predicate so MySQL can use a B-tree range scan on
        // `key_field`, then enforce exact UTF-8 codepoint prefix matching via
        // `utf8mb4_bin` regardless of the column's collation. Use a non-backslash
        // escape character so escaping works identically under `NO_BACKSLASH_ESCAPES`.
        let pattern = format!("{}%", escape_like(path));
        sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT {key_field} FROM {table} \
             WHERE {key_field} LIKE ? ESCAPE '!' \
               AND CONVERT({key_field} USING utf8mb4) COLLATE utf8mb4_bin \
                   LIKE CONVERT(? USING utf8mb4) COLLATE utf8mb4_bin ESCAPE '!' \
             ORDER BY {key_field}",
        )))
        .bind(&pattern)
        .bind(&pattern)
        .fetch_all(pool)
        .await
        .map_err(parse_mysql_error)
    }
}

// MySQL escapes backticks in identifiers by doubling them. Queries
// interpolate only these quoted identifiers and bind paths and values.
fn quote_identifier(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

fn escape_like(s: &str) -> String {
    const ESCAPE_CHAR: char = '!';
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '!' | '%' | '_' => {
                out.push(ESCAPE_CHAR);
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out
}

fn parse_mysql_error(err: sqlx::Error) -> Error {
    Error::new(ErrorKind::Unexpected, "unhandled error from mysql").set_source(err)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quote_identifier() {
        assert_eq!(quote_identifier("data"), "`data`");
        assert_eq!(quote_identifier("my`table"), "`my``table`");
        assert_eq!(quote_identifier("k` OR 1=1 -- "), "`k`` OR 1=1 -- `");
        assert_eq!(quote_identifier("a``b"), "`a````b`");
    }

    #[test]
    fn test_escape_like_basic() {
        assert_eq!(escape_like("abc"), "abc");
        assert_eq!(escape_like("foo"), "foo");
    }

    #[test]
    fn test_escape_like_wildcards() {
        assert_eq!(escape_like("%"), "!%");
        assert_eq!(escape_like("_"), "!_");
        assert_eq!(escape_like("a%b_c"), "a!%b!_c");
    }

    #[test]
    fn test_escape_like_escape_char() {
        assert_eq!(escape_like("!"), "!!");
        assert_eq!(escape_like("!%"), "!!!%");
        assert_eq!(escape_like(r"\"), r"\");
    }

    #[test]
    fn test_escape_like_mixed() {
        let input = r"foo\%bar_baz!%";
        let expected = r"foo\!%bar!_baz!!!%";

        assert_eq!(escape_like(input), expected);
    }
}
