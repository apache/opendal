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

use crate::raw::oio;
use crate::raw::*;
use crate::services::SqliteConfig;
use crate::services::sqlite::core::SqliteCore;
use crate::services::sqlite::delete::SqliteDeleter;
use crate::services::sqlite::writer::SqliteWriter;
use crate::*;
use sqlx::sqlite::SqliteConnectOptions;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::str::FromStr;
use tokio::sync::OnceCell;

#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct SqliteBuilder {
    pub(super) config: SqliteConfig,
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

        Ok(SqliteAccessor::new(SqliteCore {
            pool: OnceCell::new(),
            config,
            table,
            key_field,
            value_field,
        })
        .with_normalized_root(root))
    }
}

pub fn parse_sqlite_error(err: sqlx::Error) -> Error {
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

/// SqliteAccessor implements Access trait directly
#[derive(Debug, Clone)]
pub struct SqliteAccessor {
    core: std::sync::Arc<SqliteCore>,
    root: String,
    info: std::sync::Arc<AccessorInfo>,
}

impl SqliteAccessor {
    fn new(core: SqliteCore) -> Self {
        let info = AccessorInfo::default();
        info.set_scheme(Scheme::Sqlite.into());
        info.set_name(&core.table);
        info.set_root("/");
        info.set_native_capability(Capability {
            read: true,
            write: true,
            create_dir: true,
            delete: true,
            stat: true,
            write_can_empty: true,
            list: false,
            shared: false,
            ..Default::default()
        });

        Self {
            core: std::sync::Arc::new(core),
            root: "/".to_string(),
            info: std::sync::Arc::new(info),
        }
    }

    fn with_normalized_root(mut self, root: String) -> Self {
        self.info.set_root(&root);
        self.root = root;
        self
    }
}

impl Access for SqliteAccessor {
    type Reader = Buffer;
    type Writer = SqliteWriter;
    type Lister = ();
    type Deleter = oio::OneShotDeleter<SqliteDeleter>;

    fn info(&self) -> std::sync::Arc<AccessorInfo> {
        self.info.clone()
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_abs_path(&self.root, path);

        if p == build_abs_path(&self.root, "") {
            Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
        } else {
            let bs = self.core.get(&p).await?;
            match bs {
                Some(bs) => Ok(RpStat::new(
                    Metadata::new(EntryMode::from_path(&p)).with_content_length(bs.len() as u64),
                )),
                None => {
                    // Check if this might be a directory by looking for keys with this prefix
                    let dir_path = if p.ends_with('/') {
                        p.clone()
                    } else {
                        format!("{}/", p)
                    };
                    let count: i64 = sqlx::query_scalar(&format!(
                        "SELECT COUNT(*) FROM `{}` WHERE `{}` LIKE $1 LIMIT 1",
                        self.core.table, self.core.key_field
                    ))
                    .bind(format!("{}%", dir_path))
                    .fetch_one(self.core.get_client().await?)
                    .await
                    .map_err(crate::services::sqlite::backend::parse_sqlite_error)?;

                    if count > 0 {
                        // Directory exists (has children)
                        Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
                    } else {
                        Err(Error::new(ErrorKind::NotFound, "key not found in sqlite"))
                    }
                }
            }
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let p = build_abs_path(&self.root, path);

        let range = args.range();
        let buffer = if range.is_full() {
            // Full read - use GET
            match self.core.get(&p).await? {
                Some(bs) => bs,
                None => return Err(Error::new(ErrorKind::NotFound, "key not found in sqlite")),
            }
        } else {
            // Range read - use GETRANGE
            let start = range.offset() as isize;
            let limit = match range.size() {
                Some(size) => size as isize,
                None => -1, // Sqlite uses -1 for end of string
            };

            match self.core.get_range(&p, start, limit).await? {
                Some(bs) => bs,
                None => return Err(Error::new(ErrorKind::NotFound, "key not found in sqlite")),
            }
        };

        Ok((RpRead::new(), buffer))
    }

    async fn write(&self, path: &str, _: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let p = build_abs_path(&self.root, path);
        Ok((RpWrite::new(), SqliteWriter::new(self.core.clone(), &p)))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(SqliteDeleter::new(self.core.clone(), self.root.clone())),
        ))
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        let p = build_abs_path(&self.root, path);

        // Ensure path ends with '/' for directory marker
        let dir_path = if p.ends_with('/') {
            p
        } else {
            format!("{}/", p)
        };

        // Store directory marker with empty content
        self.core.set(&dir_path, Buffer::new()).await?;

        Ok(RpCreateDir::default())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use sqlx::SqlitePool;

    async fn build_client() -> OnceCell<SqlitePool> {
        let config = SqliteConnectOptions::from_str("sqlite::memory:").unwrap();
        let pool = SqlitePool::connect_with(config).await.unwrap();
        OnceCell::new_with(Some(pool))
    }

    #[tokio::test]
    async fn test_sqlite_accessor_creation() {
        let core = SqliteCore {
            pool: build_client().await,
            config: Default::default(),
            table: "test".to_string(),
            key_field: "key".to_string(),
            value_field: "value".to_string(),
        };

        let accessor = SqliteAccessor::new(core);

        // Verify basic properties
        assert_eq!(accessor.root, "/");
        assert_eq!(
            accessor.info.scheme(),
            <Scheme as Into<&'static str>>::into(Scheme::Sqlite)
        );
        assert!(accessor.info.native_capability().read);
        assert!(accessor.info.native_capability().write);
        assert!(accessor.info.native_capability().delete);
        assert!(accessor.info.native_capability().stat);
    }

    #[tokio::test]
    async fn test_sqlite_accessor_with_root() {
        let core = SqliteCore {
            pool: build_client().await,
            config: Default::default(),
            table: "test".to_string(),
            key_field: "key".to_string(),
            value_field: "value".to_string(),
        };

        let accessor = SqliteAccessor::new(core).with_normalized_root("/test/".to_string());

        assert_eq!(accessor.root, "/test/");
        assert_eq!(accessor.info.root(), "/test/".into());
    }
}
