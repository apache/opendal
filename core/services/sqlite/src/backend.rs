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

use std::str::FromStr;
use std::sync::Arc;

use sqlx::sqlite::SqliteConnectOptions;

use super::SQLITE_SCHEME;
use super::config::SqliteConfig;
use super::core::SqliteCore;
use super::deleter::SqliteDeleter;
use super::reader::*;
use super::writer::SqliteWriter;
use opendal_core::raw::oio;
use opendal_core::raw::*;
use opendal_core::*;

#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct SqliteBuilder {
    pub(super) config: SqliteConfig,
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
    /// The field must have a `PRIMARY KEY` or `UNIQUE` constraint so writes can
    /// update an existing key. Default to `key` if not specified.
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

fn validate_identifier(name: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        return Err(
            Error::new(ErrorKind::ConfigInvalid, format!("{name} is empty"))
                .with_context("service", SQLITE_SCHEME),
        );
    }
    if value.contains('\0') {
        return Err(Error::new(
            ErrorKind::ConfigInvalid,
            format!("{name} must not contain NUL bytes"),
        )
        .with_context("service", SQLITE_SCHEME));
    }
    Ok(())
}

impl Builder for SqliteBuilder {
    type Config = SqliteConfig;

    fn build(self) -> Result<impl Service> {
        let conn = match self.config.connection_string {
            Some(v) if !v.trim().is_empty() => v,
            _ => {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "connection_string is required but not set",
                )
                .with_context("service", SQLITE_SCHEME));
            }
        };

        let config = SqliteConnectOptions::from_str(&conn).map_err(|err| {
            Error::new(ErrorKind::ConfigInvalid, "connection_string is invalid")
                .with_context("service", SQLITE_SCHEME)
                .set_source(err)
        })?;

        let table = match self.config.table {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "table is empty")
                    .with_context("service", SQLITE_SCHEME));
            }
        };
        validate_identifier("table", &table)?;

        let key_field = self.config.key_field.unwrap_or_else(|| "key".to_string());
        validate_identifier("key_field", &key_field)?;

        let value_field = self
            .config
            .value_field
            .unwrap_or_else(|| "value".to_string());
        validate_identifier("value_field", &value_field)?;

        let root = normalize_root(self.config.root.as_deref().unwrap_or("/"));

        let core = SqliteCore::new(config, &table, &key_field, &value_field);
        Ok(SqliteBackend::new(core, &table).with_normalized_root(root))
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

/// SqliteBackend implements [`Service`] for SQLite-backed object storage.
#[derive(Debug, Clone)]
pub struct SqliteBackend {
    pub(crate) core: Arc<SqliteCore>,
    pub(crate) root: String,
    pub(crate) info: ServiceInfo,
    pub(crate) capability: Capability,
}

impl SqliteBackend {
    fn new(core: SqliteCore, table: &str) -> Self {
        let info = ServiceInfo::new(SQLITE_SCHEME, "/", table);
        let capability = Capability {
            read: true,
            write: true,
            create_dir: true,
            delete: true,
            stat: true,
            write_can_empty: true,
            list: false,
            rename: true,
            rename_with_if_not_exists: true,
            ..Default::default()
        };

        Self {
            core: Arc::new(core),
            root: "/".to_string(),
            info,
            capability,
        }
    }

    fn with_normalized_root(mut self, root: String) -> Self {
        self.info = self.info.with_root(&root);
        self.root = root;
        self
    }
}

impl Service for SqliteBackend {
    type Reader = oio::StreamReader<SqliteReader>;
    type Writer = SqliteWriter;
    type Lister = ();
    type Deleter = oio::OneShotDeleter<SqliteDeleter>;
    type Copier = ();
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        self.info.clone()
    }

    fn capability(&self) -> Capability {
        self.capability
    }

    async fn stat(&self, _ctx: &OperationContext, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_absolute_path(&self.root, path);

        if p == build_absolute_path(&self.root, "") {
            Ok(RpStat::new(MetadataBuilder::dir().build()))
        } else {
            let length = self.core.get_length(&p).await?;
            match length {
                Some(length) => {
                    let metadata = if p.ends_with('/') {
                        MetadataBuilder::dir()
                    } else {
                        MetadataBuilder::file(length)
                    };
                    Ok(RpStat::new(metadata.build()))
                }
                None => {
                    // Check if this might be a directory by looking for keys with this prefix
                    let dir_path = if p.ends_with('/') {
                        p.clone()
                    } else {
                        format!("{}/", p)
                    };
                    let count = self.core.count_under(&dir_path).await?;

                    if count > 0 {
                        // Directory exists (has children)
                        Ok(RpStat::new(MetadataBuilder::dir().build()))
                    } else {
                        Err(Error::new(ErrorKind::NotFound, "key not found in sqlite"))
                    }
                }
            }
        }
    }
    fn read(&self, _ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let output: oio::StreamReader<SqliteReader> = {
            Ok(oio::StreamReader::new(SqliteReader::new(
                self.clone(),
                path,
                args,
            )))
        }?;

        Ok(output)
    }

    fn write(&self, _ctx: &OperationContext, path: &str, _: OpWrite) -> Result<Self::Writer> {
        let output: SqliteWriter = {
            let p = build_absolute_path(&self.root, path);
            Ok(SqliteWriter::new(self.core.clone(), &p))
        }?;

        Ok(output)
    }

    fn delete(&self, _ctx: &OperationContext) -> Result<Self::Deleter> {
        let output: oio::OneShotDeleter<SqliteDeleter> = {
            Ok(oio::OneShotDeleter::new(SqliteDeleter::new(
                self.core.clone(),
                self.root.clone(),
            )))
        }?;

        Ok(output)
    }

    async fn create_dir(
        &self,
        _ctx: &OperationContext,
        path: &str,
        _: OpCreateDir,
    ) -> Result<RpCreateDir> {
        let p = build_absolute_path(&self.root, path);

        // Ensure non-root directory keys end with '/', while keeping the root
        // key (`""` when `root = "/"`) aligned with `stat` and `delete`.
        let dir_path = if p.is_empty() || p.ends_with('/') {
            p
        } else {
            format!("{p}/")
        };

        // Store directory marker with empty content
        self.core.set(&dir_path, Buffer::new()).await?;

        Ok(RpCreateDir::default())
    }

    fn list(&self, _ctx: &OperationContext, _path: &str, _args: OpList) -> Result<Self::Lister> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    fn copy(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpCopy,
    ) -> Result<Self::Copier> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn rename(
        &self,
        _ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        let from = build_absolute_path(&self.root, from);
        let to = build_absolute_path(&self.root, to);
        self.core.rename(&from, &to, args.if_not_exists()).await?;
        Ok(RpRename::new())
    }

    async fn presign(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpPresign,
    ) -> Result<RpPresign> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use opendal_core::raw::oio::Read as _;
    use opendal_core::raw::oio::ReadStream as _;
    use opendal_core::raw::oio::Write as _;
    use sqlx::SqlitePool;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn build_pool() -> SqlitePool {
        let config = SqliteConnectOptions::from_str("sqlite::memory:").unwrap();
        SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(config)
            .await
            .unwrap()
    }

    async fn build_backend() -> SqliteBackend {
        let core = SqliteCore::with_pool(build_pool().await, "test_table", "key", "value");

        SqliteBackend::new(core, "test_table")
    }

    #[tokio::test]
    async fn test_sqlite_backend_creation() {
        let backend = build_backend().await;

        // Verify basic properties
        assert_eq!(backend.root, "/");
        assert_eq!(backend.info.scheme(), SQLITE_SCHEME);
        assert!(backend.capability().read);
        assert!(backend.capability().write);
        assert!(backend.capability().delete);
        assert!(backend.capability().stat);
    }

    #[tokio::test]
    async fn test_sqlite_backend_with_root() {
        let backend = build_backend()
            .await
            .with_normalized_root("/test/".to_string());

        assert_eq!(backend.root, "/test/");
        assert_eq!(backend.info.root(), Arc::from("/test/"));
    }

    #[tokio::test]
    async fn test_sqlite_rename_missing_source_preserves_destination() {
        let backend = build_backend().await;
        let pool = backend.core.get_client().await.unwrap();
        sqlx::query("CREATE TABLE test_table (key TEXT PRIMARY KEY, value BLOB)")
            .execute(pool)
            .await
            .unwrap();

        let op = Operator::from_parts(OperationContext::new(), Arc::new(backend));
        op.write("target", "target value").await.unwrap();

        for if_not_exists in [false, true] {
            let err = op
                .rename_with("missing", "target")
                .if_not_exists(if_not_exists)
                .await
                .unwrap_err();
            assert_eq!(err.kind(), ErrorKind::NotFound);
            assert_eq!(op.read("target").await.unwrap().to_vec(), b"target value");
        }
    }

    #[tokio::test]
    async fn test_sqlite_rename_preserves_rows_on_unrelated_unique_conflict() {
        let backend = build_backend().await;
        let pool = backend.core.get_client().await.unwrap();
        sqlx::query("CREATE TABLE test_table (key TEXT PRIMARY KEY, value BLOB)")
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("CREATE UNIQUE INDEX prefix_value ON test_table (substr(key, 1, 6), value)")
            .execute(pool)
            .await
            .unwrap();

        let op = Operator::from_parts(OperationContext::new(), Arc::new(backend));
        op.write("source", "source value").await.unwrap();
        op.write("target-neighbor", "source value").await.unwrap();

        let err = op
            .rename_with("source", "target")
            .if_not_exists(true)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Conflict);
        assert!(!op.exists("target").await.unwrap());

        op.write("target", "target value").await.unwrap();
        let err = op.rename("source", "target").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Conflict);
        assert_eq!(op.read("source").await.unwrap().to_vec(), b"source value");
        assert_eq!(op.read("target").await.unwrap().to_vec(), b"target value");
        assert_eq!(
            op.read("target-neighbor").await.unwrap().to_vec(),
            b"source value"
        );
    }

    #[tokio::test]
    async fn test_sqlite_read_range_from_offset_reads_to_eof() {
        let backend = build_backend().await;

        let pool = backend.core.get_client().await.unwrap();
        sqlx::query("CREATE TABLE test_table (key TEXT PRIMARY KEY, value BLOB)")
            .execute(pool)
            .await
            .unwrap();

        let ctx = OperationContext::new();
        let mut writer = backend.write(&ctx, "hello", OpWrite::default()).unwrap();
        writer.write(Buffer::from("hello world")).await.unwrap();
        writer.close().await.unwrap();

        let reader = backend.read(&ctx, "hello", OpRead::default()).unwrap();
        let (_, mut stream) = reader.open(BytesRange::from(6_u64..)).await.unwrap();
        let buffer = stream.read_all().await.unwrap();

        assert_eq!(buffer.to_vec(), b"world");
    }

    #[tokio::test]
    async fn test_sqlite_stat_uses_value_length() {
        let backend = build_backend().await;

        let pool = backend.core.get_client().await.unwrap();
        sqlx::query("CREATE TABLE test_table (key TEXT PRIMARY KEY, value BLOB)")
            .execute(pool)
            .await
            .unwrap();

        let ctx = OperationContext::new();
        let mut writer = backend.write(&ctx, "key_id", OpWrite::default()).unwrap();
        writer.write(Buffer::from("hello world")).await.unwrap();
        writer.close().await.unwrap();

        let rp = backend
            .stat(&ctx, "key_id", OpStat::default())
            .await
            .unwrap();

        assert_eq!(rp.into_metadata().content_length(), 11);
    }

    #[tokio::test]
    async fn test_sqlite_stat_returns_byte_length_for_text_value() {
        let backend = build_backend().await;

        let pool = backend.core.get_client().await.unwrap();
        sqlx::query("CREATE TABLE test_table (key TEXT PRIMARY KEY, value BLOB)")
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO test_table (key, value) VALUES ($1, $2)")
            .bind("key_id")
            .bind("你好")
            .execute(pool)
            .await
            .unwrap();

        let ctx = OperationContext::new();

        let rp = backend
            .stat(&ctx, "key_id", OpStat::default())
            .await
            .unwrap();
        assert_eq!(rp.into_metadata().content_length(), 6);

        let reader = backend.read(&ctx, "key_id", OpRead::default()).unwrap();
        let (rp, mut stream) = reader.open(BytesRange::from(0_u64..3)).await.unwrap();
        let buffer = stream.read_all().await.unwrap();

        assert_eq!(rp.into_metadata().unwrap().content_length(), 6);
        assert_eq!(buffer.to_vec(), "你".as_bytes());
    }

    #[tokio::test]
    async fn test_sqlite_stat_returns_byte_length_for_text_column() {
        let backend = build_backend().await;
        let pool = backend.core.get_client().await.unwrap();

        sqlx::query("CREATE TABLE test_table (key TEXT PRIMARY KEY, value TEXT)")
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO test_table (key, value) VALUES ($1, $2)")
            .bind("key_id")
            .bind("你好")
            .execute(pool)
            .await
            .unwrap();

        let ctx = OperationContext::new();

        let rp = backend
            .stat(&ctx, "key_id", OpStat::default())
            .await
            .unwrap();
        assert_eq!(rp.into_metadata().content_length(), 6);

        let reader = backend.read(&ctx, "key_id", OpRead::default()).unwrap();
        let (rp, mut stream) = reader.open(BytesRange::from(0_u64..3)).await.unwrap();
        let buffer = stream.read_all().await.unwrap();

        assert_eq!(rp.into_metadata().unwrap().content_length(), 6);
        assert_eq!(buffer.to_vec(), "你".as_bytes());
    }

    #[tokio::test]
    async fn test_sqlite_set_inserts_and_overwrites_existing_key() {
        let backend = build_backend().await;
        let pool = backend.core.get_client().await.unwrap();
        sqlx::query("CREATE TABLE test_table (key TEXT PRIMARY KEY, value BLOB)")
            .execute(pool)
            .await
            .unwrap();

        backend
            .core
            .set("key", Buffer::from("first"))
            .await
            .unwrap();
        assert_eq!(
            backend.core.get("key").await.unwrap().unwrap().to_vec(),
            b"first"
        );

        backend
            .core
            .set("key", Buffer::from("second"))
            .await
            .unwrap();
        assert_eq!(
            backend.core.get("key").await.unwrap().unwrap().to_vec(),
            b"second"
        );

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM test_table")
            .fetch_one(pool)
            .await
            .unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_sqlite_set_preserves_existing_row_on_unrelated_unique_conflict() {
        let backend = build_backend().await;
        let pool = backend.core.get_client().await.unwrap();
        sqlx::query("CREATE TABLE test_table (key TEXT PRIMARY KEY, value BLOB UNIQUE)")
            .execute(pool)
            .await
            .unwrap();

        backend
            .core
            .set("first", Buffer::from("shared"))
            .await
            .unwrap();

        let result = backend.core.set("second", Buffer::from("shared")).await;
        assert!(result.is_err());

        assert_eq!(
            backend.core.get("first").await.unwrap().unwrap().to_vec(),
            b"shared"
        );
        assert!(backend.core.get("second").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_sqlite_set_preserves_unmodified_columns() {
        let backend = build_backend().await;
        let pool = backend.core.get_client().await.unwrap();
        sqlx::query(
            "CREATE TABLE test_table (key TEXT PRIMARY KEY, value BLOB, extra TEXT DEFAULT 'keep')",
        )
        .execute(pool)
        .await
        .unwrap();

        backend
            .core
            .set("key", Buffer::from("first"))
            .await
            .unwrap();
        sqlx::query("UPDATE test_table SET extra = 'custom' WHERE key = 'key'")
            .execute(pool)
            .await
            .unwrap();

        backend
            .core
            .set("key", Buffer::from("second"))
            .await
            .unwrap();

        let (value, extra): (Vec<u8>, String) =
            sqlx::query_as("SELECT value, extra FROM test_table WHERE key = 'key'")
                .fetch_one(pool)
                .await
                .unwrap();
        assert_eq!(value, b"second");
        assert_eq!(extra, "custom");
    }

    #[tokio::test]
    async fn test_sqlite_create_dir_and_delete_root() {
        for root in ["/", "/sub/"] {
            let backend = build_backend().await.with_normalized_root(root.to_string());
            let pool = backend.core.get_client().await.unwrap();
            sqlx::query("CREATE TABLE test_table (key TEXT PRIMARY KEY, value BLOB)")
                .execute(pool)
                .await
                .unwrap();

            let op = Operator::from_parts(OperationContext::new(), Arc::new(backend.clone()));

            op.create_dir("/").await.unwrap();
            let meta = op.stat("/").await.unwrap();
            assert_eq!(meta.mode(), EntryMode::DIR, "root {root}");

            op.delete("/").await.unwrap();
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM test_table")
                .fetch_one(pool)
                .await
                .unwrap();
            assert_eq!(count, 0, "root marker should be deleted for root {root}");

            op.create_dir("nested/").await.unwrap();
            assert_eq!(
                op.stat("nested/").await.unwrap().mode(),
                EntryMode::DIR,
                "root {root}"
            );
            op.delete("nested/").await.unwrap();
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM test_table")
                .fetch_one(pool)
                .await
                .unwrap();
            assert_eq!(
                count, 0,
                "nested dir marker should be deleted for root {root}"
            );
        }
    }

    #[tokio::test]
    async fn test_sqlite_read_range_cases() {
        let backend = build_backend().await;
        let pool = backend.core.get_client().await.unwrap();
        sqlx::query("CREATE TABLE test_table (key TEXT PRIMARY KEY, value BLOB)")
            .execute(pool)
            .await
            .unwrap();

        backend
            .core
            .set("data", Buffer::from("0123456789"))
            .await
            .unwrap();
        backend.core.set("empty", Buffer::new()).await.unwrap();
        sqlx::query("INSERT INTO test_table (key, value) VALUES ($1, $2)")
            .bind("utf8")
            .bind("你好")
            .execute(pool)
            .await
            .unwrap();

        let ctx = OperationContext::new();
        let reader = backend.read(&ctx, "data", OpRead::default()).unwrap();

        // Full read.
        let (rp, mut stream) = reader.open(BytesRange::default()).await.unwrap();
        assert_eq!(rp.into_metadata().unwrap().content_length(), 10);
        assert_eq!(stream.read_all().await.unwrap().to_vec(), b"0123456789");

        // Offset + size within bounds.
        let (rp, mut stream) = reader.open(BytesRange::from(2_u64..7)).await.unwrap();
        assert_eq!(rp.into_metadata().unwrap().content_length(), 10);
        assert_eq!(stream.read_all().await.unwrap().to_vec(), b"23456");

        // Offset >= length with non-zero bounded size -> RangeNotSatisfied.
        for range in [
            BytesRange::from(10_u64..15),
            BytesRange::from(15_u64..20),
            BytesRange::new(i32::MAX as u64, Some(5)),
            BytesRange::new(u64::MAX, Some(5)),
        ] {
            let err = reader
                .open(range)
                .await
                .err()
                .expect("expected RangeNotSatisfied");
            assert_eq!(err.kind(), ErrorKind::RangeNotSatisfied, "range {range:?}");
        }

        // Offset >= length with unbounded size -> empty buffer with total length.
        for range in [
            BytesRange::from(10_u64..),
            BytesRange::from(i32::MAX as u64..),
            BytesRange::from(u64::MAX..),
        ] {
            let (rp, mut stream) = reader.open(range).await.unwrap();
            assert_eq!(rp.into_metadata().unwrap().content_length(), 10);
            assert!(stream.read_all().await.unwrap().is_empty());
        }

        // Offset + size > length (clamped), including size > i32::MAX and u64 overflow.
        for range in [
            BytesRange::from(6_u64..100),
            BytesRange::new(6, Some(i32::MAX as u64 + 1)),
            BytesRange::new(6, Some(u64::MAX)),
        ] {
            let (rp, mut stream) = reader.open(range).await.unwrap();
            assert_eq!(rp.into_metadata().unwrap().content_length(), 10);
            assert_eq!(
                stream.read_all().await.unwrap().to_vec(),
                b"6789",
                "range {range:?}"
            );
        }

        // Zero-size ranges.
        for range in [
            BytesRange::new(0, Some(0)),
            BytesRange::new(100, Some(0)),
            BytesRange::suffix(0),
        ] {
            let (rp, mut stream) = reader.open(range).await.unwrap();
            assert_eq!(rp.into_metadata().unwrap().content_length(), 10);
            assert!(stream.read_all().await.unwrap().is_empty());
        }

        // Suffix ranges within bounds and larger than length.
        let (rp, mut stream) = reader.open(BytesRange::suffix(4)).await.unwrap();
        assert_eq!(rp.into_metadata().unwrap().content_length(), 10);
        assert_eq!(stream.read_all().await.unwrap().to_vec(), b"6789");

        for size in [10, 100, i32::MAX as u64, u64::MAX] {
            let (rp, mut stream) = reader.open(BytesRange::suffix(size)).await.unwrap();
            assert_eq!(rp.into_metadata().unwrap().content_length(), 10);
            assert_eq!(
                stream.read_all().await.unwrap().to_vec(),
                b"0123456789",
                "suffix {size}"
            );
        }

        // Empty blob ranges.
        let empty_reader = backend.read(&ctx, "empty", OpRead::default()).unwrap();
        let (rp, mut stream) = empty_reader.open(BytesRange::default()).await.unwrap();
        assert_eq!(rp.into_metadata().unwrap().content_length(), 0);
        assert!(stream.read_all().await.unwrap().is_empty());

        let (rp, mut stream) = empty_reader.open(BytesRange::suffix(4)).await.unwrap();
        assert_eq!(rp.into_metadata().unwrap().content_length(), 0);
        assert!(stream.read_all().await.unwrap().is_empty());

        let err = empty_reader
            .open(BytesRange::from(0_u64..4))
            .await
            .err()
            .expect("expected RangeNotSatisfied");
        assert_eq!(err.kind(), ErrorKind::RangeNotSatisfied);

        // Multi-byte UTF-8 text value with suffix range.
        let utf8_reader = backend.read(&ctx, "utf8", OpRead::default()).unwrap();
        let (rp, mut stream) = utf8_reader.open(BytesRange::suffix(3)).await.unwrap();
        assert_eq!(rp.into_metadata().unwrap().content_length(), 6);
        assert_eq!(stream.read_all().await.unwrap().to_vec(), "好".as_bytes());

        // Missing key returns NotFound across all range kinds.
        let missing_reader = backend.read(&ctx, "missing", OpRead::default()).unwrap();
        for range in [
            BytesRange::default(),
            BytesRange::from(0_u64..4),
            BytesRange::new(0, Some(0)),
            BytesRange::new(u64::MAX, Some(4)),
            BytesRange::suffix(4),
            BytesRange::suffix(u64::MAX),
        ] {
            let err = missing_reader
                .open(range)
                .await
                .err()
                .expect("expected NotFound");
            assert_eq!(err.kind(), ErrorKind::NotFound, "range {range:?}");
        }
    }

    #[tokio::test]
    async fn test_sqlite_read_range_atomic_length_under_concurrent_writes() {
        use sqlx::sqlite::SqliteJournalMode;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::time::{Duration, SystemTime, UNIX_EPOCH};

        struct TempDbGuard(std::path::PathBuf);
        impl Drop for TempDbGuard {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(&self.0);
                let _ = std::fs::remove_file(format!("{}-wal", self.0.display()));
                let _ = std::fs::remove_file(format!("{}-shm", self.0.display()));
            }
        }

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let db_path = std::env::temp_dir().join(format!(
            "opendal-sqlite-range-race-{}-{unique}.db",
            std::process::id()
        ));
        let _guard = TempDbGuard(db_path.clone());

        let options = SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5));

        let writer_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options.clone())
            .await
            .unwrap();
        sqlx::query("CREATE TABLE test_table (key TEXT PRIMARY KEY, value BLOB)")
            .execute(&writer_pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO test_table (key, value) VALUES ('target', $1)")
            .bind(b"0123456789".as_slice())
            .execute(&writer_pool)
            .await
            .unwrap();

        let reader_pool = SqlitePoolOptions::new()
            .max_connections(2)
            .connect_with(options)
            .await
            .unwrap();
        let backend = SqliteBackend::new(
            SqliteCore::with_pool(reader_pool, "test_table", "key", "value"),
            "test_table",
        );

        let stop = Arc::new(AtomicBool::new(false));
        let writer_stop = stop.clone();
        let writer_task = tokio::spawn(async move {
            let mut toggle = false;
            while !writer_stop.load(Ordering::Relaxed) {
                let value: &[u8] = if toggle { b"0123456789" } else { b"xy" };
                toggle = !toggle;
                sqlx::query("UPDATE test_table SET value = $1 WHERE key = 'target'")
                    .bind(value)
                    .execute(&writer_pool)
                    .await
                    .unwrap();
                tokio::task::yield_now().await;
            }
        });

        let ctx = OperationContext::new();
        let reader = backend.read(&ctx, "target", OpRead::default()).unwrap();
        for _ in 0..500 {
            let (rp, mut stream) = reader.open(BytesRange::from(0_u64..6)).await.unwrap();
            let content_length = rp.into_metadata().unwrap().content_length();
            let buf = stream.read_all().await.unwrap().to_vec();
            match content_length {
                10 => assert_eq!(
                    buf, b"012345",
                    "stale content_length=10 returned mismatched slice {buf:?}"
                ),
                2 => assert_eq!(
                    buf, b"xy",
                    "stale content_length=2 returned mismatched slice {buf:?}"
                ),
                other => panic!("unexpected content_length {other}"),
            }

            match reader.open(BytesRange::from(4_u64..8)).await {
                Ok((rp, mut stream)) => {
                    assert_eq!(rp.into_metadata().unwrap().content_length(), 10);
                    assert_eq!(stream.read_all().await.unwrap().to_vec(), b"4567");
                }
                Err(err) => assert_eq!(err.kind(), ErrorKind::RangeNotSatisfied),
            }

            let (rp, mut stream) = reader.open(BytesRange::suffix(4)).await.unwrap();
            let content_length = rp.into_metadata().unwrap().content_length();
            let buf = stream.read_all().await.unwrap().to_vec();
            match content_length {
                10 => assert_eq!(buf, b"6789"),
                2 => assert_eq!(buf, b"xy"),
                other => panic!("unexpected content_length {other}"),
            }
        }

        stop.store(true, Ordering::Relaxed);
        writer_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_sqlite_delete_non_empty_dir_keeps_entries() {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            for root in ["/", "/sub/"] {
                for explicit in [false, true] {
                    let backend = build_backend().await.with_normalized_root(root.to_string());
                    let pool = backend.core.get_client().await.unwrap();
                    sqlx::query("CREATE TABLE test_table (key TEXT PRIMARY KEY, value BLOB)")
                        .execute(pool)
                        .await
                        .unwrap();

                    let op =
                        Operator::from_parts(OperationContext::new(), Arc::new(backend.clone()));
                    if explicit {
                        op.create_dir("d/").await.unwrap();
                    }
                    op.write("d/a.txt", "a").await.unwrap();
                    op.write("d/b.txt", "b").await.unwrap();
                    op.write("d", "same-name-file").await.unwrap();
                    op.write("dd/sibling.txt", "sibling").await.unwrap();

                    let err = op
                        .delete("d/")
                        .await
                        .expect_err("non-empty directory delete must fail");
                    assert_eq!(err.kind(), ErrorKind::Unexpected);
                    assert_eq!(err.message(), "directory not empty");

                    assert!(op.exists("d/").await.unwrap());
                    assert_eq!(op.read("d/a.txt").await.unwrap().to_vec(), b"a");
                    assert_eq!(op.read("d/b.txt").await.unwrap().to_vec(), b"b");
                    assert_eq!(op.read("d").await.unwrap().to_vec(), b"same-name-file");
                    assert_eq!(
                        op.read("dd/sibling.txt").await.unwrap().to_vec(),
                        b"sibling"
                    );

                    if explicit {
                        let expected_key = build_absolute_path(root, "d/");
                        let marker_exists: bool = sqlx::query_scalar(
                            "SELECT EXISTS (SELECT 1 FROM test_table WHERE key = $1)",
                        )
                        .bind(&expected_key)
                        .fetch_one(pool)
                        .await
                        .unwrap();
                        assert!(marker_exists, "explicit dir marker must remain intact");
                    }

                    // Deleting a plain file with the same name deletes only the file
                    // row and keeps directory entries intact.
                    op.delete("d").await.unwrap();
                    assert_eq!(op.read("d").await.unwrap_err().kind(), ErrorKind::NotFound);
                    assert_eq!(op.stat("d").await.unwrap().mode(), EntryMode::DIR);
                    assert!(op.exists("d/a.txt").await.unwrap());

                    // A nested empty subdirectory also keeps the parent non-empty.
                    op.create_dir("d/nested/").await.unwrap();
                    op.delete("d/a.txt").await.unwrap();
                    op.delete("d/b.txt").await.unwrap();

                    let err = op
                        .delete("d/")
                        .await
                        .expect_err("directory with nested subdir must fail");
                    assert_eq!(err.kind(), ErrorKind::Unexpected);
                    assert_eq!(err.message(), "directory not empty");

                    op.delete("d/nested/").await.unwrap();
                    assert!(!op.exists("d/nested/").await.unwrap());

                    // Now empty directory delete succeeds and keeps sibling intact.
                    op.delete("d/").await.unwrap();
                    assert!(!op.exists("d/").await.unwrap());
                    assert!(op.exists("dd/sibling.txt").await.unwrap());

                    // Deleting a missing directory is an idempotent no-op.
                    op.delete("d/").await.unwrap();
                }
            }
        })
        .await
        .expect("test timed out");
    }

    #[tokio::test]
    async fn test_sqlite_delete_non_empty_root_keeps_entries() {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            for root in ["/", "/sub/"] {
                let backend = build_backend().await.with_normalized_root(root.to_string());
                let pool = backend.core.get_client().await.unwrap();
                sqlx::query("CREATE TABLE test_table (key TEXT PRIMARY KEY, value BLOB)")
                    .execute(pool)
                    .await
                    .unwrap();

                if root != "/" {
                    sqlx::query(
                        "INSERT INTO test_table (key, value) VALUES ('other/file.txt', $1)",
                    )
                    .bind(b"outside".as_slice())
                    .execute(pool)
                    .await
                    .unwrap();
                }

                let op = Operator::from_parts(OperationContext::new(), Arc::new(backend.clone()));
                op.create_dir("/").await.unwrap();
                op.write("a.txt", "hello").await.unwrap();

                let err = op
                    .delete("/")
                    .await
                    .expect_err("non-empty root delete must fail");
                assert_eq!(err.kind(), ErrorKind::Unexpected);
                assert_eq!(err.message(), "directory not empty");
                assert_eq!(op.read("a.txt").await.unwrap().to_vec(), b"hello");

                let root_key = build_absolute_path(root, "");
                let root_marker_exists: bool =
                    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM test_table WHERE key = $1)")
                        .bind(&root_key)
                        .fetch_one(pool)
                        .await
                        .unwrap();
                assert!(root_marker_exists, "root marker must remain intact");

                op.delete("a.txt").await.unwrap();
                op.delete("/").await.unwrap();

                let root_marker_exists: bool =
                    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM test_table WHERE key = $1)")
                        .bind(&root_key)
                        .fetch_one(pool)
                        .await
                        .unwrap();
                assert!(!root_marker_exists, "empty root marker should be deleted");

                if root != "/" {
                    let outside_exists: bool = sqlx::query_scalar(
                        "SELECT EXISTS (SELECT 1 FROM test_table WHERE key = 'other/file.txt')",
                    )
                    .fetch_one(pool)
                    .await
                    .unwrap();
                    assert!(outside_exists, "keys outside root must remain intact");
                }
            }
        })
        .await
        .expect("test timed out");
    }

    #[tokio::test]
    async fn test_sqlite_writer_close_returns_metadata_for_files_and_directory_markers() {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            for root in ["/", "/sub/"] {
                let backend = build_backend().await.with_normalized_root(root.to_string());
                let pool = backend.core.get_client().await.unwrap();
                sqlx::query("CREATE TABLE test_table (key TEXT PRIMARY KEY, value BLOB)")
                    .execute(pool)
                    .await
                    .unwrap();

                let ctx = OperationContext::new();
                for dir_path in ["", "/", "nested/"] {
                    let mut writer = backend.write(&ctx, dir_path, OpWrite::default()).unwrap();
                    let meta = writer.close().await.unwrap();
                    let expected_mode = if root == "/" && matches!(dir_path, "" | "/") {
                        EntryMode::FILE
                    } else {
                        EntryMode::DIR
                    };
                    assert_eq!(
                        meta.mode(),
                        expected_mode,
                        "writer close mode for path {dir_path:?} under root {root}"
                    );
                    let stat_meta = backend
                        .stat(&ctx, dir_path, OpStat::default())
                        .await
                        .unwrap()
                        .into_metadata();
                    assert_eq!(
                        stat_meta.mode(),
                        EntryMode::DIR,
                        "stat mode for path {dir_path:?} under root {root}"
                    );
                }

                let mut file_writer = backend.write(&ctx, "file.txt", OpWrite::default()).unwrap();
                file_writer.write(Buffer::from("hello")).await.unwrap();
                let file_meta = file_writer.close().await.unwrap();
                assert_eq!(file_meta.mode(), EntryMode::FILE);
                assert_eq!(file_meta.content_length(), 5);
            }
        })
        .await
        .expect("test timed out");
    }

    #[test]
    fn test_sqlite_build_rejects_invalid_config() {
        let valid_config = || SqliteConfig {
            connection_string: Some("sqlite::memory:".to_string()),
            table: Some("data".to_string()),
            ..Default::default()
        };

        for config in [
            SqliteConfig {
                connection_string: None,
                ..valid_config()
            },
            SqliteConfig {
                connection_string: Some(String::new()),
                ..valid_config()
            },
            SqliteConfig {
                connection_string: Some("   \t\n ".to_string()),
                ..valid_config()
            },
            SqliteConfig {
                table: None,
                ..valid_config()
            },
            SqliteConfig {
                table: Some(String::new()),
                ..valid_config()
            },
            SqliteConfig {
                key_field: Some(String::new()),
                ..valid_config()
            },
            SqliteConfig {
                value_field: Some(String::new()),
                ..valid_config()
            },
            SqliteConfig {
                table: Some("data\0bad".to_string()),
                ..valid_config()
            },
            SqliteConfig {
                key_field: Some("key\0bad".to_string()),
                ..valid_config()
            },
            SqliteConfig {
                value_field: Some("val\0bad".to_string()),
                ..valid_config()
            },
        ] {
            let err = config.into_builder().build().unwrap_err();
            assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
        }

        for pairs in [
            vec![("connection_string", ""), ("table", "data")],
            vec![("connection_string", "   "), ("table", "data")],
            vec![("connection_string", "sqlite::memory:"), ("table", "")],
            vec![
                ("connection_string", "sqlite::memory:"),
                ("table", "data\0bad"),
            ],
            vec![
                ("connection_string", "sqlite::memory:"),
                ("table", "data"),
                ("key_field", "key\0bad"),
            ],
            vec![
                ("connection_string", "sqlite::memory:"),
                ("table", "data"),
                ("value_field", "val\0bad"),
            ],
        ] {
            let config = SqliteConfig::from_iter(
                pairs
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.to_string())),
            )
            .unwrap();
            let err = config.into_builder().build().unwrap_err();
            assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
        }

        // Empty optional fields in ConfigDeserializer deserialize as None and
        // fall back to their default column names.
        let config = SqliteConfig::from_iter([
            (
                "connection_string".to_string(),
                "sqlite::memory:".to_string(),
            ),
            ("table".to_string(), "data".to_string()),
            ("key_field".to_string(), String::new()),
            ("value_field".to_string(), String::new()),
        ])
        .unwrap();
        assert!(config.into_builder().build().is_ok());

        for builder in [
            SqliteBuilder::default().connection_string("").table("data"),
            SqliteBuilder::default()
                .connection_string("   ")
                .table("data"),
            SqliteBuilder::default()
                .connection_string("sqlite::memory:")
                .table(""),
            SqliteBuilder::default()
                .connection_string("sqlite::memory:")
                .table("data\0bad"),
            SqliteBuilder::default()
                .connection_string("sqlite::memory:")
                .table("data")
                .key_field("key\0bad"),
            SqliteBuilder::default()
                .connection_string("sqlite::memory:")
                .table("data")
                .value_field("val\0bad"),
        ] {
            let err = builder.build().unwrap_err();
            assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
        }

        let service = SqliteConfig {
            table: Some("my\"table".to_string()),
            ..valid_config()
        }
        .into_builder()
        .build()
        .unwrap();
        assert_eq!(service.info().name().as_ref(), "my\"table");
    }

    #[tokio::test]
    async fn test_sqlite_crud_with_double_quoted_identifiers() {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let table = "my\"table";
            let key_field = "my\"key";
            let value_field = "my\"value";

            let pool = build_pool().await;
            sqlx::query(
                r#"CREATE TABLE "my""table" ("my""key" TEXT PRIMARY KEY, "my""value" BLOB)"#,
            )
            .execute(&pool)
            .await
            .unwrap();

            let backend = SqliteBackend::new(
                SqliteCore::with_pool(pool, table, key_field, value_field),
                table,
            );
            assert_eq!(backend.info().name().as_ref(), table);

            let ctx = OperationContext::new();
            let op = Operator::from_parts(ctx.clone(), Arc::new(backend.clone()));

            op.create_dir("dir/").await.unwrap();
            assert_eq!(op.stat("dir/").await.unwrap().mode(), EntryMode::DIR);

            op.write("dir/hello.txt", "hello world").await.unwrap();
            let meta = op.stat("dir/hello.txt").await.unwrap();
            assert_eq!(meta.mode(), EntryMode::FILE);
            assert_eq!(meta.content_length(), 11);
            assert_eq!(
                op.read("dir/hello.txt").await.unwrap().to_vec(),
                b"hello world"
            );

            // Exercise get_range (bounded and unbounded) and get_suffix with quoted identifiers.
            let reader = backend
                .read(&ctx, "dir/hello.txt", OpRead::default())
                .unwrap();
            let (_, mut stream) = reader.open(BytesRange::from(0_u64..5)).await.unwrap();
            assert_eq!(stream.read_all().await.unwrap().to_vec(), b"hello");
            let (_, mut stream) = reader.open(BytesRange::from(6_u64..)).await.unwrap();
            assert_eq!(stream.read_all().await.unwrap().to_vec(), b"world");
            let (_, mut stream) = reader.open(BytesRange::suffix(5)).await.unwrap();
            assert_eq!(stream.read_all().await.unwrap().to_vec(), b"world");

            // Exercise count_under via implicit directory stat.
            op.write("implicit/child.txt", "x").await.unwrap();
            assert_eq!(op.stat("implicit/").await.unwrap().mode(), EntryMode::DIR);

            // Exercise rename (unconditional and if_not_exists).
            op.rename("dir/hello.txt", "dir/renamed.txt").await.unwrap();
            assert!(!op.exists("dir/hello.txt").await.unwrap());
            assert_eq!(
                op.read("dir/renamed.txt").await.unwrap().to_vec(),
                b"hello world"
            );

            op.write("dir/other.txt", "other").await.unwrap();
            let err = op
                .rename_with("dir/other.txt", "dir/renamed.txt")
                .if_not_exists(true)
                .await
                .unwrap_err();
            assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);

            // Exercise directory non-empty delete check and successful deletes.
            let err = op.delete("dir/").await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert_eq!(err.message(), "directory not empty");

            op.delete("dir/renamed.txt").await.unwrap();
            op.delete("dir/other.txt").await.unwrap();
            op.delete("dir/").await.unwrap();
            assert!(!op.exists("dir/").await.unwrap());
        })
        .await
        .expect("test timed out");
    }
}
