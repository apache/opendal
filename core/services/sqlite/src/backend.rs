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

use asyncband::once::OnceCell;
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

impl Builder for SqliteBuilder {
    type Config = SqliteConfig;

    fn build(self) -> Result<impl Service> {
        let conn = match self.config.connection_string {
            Some(v) => v,
            None => {
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

        let key_field = self.config.key_field.unwrap_or_else(|| "key".to_string());

        let value_field = self
            .config
            .value_field
            .unwrap_or_else(|| "value".to_string());

        let root = normalize_root(self.config.root.as_deref().unwrap_or("/"));

        Ok(SqliteBackend::new(SqliteCore {
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

/// SqliteBackend implements [`Service`] for SQLite-backed object storage.
#[derive(Debug, Clone)]
pub struct SqliteBackend {
    pub(crate) core: Arc<SqliteCore>,
    pub(crate) root: String,
    pub(crate) info: ServiceInfo,
    pub(crate) capability: Capability,
}

impl SqliteBackend {
    fn new(core: SqliteCore) -> Self {
        let info = ServiceInfo::new(SQLITE_SCHEME, "/", &core.table);
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

    async fn build_client() -> OnceCell<SqlitePool> {
        let config = SqliteConnectOptions::from_str("sqlite::memory:").unwrap();
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(config)
            .await
            .unwrap();
        OnceCell::from_value(pool)
    }

    async fn build_backend() -> SqliteBackend {
        let core = SqliteCore {
            pool: build_client().await,
            config: Default::default(),
            table: "test_table".to_string(),
            key_field: "key".to_string(),
            value_field: "value".to_string(),
        };

        SqliteBackend::new(core)
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
        let backend = SqliteBackend::new(SqliteCore {
            pool: OnceCell::from_value(reader_pool),
            config: Default::default(),
            table: "test_table".to_string(),
            key_field: "key".to_string(),
            value_field: "value".to_string(),
        });

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
}
