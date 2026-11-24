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
use std::sync::Arc;

use asyncband::once::OnceCell;

use super::SURREALDB_SCHEME;
use super::config::SurrealdbConfig;
use super::core::*;
use super::deleter::SurrealdbDeleter;
use super::reader::*;
use super::writer::SurrealdbWriter;
use opendal_core::raw::*;
use opendal_core::*;

#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct SurrealdbBuilder {
    pub(super) config: SurrealdbConfig,
}

impl SurrealdbBuilder {
    /// Set the connection_string of the surrealdb service.
    ///
    /// This connection string is used to connect to the surrealdb service. There are url based formats:
    ///
    /// ## Url
    ///
    /// - `ws://ip:port`
    /// - `wss://ip:port`
    /// - `http://ip:port`
    /// - `https://ip:port`
    pub fn connection_string(mut self, connection_string: &str) -> Self {
        if !connection_string.is_empty() {
            self.config.connection_string = Some(connection_string.to_string());
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

    /// Set the table name of the surrealdb service for read/write.
    pub fn table(mut self, table: &str) -> Self {
        if !table.is_empty() {
            self.config.table = Some(table.to_string());
        }
        self
    }

    /// Set the username of the surrealdb service for signin.
    pub fn username(mut self, username: &str) -> Self {
        if !username.is_empty() {
            self.config.username = Some(username.to_string());
        }
        self
    }

    /// Set the password of the surrealdb service for signin.
    pub fn password(mut self, password: &str) -> Self {
        if !password.is_empty() {
            self.config.password = Some(password.to_string());
        }
        self
    }

    /// Set the namespace of the surrealdb service for read/write.
    pub fn namespace(mut self, namespace: &str) -> Self {
        if !namespace.is_empty() {
            self.config.namespace = Some(namespace.to_string());
        }
        self
    }

    /// Set the database of the surrealdb service for read/write.
    pub fn database(mut self, database: &str) -> Self {
        if !database.is_empty() {
            self.config.database = Some(database.to_string());
        }
        self
    }

    /// Set the key field name of the surrealdb service for read/write.
    ///
    /// Default to `key` if not specified.
    pub fn key_field(mut self, key_field: &str) -> Self {
        if !key_field.is_empty() {
            self.config.key_field = Some(key_field.to_string());
        }
        self
    }

    /// Set the value field name of the surrealdb service for read/write.
    ///
    /// Default to `value` if not specified.
    pub fn value_field(mut self, value_field: &str) -> Self {
        if !value_field.is_empty() {
            self.config.value_field = Some(value_field.to_string());
        }
        self
    }
}

impl SurrealdbBuilder {
    fn build_backend(self) -> Result<SurrealdbBackend> {
        let connection_string = match self.config.connection_string.filter(|v| !v.is_empty()) {
            Some(v) => v,
            None => {
                return Err(
                    Error::new(ErrorKind::ConfigInvalid, "connection_string is empty")
                        .with_context("service", SURREALDB_SCHEME),
                );
            }
        };

        let namespace = match self.config.namespace.filter(|v| !v.is_empty()) {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "namespace is empty")
                    .with_context("service", SURREALDB_SCHEME));
            }
        };
        let database = match self.config.database.filter(|v| !v.is_empty()) {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "database is empty")
                    .with_context("service", SURREALDB_SCHEME));
            }
        };
        let table = match self.config.table.filter(|v| !v.is_empty()) {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "table is empty")
                    .with_context("service", SURREALDB_SCHEME));
            }
        };

        let username = self
            .config
            .username
            .filter(|v| !v.is_empty())
            .unwrap_or_default();
        let password = self
            .config
            .password
            .filter(|v| !v.is_empty())
            .unwrap_or_default();
        let key_field = self
            .config
            .key_field
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| "key".to_string());
        let value_field = self
            .config
            .value_field
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| "value".to_string());
        let root = normalize_root(
            self.config
                .root
                .as_deref()
                .filter(|v| !v.is_empty())
                .unwrap_or("/"),
        );

        Ok(SurrealdbBackend::new(SurrealdbCore {
            db: OnceCell::new(),
            connection_string,
            username,
            password,
            namespace,
            database,
            table,
            key_field,
            value_field,
        })
        .with_normalized_root(root))
    }
}

impl Builder for SurrealdbBuilder {
    type Config = SurrealdbConfig;

    fn build(self) -> Result<impl Service> {
        self.build_backend()
    }
}

/// Backend for Surrealdb service
#[derive(Clone, Debug)]
pub struct SurrealdbBackend {
    pub(crate) core: Arc<SurrealdbCore>,
    pub(crate) root: String,
    pub(crate) info: ServiceInfo,
    pub(crate) capability: Capability,
}

impl SurrealdbBackend {
    pub fn new(core: SurrealdbCore) -> Self {
        let info = ServiceInfo::new(SURREALDB_SCHEME, "/", &core.table);
        let capability = Capability {
            read: true,
            stat: true,
            write: true,
            write_can_empty: true,
            delete: true,
            shared: true,
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

impl Service for SurrealdbBackend {
    type Reader = oio::StreamReader<SurrealdbReader>;
    type Writer = SurrealdbWriter;
    type Lister = ();
    type Deleter = oio::OneShotDeleter<SurrealdbDeleter>;
    type Copier = ();
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        self.info.clone()
    }

    fn capability(&self) -> Capability {
        self.capability
    }

    async fn create_dir(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn stat(&self, _ctx: &OperationContext, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_absolute_path(&self.root, path);

        if p == build_absolute_path(&self.root, "") {
            Ok(RpStat::new(MetadataBuilder::dir().build()))
        } else {
            match self.core.get_length(&p).await? {
                Some(length) => {
                    let metadata = if p.ends_with('/') {
                        MetadataBuilder::dir()
                    } else {
                        MetadataBuilder::file(length as u64)
                    };
                    Ok(RpStat::new(metadata.build()))
                }
                None => Err(Error::new(ErrorKind::NotFound, "kv not found in surrealdb")),
            }
        }
    }
    fn read(&self, _ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let output: oio::StreamReader<SurrealdbReader> = {
            Ok(oio::StreamReader::new(SurrealdbReader::new(
                self.clone(),
                path,
                args,
            )))
        }?;

        Ok(output)
    }

    fn write(&self, _ctx: &OperationContext, path: &str, _: OpWrite) -> Result<Self::Writer> {
        let output: SurrealdbWriter = {
            let p = build_absolute_path(&self.root, path);
            Ok(SurrealdbWriter::new(self.core.clone(), p))
        }?;

        Ok(output)
    }

    fn delete(&self, _ctx: &OperationContext) -> Result<Self::Deleter> {
        let output: oio::OneShotDeleter<SurrealdbDeleter> = {
            Ok(oio::OneShotDeleter::new(SurrealdbDeleter::new(
                self.core.clone(),
                self.root.clone(),
            )))
        }?;

        Ok(output)
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
        _from: &str,
        _to: &str,
        _args: OpRename,
    ) -> Result<RpRename> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
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
mod tests {
    use std::time::Duration;

    use super::*;

    fn valid_config_entries() -> Vec<(String, String)> {
        vec![
            (
                "connection_string".to_string(),
                "http://127.0.0.1:8000".to_string(),
            ),
            ("namespace".to_string(), "test_ns".to_string()),
            ("database".to_string(), "test_db".to_string()),
            ("table".to_string(), "test_table".to_string()),
        ]
    }

    #[test]
    fn build_rejects_empty_required_fields() {
        for field in ["connection_string", "namespace", "database", "table"] {
            let mut entries = valid_config_entries();
            for (k, v) in &mut entries {
                if k == field {
                    v.clear();
                }
            }
            let cfg = SurrealdbConfig::from_iter(entries).unwrap();
            let err = cfg.into_builder().build().err().unwrap();
            assert_eq!(
                err.kind(),
                ErrorKind::ConfigInvalid,
                "empty {field} must fail with ConfigInvalid"
            );
        }
    }

    #[test]
    fn build_falls_back_to_default_fields_when_empty() {
        let mut entries = valid_config_entries();
        entries.push(("key_field".to_string(), String::new()));
        entries.push(("value_field".to_string(), String::new()));
        entries.push(("root".to_string(), String::new()));

        let cfg = SurrealdbConfig::from_iter(entries).unwrap();
        let backend = cfg.into_builder().build_backend().unwrap();
        assert_eq!(backend.info().root().as_ref(), "/");
        assert_eq!(backend.info().name().as_ref(), "test_table");
        assert_eq!(backend.core.key_field, "key");
        assert_eq!(backend.core.value_field, "value");
    }

    #[tokio::test]
    async fn stat_root_returns_dir() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let cfg = SurrealdbConfig::from_iter(valid_config_entries()).unwrap();
            let service = cfg.into_builder().build().unwrap();
            let ctx = OperationContext::default();

            let root_stat = service.stat(&ctx, "/", OpStat::new()).await.unwrap();
            assert_eq!(root_stat.into_metadata().mode(), EntryMode::DIR);

            let empty_stat = service.stat(&ctx, "", OpStat::new()).await.unwrap();
            assert_eq!(empty_stat.into_metadata().mode(), EntryMode::DIR);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn root_write_and_delete_short_circuit_with_id_key_field() {
        use oio::Delete;
        use oio::Write;

        tokio::time::timeout(Duration::from_secs(5), async {
            let mut entries = valid_config_entries();
            entries.push(("key_field".to_string(), "id".to_string()));
            entries.push(("root".to_string(), "/".to_string()));

            let cfg = SurrealdbConfig::from_iter(entries).unwrap();
            let service = cfg.into_builder().build().unwrap();
            let ctx = OperationContext::default();

            let mut writer = service.write(&ctx, "/", OpWrite::new()).unwrap();
            let meta = writer.close().await.unwrap();
            assert_eq!(meta.mode(), EntryMode::DIR);

            let mut deleter = service.delete(&ctx).unwrap();
            deleter.delete("/", OpDelete::new()).await.unwrap();
            deleter.close().await.unwrap();
        })
        .await
        .unwrap();
    }
}
