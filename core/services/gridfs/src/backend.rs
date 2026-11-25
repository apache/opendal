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

use std::sync::Arc;

use asyncband::once::OnceCell;
use opendal_core::raw::*;
use opendal_core::*;

use super::GRIDFS_SCHEME;
use super::config::GridfsConfig;
use super::core::*;
use super::deleter::GridfsDeleter;
use super::reader::*;
use super::writer::GridfsWriter;

#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct GridfsBuilder {
    pub(super) config: GridfsConfig,
}

impl GridfsBuilder {
    /// Set the connection_string of the MongoDB service.
    ///
    /// This connection string is used to connect to the MongoDB service. It typically follows the format:
    ///
    /// ## Format
    ///
    /// `mongodb://[username:password@]host1[:port1][,...hostN[:portN]][/[defaultauthdb][?options]]`
    ///
    /// Examples:
    ///
    /// - Connecting to a local MongoDB instance: `mongodb://localhost:27017`
    /// - Using authentication: `mongodb://myUser:myPassword@localhost:27017/myAuthDB`
    /// - Specifying authentication mechanism: `mongodb://myUser:myPassword@localhost:27017/myAuthDB?authMechanism=SCRAM-SHA-256`
    ///
    /// ## Options
    ///
    /// - `authMechanism`: Specifies the authentication method to use. Examples include `SCRAM-SHA-1`, `SCRAM-SHA-256`, and `MONGODB-AWS`.
    /// - ... (any other options you wish to highlight)
    ///
    /// For more information, please refer to [MongoDB Connection String URI Format](https://docs.mongodb.com/manual/reference/connection-string/).
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

    /// Set the database name of the MongoDB GridFs service to read/write.
    pub fn database(mut self, database: &str) -> Self {
        if !database.is_empty() {
            self.config.database = Some(database.to_string());
        }
        self
    }

    /// Set the bucket name of the MongoDB GridFs service to read/write.
    ///
    /// Default to `fs` if not specified.
    pub fn bucket(mut self, bucket: &str) -> Self {
        if !bucket.is_empty() {
            self.config.bucket = Some(bucket.to_string());
        }
        self
    }

    /// Set the chunk size in bytes for MongoDB GridFs service. We break the user file into chunks by size.
    ///
    /// Default to `255 KiB` if not specified.
    pub fn chunk_size(mut self, chunk_size: u32) -> Self {
        if chunk_size > 0 {
            self.config.chunk_size = Some(chunk_size);
        }
        self
    }
}

/// Default GridFS chunk size in bytes.
const DEFAULT_CHUNK_SIZE_BYTES: u32 = 255 * 1024; // 255 KiB

impl Builder for GridfsBuilder {
    type Config = GridfsConfig;

    fn build(self) -> Result<impl Service> {
        let Some(connection_string) = self.config.connection_string else {
            return Err(
                Error::new(ErrorKind::ConfigInvalid, "connection_string is required")
                    .with_context("service", GRIDFS_SCHEME),
            );
        };
        let Some(database) = self.config.database else {
            return Err(Error::new(ErrorKind::ConfigInvalid, "database is required")
                .with_context("service", GRIDFS_SCHEME));
        };
        let bucket = self.config.bucket.unwrap_or_else(|| "fs".to_string());
        let chunk_size = match self.config.chunk_size {
            Some(0) => {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "chunk_size must be greater than 0",
                )
                .with_context("service", GRIDFS_SCHEME));
            }
            Some(v) => v,
            None => DEFAULT_CHUNK_SIZE_BYTES,
        };

        let root = normalize_root(self.config.root.as_deref().unwrap_or("/"));

        Ok(GridfsBackend::new(GridfsCore {
            connection_string,
            database,
            bucket,
            chunk_size,
            bucket_instance: OnceCell::new(),
        })
        .with_normalized_root(root))
    }
}

/// Backend for Gridfs services.
#[derive(Clone, Debug)]
pub struct GridfsBackend {
    pub(crate) core: Arc<GridfsCore>,
    pub(crate) root: String,
    pub(crate) info: ServiceInfo,
    pub(crate) capability: Capability,
}

impl GridfsBackend {
    pub fn new(core: GridfsCore) -> Self {
        let info = ServiceInfo::new(
            GRIDFS_SCHEME,
            "/",
            format!("{}/{}", core.database, core.bucket),
        );
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

impl Service for GridfsBackend {
    type Reader = oio::StreamReader<GridfsReader>;
    type Writer = GridfsWriter;
    type Lister = ();
    type Deleter = oio::OneShotDeleter<GridfsDeleter>;
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
                Some(len) => {
                    let metadata = if p.ends_with('/') {
                        MetadataBuilder::dir()
                    } else {
                        MetadataBuilder::file(len)
                    };
                    Ok(RpStat::new(metadata.build()))
                }
                None => Err(Error::new(ErrorKind::NotFound, "kv not found in gridfs")),
            }
        }
    }
    fn read(&self, _ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let output: oio::StreamReader<GridfsReader> = {
            Ok(oio::StreamReader::new(GridfsReader::new(
                self.clone(),
                path,
                args,
            )))
        }?;

        Ok(output)
    }

    fn write(&self, _ctx: &OperationContext, path: &str, _: OpWrite) -> Result<Self::Writer> {
        let output: GridfsWriter = {
            let p = build_absolute_path(&self.root, path);
            Ok(GridfsWriter::new(self.core.clone(), p))
        }?;

        Ok(output)
    }

    fn delete(&self, _ctx: &OperationContext) -> Result<Self::Deleter> {
        let output: oio::OneShotDeleter<GridfsDeleter> = {
            Ok(oio::OneShotDeleter::new(GridfsDeleter::new(
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
    use super::*;

    #[test]
    fn build_rejects_zero_chunk_size_from_iter() -> Result<()> {
        let cfg = GridfsConfig::from_iter([
            (
                "connection_string".to_string(),
                "mongodb://127.0.0.1:27017".to_string(),
            ),
            ("database".to_string(), "demo".to_string()),
            ("chunk_size".to_string(), "0".to_string()),
        ])?;
        assert_eq!(
            cfg.into_builder().build().err().unwrap().kind(),
            ErrorKind::ConfigInvalid
        );
        Ok(())
    }

    #[test]
    fn builder_setter_ignores_zero_chunk_size() -> Result<()> {
        let default_builder_service = GridfsBuilder::default()
            .connection_string("mongodb://127.0.0.1:27017")
            .database("demo")
            .chunk_size(0)
            .build()?;
        assert_eq!(default_builder_service.info().name().as_ref(), "demo/fs");
        Ok(())
    }
}
