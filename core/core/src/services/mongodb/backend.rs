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

use mea::once::OnceCell;

use super::MONGODB_SCHEME;
use super::config::MongodbConfig;
use super::core::*;
use super::deleter::MongodbDeleter;
use super::writer::MongodbWriter;
use crate::raw::*;
use crate::*;

#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct MongodbBuilder {
    pub(super) config: MongodbConfig,
}

impl MongodbBuilder {
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

    /// Set the database name of the MongoDB service to read/write.
    pub fn database(mut self, database: &str) -> Self {
        if !database.is_empty() {
            self.config.database = Some(database.to_string());
        }
        self
    }

    /// Set the collection name of the MongoDB service to read/write.
    pub fn collection(mut self, collection: &str) -> Self {
        if !collection.is_empty() {
            self.config.collection = Some(collection.to_string());
        }
        self
    }

    /// Set the key field name of the MongoDB service to read/write.
    ///
    /// Default to `key` if not specified.
    pub fn key_field(mut self, key_field: &str) -> Self {
        if !key_field.is_empty() {
            self.config.key_field = Some(key_field.to_string());
        }
        self
    }

    /// Set the value field name of the MongoDB service to read/write.
    ///
    /// Default to `value` if not specified.
    pub fn value_field(mut self, value_field: &str) -> Self {
        if !value_field.is_empty() {
            self.config.value_field = Some(value_field.to_string());
        }
        self
    }
}

impl Builder for MongodbBuilder {
    type Config = MongodbConfig;

    fn build(self) -> Result<impl Access> {
        let conn = match &self.config.connection_string.clone() {
            Some(v) => v.clone(),
            None => {
                return Err(
                    Error::new(ErrorKind::ConfigInvalid, "connection_string is required")
                        .with_context("service", MONGODB_SCHEME),
                );
            }
        };
        let database = match &self.config.database.clone() {
            Some(v) => v.clone(),
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "database is required")
                    .with_context("service", MONGODB_SCHEME));
            }
        };
        let collection = match &self.config.collection.clone() {
            Some(v) => v.clone(),
            None => {
                return Err(
                    Error::new(ErrorKind::ConfigInvalid, "collection is required")
                        .with_context("service", MONGODB_SCHEME),
                );
            }
        };
        let key_field = match &self.config.key_field.clone() {
            Some(v) => v.clone(),
            None => "key".to_string(),
        };
        let value_field = match &self.config.value_field.clone() {
            Some(v) => v.clone(),
            None => "value".to_string(),
        };
        let root = normalize_root(
            self.config
                .root
                .clone()
                .unwrap_or_else(|| "/".to_string())
                .as_str(),
        );
        Ok(MongodbBackend::new(MongodbCore {
            connection_string: conn,
            database,
            collection,
            collection_instance: OnceCell::new(),
            key_field,
            value_field,
        })
        .with_normalized_root(root))
    }
}

/// Backend for Mongodb services.
#[derive(Clone, Debug)]
pub struct MongodbBackend {
    core: Arc<MongodbCore>,
    root: String,
    info: Arc<AccessorInfo>,
}

impl MongodbBackend {
    pub fn new(core: MongodbCore) -> Self {
        let info = AccessorInfo::default();
        info.set_scheme(MONGODB_SCHEME);
        info.set_name(&format!("{}/{}", core.database, core.collection));
        info.set_root("/");
        info.set_native_capability(Capability {
            read: true,
            stat: true,
            write: true,
            write_can_empty: true,
            delete: true,
            shared: true,
            ..Default::default()
        });

        Self {
            core: Arc::new(core),
            root: "/".to_string(),
            info: Arc::new(info),
        }
    }

    fn with_normalized_root(mut self, root: String) -> Self {
        self.info.set_root(&root);
        self.root = root;
        self
    }
}

impl Access for MongodbBackend {
    type Reader = Buffer;
    type Writer = MongodbWriter;
    type Lister = ();
    type Deleter = oio::OneShotDeleter<MongodbDeleter>;

    fn info(&self) -> Arc<AccessorInfo> {
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
                    Metadata::new(EntryMode::FILE).with_content_length(bs.len() as u64),
                )),
                None => Err(Error::new(ErrorKind::NotFound, "kv not found in mongodb")),
            }
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let p = build_abs_path(&self.root, path);
        let bs = match self.core.get(&p).await? {
            Some(bs) => bs,
            None => {
                return Err(Error::new(ErrorKind::NotFound, "kv not found in mongodb"));
            }
        };
        Ok((RpRead::new(), bs.slice(args.range().to_range_as_usize())))
    }

    async fn write(&self, path: &str, _: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let p = build_abs_path(&self.root, path);
        Ok((RpWrite::new(), MongodbWriter::new(self.core.clone(), p)))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(MongodbDeleter::new(self.core.clone(), self.root.clone())),
        ))
    }
}
