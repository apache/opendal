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
use std::fmt::Formatter;

use mongodb::bson::doc;
use tokio::sync::OnceCell;

use super::config::GridfsConfig;
use super::core::GridFsCore;
use crate::raw::adapters::kv;
use crate::raw::*;
use crate::*;

impl Configurator for GridfsConfig {
    type Builder = GridfsBuilder;
    fn into_builder(self) -> Self::Builder {
        GridfsBuilder { config: self }
    }
}

#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct GridfsBuilder {
    config: GridfsConfig,
}

impl Debug for GridfsBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("GridFsBuilder");
        d.field("config", &self.config);
        d.finish_non_exhaustive()
    }
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

    /// Set the chunk size of the MongoDB GridFs service used to break the user file into chunks.
    ///
    /// Default to `255 KiB` if not specified.
    pub fn chunk_size(mut self, chunk_size: u32) -> Self {
        if chunk_size > 0 {
            self.config.chunk_size = Some(chunk_size);
        }
        self
    }
}

impl Builder for GridfsBuilder {
    type Config = GridfsConfig;

    fn build(self) -> Result<impl Access> {
        let conn = match &self.config.connection_string.clone() {
            Some(v) => v.clone(),
            None => {
                return Err(
                    Error::new(ErrorKind::ConfigInvalid, "connection_string is required")
                        .with_context("service", Scheme::Gridfs),
                )
            }
        };
        let database = match &self.config.database.clone() {
            Some(v) => v.clone(),
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "database is required")
                    .with_context("service", Scheme::Gridfs))
            }
        };
        let bucket = match &self.config.bucket.clone() {
            Some(v) => v.clone(),
            None => "fs".to_string(),
        };
        let chunk_size = self.config.chunk_size.unwrap_or(255);

        let root = normalize_root(
            self.config
                .root
                .clone()
                .unwrap_or_else(|| "/".to_string())
                .as_str(),
        );

        Ok(GridFsBackend::new(GridFsCore {
            connection_string: conn,
            database,
            bucket,
            chunk_size,
            bucket_instance: OnceCell::new(),
        })
        .with_normalized_root(root))
    }
}

pub type GridFsBackend = kv::Backend<GridFsCore>;
