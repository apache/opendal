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
use mongodb::bson::Binary;
use mongodb::bson::Document;
use mongodb::options::ClientOptions;
use tokio::sync::OnceCell;

use crate::raw::adapters::kv;
use crate::raw::*;
use crate::services::MongodbConfig;
use crate::*;

impl Configurator for MongodbConfig {
    type Builder = MongodbBuilder;
    fn into_builder(self) -> Self::Builder {
        MongodbBuilder { config: self }
    }
}

#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct MongodbBuilder {
    config: MongodbConfig,
}

impl Debug for MongodbBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MongodbBuilder")
            .field("config", &self.config)
            .finish()
    }
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
    const SCHEME: Scheme = Scheme::Mongodb;
    type Config = MongodbConfig;

    fn build(self) -> Result<impl Access> {
        let conn = match &self.config.connection_string.clone() {
            Some(v) => v.clone(),
            None => {
                return Err(
                    Error::new(ErrorKind::ConfigInvalid, "connection_string is required")
                        .with_context("service", Scheme::Mongodb),
                )
            }
        };
        let database = match &self.config.database.clone() {
            Some(v) => v.clone(),
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "database is required")
                    .with_context("service", Scheme::Mongodb))
            }
        };
        let collection = match &self.config.collection.clone() {
            Some(v) => v.clone(),
            None => {
                return Err(
                    Error::new(ErrorKind::ConfigInvalid, "collection is required")
                        .with_context("service", Scheme::Mongodb),
                )
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
        Ok(MongodbBackend::new(Adapter {
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

pub type MongodbBackend = kv::Backend<Adapter>;

#[derive(Clone)]
pub struct Adapter {
    connection_string: String,
    database: String,
    collection: String,
    collection_instance: OnceCell<mongodb::Collection<Document>>,
    key_field: String,
    value_field: String,
}

impl Debug for Adapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Adapter")
            .field("connection_string", &self.connection_string)
            .field("database", &self.database)
            .field("collection", &self.collection)
            .field("key_field", &self.key_field)
            .field("value_field", &self.value_field)
            .finish()
    }
}

impl Adapter {
    async fn get_collection(&self) -> Result<&mongodb::Collection<Document>> {
        self.collection_instance
            .get_or_try_init(|| async {
                let client_options = ClientOptions::parse(&self.connection_string)
                    .await
                    .map_err(parse_mongodb_error)?;
                let client =
                    mongodb::Client::with_options(client_options).map_err(parse_mongodb_error)?;
                let database = client.database(&self.database);
                let collection = database.collection(&self.collection);
                Ok(collection)
            })
            .await
    }
}

impl kv::Adapter for Adapter {
    type Scanner = ();

    fn info(&self) -> kv::Info {
        kv::Info::new(
            Scheme::Mongodb,
            &format!("{}/{}", self.database, self.collection),
            Capability {
                read: true,
                write: true,
                shared: true,
                ..Default::default()
            },
        )
    }

    async fn get(&self, path: &str) -> Result<Option<Buffer>> {
        let collection = self.get_collection().await?;
        let filter = doc! {self.key_field.as_str():path};
        let result = collection
            .find_one(filter)
            .await
            .map_err(parse_mongodb_error)?;
        match result {
            Some(doc) => {
                let value = doc
                    .get_binary_generic(&self.value_field)
                    .map_err(parse_bson_error)?;
                Ok(Some(Buffer::from(value.to_vec())))
            }
            None => Ok(None),
        }
    }

    async fn set(&self, path: &str, value: Buffer) -> Result<()> {
        let collection = self.get_collection().await?;
        let filter = doc! { self.key_field.as_str(): path };
        let update = doc! { "$set": { self.value_field.as_str(): Binary { subtype: mongodb::bson::spec::BinarySubtype::Generic, bytes: value.to_vec() } } };
        collection
            .update_one(filter, update)
            .upsert(true)
            .await
            .map_err(parse_mongodb_error)?;

        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let collection = self.get_collection().await?;
        let filter = doc! {self.key_field.as_str():path};
        collection
            .delete_one(filter)
            .await
            .map_err(parse_mongodb_error)?;
        Ok(())
    }
}

fn parse_mongodb_error(err: mongodb::error::Error) -> Error {
    Error::new(ErrorKind::Unexpected, "mongodb error").set_source(err)
}

fn parse_bson_error(err: mongodb::bson::document::ValueAccessError) -> Error {
    Error::new(ErrorKind::Unexpected, "bson error").set_source(err)
}
