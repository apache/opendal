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

use futures::AsyncWriteExt;
use futures::StreamExt;
use mongodb::bson::doc;
use mongodb::options::ClientOptions;
use mongodb::options::GridFsBucketOptions;
use mongodb::options::GridFsFindOptions;
use mongodb::GridFsBucket;
use tokio::sync::OnceCell;

use crate::raw::adapters::kv;
use crate::raw::new_std_io_error;
use crate::*;

#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct GridFsBuilder {
    connection_string: Option<String>,
    database: Option<String>,
    bucket: Option<String>,
    chunk_size: Option<u32>,
    root: Option<String>,
}

impl Debug for GridFsBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GridFsBuilder")
            .field("database", &self.database)
            .field("bucket", &self.bucket)
            .field("chunk_size", &self.chunk_size)
            .field("root", &self.root)
            .finish()
    }
}

impl GridFsBuilder {
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
    pub fn connection_string(&mut self, v: &str) -> &mut Self {
        if !v.is_empty() {
            self.connection_string = Some(v.to_string());
        }
        self
    }

    /// Set the working directory, all operations will be performed under it.
    ///
    /// default: "/"
    pub fn root(&mut self, root: &str) -> &mut Self {
        if !root.is_empty() {
            self.root = Some(root.to_owned());
        }
        self
    }

    /// Set the database name of the MongoDB GridFs service to read/write.
    pub fn database(&mut self, database: &str) -> &mut Self {
        if !database.is_empty() {
            self.database = Some(database.to_string());
        }
        self
    }

    /// Set the bucket name of the MongoDB GridFs service to read/write.
    ///
    /// Default to `fs` if not specified.
    pub fn bucket(&mut self, bucket: &str) -> &mut Self {
        if !bucket.is_empty() {
            self.bucket = Some(bucket.to_string());
        }
        self
    }

    /// Set the chunk size of the MongoDB GridFs service used to break the user file into chunks.
    ///
    /// Default to `255 KiB` if not specified.
    pub fn chunk_size(&mut self, chunk_size: u32) -> &mut Self {
        if chunk_size > 0 {
            self.chunk_size = Some(chunk_size);
        }
        self
    }
}

impl Builder for GridFsBuilder {
    const SCHEME: Scheme = Scheme::Mongodb;
    type Accessor = GridFsBackend;

    fn from_map(map: std::collections::HashMap<String, String>) -> Self {
        let mut builder = Self::default();

        map.get("connection_string")
            .map(|v| builder.connection_string(v));
        map.get("database").map(|v| builder.database(v));
        map.get("bucket").map(|v| builder.bucket(v));
        map.get("chunk_size")
            .map(|v| builder.chunk_size(v.parse::<u32>().unwrap_or_default()));
        map.get("root").map(|v| builder.root(v));

        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        let conn = match &self.connection_string.clone() {
            Some(v) => v.clone(),
            None => {
                return Err(
                    Error::new(ErrorKind::ConfigInvalid, "connection_string is required")
                        .with_context("service", Scheme::Gridfs),
                )
            }
        };
        let database = match &self.database.clone() {
            Some(v) => v.clone(),
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "database is required")
                    .with_context("service", Scheme::Gridfs))
            }
        };
        let bucket = match &self.bucket.clone() {
            Some(v) => v.clone(),
            None => "fs".to_string(),
        };
        let chunk_size = self.chunk_size.unwrap_or(255);

        Ok(GridFsBackend::new(Adapter {
            connection_string: conn,
            database,
            bucket,
            chunk_size,
            bucket_instance: OnceCell::new(),
        }))
    }
}

pub type GridFsBackend = kv::Backend<Adapter>;

#[derive(Clone)]
pub struct Adapter {
    connection_string: String,
    database: String,
    bucket: String,
    chunk_size: u32,
    bucket_instance: OnceCell<GridFsBucket>,
}

impl Debug for Adapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Adapter")
            .field("database", &self.database)
            .field("bucket", &self.bucket)
            .field("chunk_size", &self.chunk_size)
            .finish()
    }
}

impl Adapter {
    async fn get_bucket(&self) -> Result<&GridFsBucket> {
        self.bucket_instance
            .get_or_try_init(|| async {
                let client_options = ClientOptions::parse(&self.connection_string)
                    .await
                    .map_err(parse_mongodb_error)?;
                let client =
                    mongodb::Client::with_options(client_options).map_err(parse_mongodb_error)?;
                let bucket_options = GridFsBucketOptions::builder()
                    .bucket_name(Some(self.bucket.clone()))
                    .chunk_size_bytes(Some(self.chunk_size))
                    .build();
                let bucket = client
                    .database(&self.database)
                    .gridfs_bucket(bucket_options);
                Ok(bucket)
            })
            .await
    }
}

impl kv::Adapter for Adapter {
    fn metadata(&self) -> kv::Metadata {
        kv::Metadata::new(
            Scheme::Gridfs,
            &format!("{}/{}", self.database, self.bucket),
            Capability {
                read: true,
                write: true,
                ..Default::default()
            },
        )
    }

    async fn get(&self, path: &str) -> Result<Option<Buffer>> {
        let bucket = self.get_bucket().await?;
        let filter = doc! { "filename": path };
        let options = GridFsFindOptions::builder().limit(Some(1)).build();
        let mut cursor = bucket
            .find(filter, options)
            .await
            .map_err(parse_mongodb_error)?;

        match cursor.next().await {
            Some(doc) => {
                let mut destination = Vec::new();
                let file_id = doc.map_err(parse_mongodb_error)?.id;
                bucket
                    .download_to_futures_0_3_writer(file_id, &mut destination)
                    .await
                    .map_err(parse_mongodb_error)?;
                Ok(Some(Buffer::from(destination)))
            }
            None => Ok(None),
        }
    }

    async fn set(&self, path: &str, value: Buffer) -> Result<()> {
        let bucket = self.get_bucket().await?;
        // delete old file if exists
        let filter = doc! { "filename": path };
        let options = GridFsFindOptions::builder().limit(Some(1)).build();
        let mut cursor = bucket
            .find(filter, options)
            .await
            .map_err(parse_mongodb_error)?;
        if let Some(doc) = cursor.next().await {
            let file_id = doc.map_err(parse_mongodb_error)?.id;
            bucket.delete(file_id).await.map_err(parse_mongodb_error)?;
        }
        // set new file
        let mut upload_stream = bucket.open_upload_stream(path, None);
        upload_stream
            .write_all(&value.to_vec())
            .await
            .map_err(new_std_io_error)?;
        upload_stream.close().await.map_err(new_std_io_error)?;

        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let bucket = self.get_bucket().await?;
        let filter = doc! { "filename": path };
        let mut cursor = bucket
            .find(filter, None)
            .await
            .map_err(parse_mongodb_error)?;
        while let Some(doc) = cursor.next().await {
            let file_id = doc.map_err(parse_mongodb_error)?.id;
            bucket.delete(file_id).await.map_err(parse_mongodb_error)?;
        }
        Ok(())
    }
}

fn parse_mongodb_error(err: mongodb::error::Error) -> Error {
    Error::new(ErrorKind::Unexpected, "mongodb error").set_source(err)
}
