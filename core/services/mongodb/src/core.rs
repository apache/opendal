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

use asyncband::once::OnceCell;
use mongodb::bson::Binary;
use mongodb::bson::Bson;
use mongodb::bson::Document;
use mongodb::bson::doc;
use mongodb::options::ClientOptions;
use mongodb::options::ConnectionString;

use opendal_core::*;

const DUPLICATE_KEY_ERROR_CODE: i32 = 11000;

#[derive(Clone)]
pub struct MongodbCore {
    pub connection_string: ConnectionString,
    pub database: String,
    pub collection: String,
    pub collection_instance: OnceCell<mongodb::Collection<Document>>,
    pub key_field: String,
    pub value_field: String,
}

impl Debug for MongodbCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MongodbCore")
            .field("database", &self.database)
            .field("collection", &self.collection)
            .field("key_field", &self.key_field)
            .field("value_field", &self.value_field)
            .finish_non_exhaustive()
    }
}

impl MongodbCore {
    async fn get_collection(&self) -> Result<&mongodb::Collection<Document>> {
        self.collection_instance
            .get_or_try_init(|| async {
                let client_options = ClientOptions::parse(self.connection_string.clone())
                    .await
                    .map_err(parse_mongodb_config_error)?;
                let client = mongodb::Client::with_options(client_options)
                    .map_err(parse_mongodb_config_error)?;
                let database = client.database(&self.database);
                let collection = database.collection(&self.collection);
                Ok(collection)
            })
            .await
    }

    pub async fn get(&self, path: &str) -> Result<Option<Buffer>> {
        let collection = self.get_collection().await?;
        let filter = doc! {self.key_field.as_str(): path};
        let result = collection
            .find_one(filter)
            .await
            .map_err(parse_mongodb_error)?;
        match result {
            Some(doc) => {
                let value = get_binary_field(&doc, &self.value_field)?;
                Ok(Some(Buffer::from(value.to_vec())))
            }
            None => Ok(None),
        }
    }

    pub async fn get_length(&self, path: &str) -> Result<Option<usize>> {
        let collection = self.get_collection().await?;
        let mut cursor = collection
            .aggregate(vec![
                doc! { "$match": { self.key_field.as_str(): path } },
                doc! { "$limit": 1 },
                doc! {
                    "$project": {
                        "_id": 0,
                        "content_length": {
                            "$binarySize": format!("${}", self.value_field)
                        }
                    }
                },
            ])
            .await
            .map_err(parse_mongodb_error)?;

        if !cursor.advance().await.map_err(parse_mongodb_error)? {
            return Ok(None);
        }

        let doc: Document = cursor.deserialize_current().map_err(parse_mongodb_error)?;
        parse_bson_usize(doc.get("content_length"))
    }

    pub async fn set(&self, path: &str, value: Buffer) -> Result<()> {
        let collection = self.get_collection().await?;
        let make_update = || {
            doc! {
                "$set": {
                    self.value_field.as_str(): Binary {
                        subtype: mongodb::bson::spec::BinarySubtype::Generic,
                        bytes: value.to_vec(),
                    }
                }
            }
        };

        match collection
            .update_one(doc! { self.key_field.as_str(): path }, make_update())
            .upsert(true)
            .await
        {
            Ok(_) => Ok(()),
            // Concurrent upserts against a unique index on `key_field` can fail
            // with E11000 (MongoDB SERVER-14322); retrying once updates the
            // document inserted by the winning writer.
            Err(err) if is_duplicate_key_error(&err) => collection
                .update_one(doc! { self.key_field.as_str(): path }, make_update())
                .upsert(true)
                .await
                .map(|_| ())
                .map_err(|retry_err| {
                    let is_dup = is_duplicate_key_error(&retry_err);
                    let err = parse_mongodb_error(retry_err);
                    if is_dup { err.set_temporary() } else { err }
                }),
            Err(err) => Err(parse_mongodb_error(err)),
        }
    }

    pub async fn delete(&self, path: &str) -> Result<()> {
        let collection = self.get_collection().await?;
        let filter = doc! {self.key_field.as_str(): path};
        collection
            .delete_many(filter)
            .await
            .map_err(parse_mongodb_error)?;
        Ok(())
    }
}

fn get_binary_field<'a>(doc: &'a Document, field_path: &str) -> Result<&'a [u8]> {
    let mut segments = field_path.split('.');
    let mut current = doc;
    let mut segment = segments
        .next()
        .filter(|segment| !segment.is_empty())
        .ok_or_else(|| Error::new(ErrorKind::ConfigInvalid, "value_field is invalid"))?;

    for next in segments {
        current = current.get_document(segment).map_err(parse_bson_error)?;
        segment = next;
    }

    current
        .get_binary_generic(segment)
        .map(Vec::as_slice)
        .map_err(parse_bson_error)
}

fn is_duplicate_key_error(err: &mongodb::error::Error) -> bool {
    match err.kind.as_ref() {
        mongodb::error::ErrorKind::Write(mongodb::error::WriteFailure::WriteError(write_err)) => {
            write_err.code == DUPLICATE_KEY_ERROR_CODE
        }
        mongodb::error::ErrorKind::Command(cmd_err) => cmd_err.code == DUPLICATE_KEY_ERROR_CODE,
        _ => false,
    }
}

fn parse_mongodb_config_error(err: mongodb::error::Error) -> Error {
    match err.kind.as_ref() {
        mongodb::error::ErrorKind::InvalidArgument { .. }
        | mongodb::error::ErrorKind::InvalidTlsConfig { .. } => Error::new(
            ErrorKind::ConfigInvalid,
            "mongodb client configuration is invalid",
        )
        .set_source(err),
        _ => parse_mongodb_error(err),
    }
}

fn parse_mongodb_error(err: mongodb::error::Error) -> Error {
    let is_temporary = err.contains_label(mongodb::error::TRANSIENT_TRANSACTION_ERROR)
        || err.contains_label(mongodb::error::RETRYABLE_WRITE_ERROR)
        || matches!(
            err.kind.as_ref(),
            mongodb::error::ErrorKind::Io(_)
                | mongodb::error::ErrorKind::ConnectionPoolCleared { .. }
                | mongodb::error::ErrorKind::ServerSelection { .. }
                | mongodb::error::ErrorKind::DnsResolve { .. }
        );

    let opendal_err = Error::new(ErrorKind::Unexpected, "mongodb error").set_source(err);
    if is_temporary {
        opendal_err.set_temporary()
    } else {
        opendal_err
    }
}

fn parse_bson_error(err: mongodb::bson::document::ValueAccessError) -> Error {
    Error::new(ErrorKind::Unexpected, "bson error").set_source(err)
}

fn parse_bson_usize(value: Option<&Bson>) -> Result<Option<usize>> {
    let Some(value) = value else {
        return Ok(None);
    };

    match value {
        Bson::Int32(v) => (*v).try_into().map(Some).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "mongodb value length is invalid").set_source(err)
        }),
        Bson::Int64(v) => (*v).try_into().map(Some).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "mongodb value length is invalid").set_source(err)
        }),
        _ => Err(Error::new(
            ErrorKind::Unexpected,
            "mongodb value length is invalid",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::spec::BinarySubtype;

    #[test]
    fn test_parse_bson_usize() {
        assert_eq!(parse_bson_usize(Some(&Bson::Int32(6))).unwrap(), Some(6));
        assert_eq!(parse_bson_usize(Some(&Bson::Int64(6))).unwrap(), Some(6));
        assert_eq!(parse_bson_usize(None).unwrap(), None);
        assert!(parse_bson_usize(Some(&Bson::Int32(-1))).is_err());
        assert!(parse_bson_usize(Some(&Bson::String("6".to_string()))).is_err());
    }

    #[test]
    fn test_get_binary_field() {
        let binary = Binary {
            subtype: BinarySubtype::Generic,
            bytes: b"hello".to_vec(),
        };
        let doc = doc! {
            "value": binary.clone(),
            "payload": {
                "data": binary.clone(),
                "deep": {
                    "blob": binary,
                },
                "not_a_doc": 42_i32,
                "not_binary": "text",
            },
        };

        assert_eq!(get_binary_field(&doc, "value").unwrap(), b"hello");
        assert_eq!(get_binary_field(&doc, "payload.data").unwrap(), b"hello");
        assert_eq!(
            get_binary_field(&doc, "payload.deep.blob").unwrap(),
            b"hello"
        );

        assert_eq!(
            get_binary_field(&doc, "payload.missing")
                .unwrap_err()
                .kind(),
            ErrorKind::Unexpected
        );
        assert_eq!(
            get_binary_field(&doc, "missing.data").unwrap_err().kind(),
            ErrorKind::Unexpected
        );
        assert_eq!(
            get_binary_field(&doc, "payload.not_a_doc.leaf")
                .unwrap_err()
                .kind(),
            ErrorKind::Unexpected
        );
        assert_eq!(
            get_binary_field(&doc, "payload.not_binary")
                .unwrap_err()
                .kind(),
            ErrorKind::Unexpected
        );
    }

    #[test]
    fn test_parse_mongodb_config_error_maps_invalid_argument() {
        let invalid = ConnectionString::parse("not-a-mongodb-uri").unwrap_err();
        let err = parse_mongodb_config_error(invalid);
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    }
}
