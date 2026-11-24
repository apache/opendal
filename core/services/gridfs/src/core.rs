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
use futures::AsyncReadExt;
use futures::AsyncWriteExt;
use futures::TryStreamExt;
use mongodb::bson::Bson;
use mongodb::bson::doc;
use mongodb::gridfs::{FilesCollectionDocument, GridFsBucket};
use mongodb::options::ClientOptions;
use mongodb::options::ConnectionString;
use mongodb::options::GridFsBucketOptions;
use opendal_core::raw::*;
use opendal_core::*;

#[derive(Clone)]
pub struct GridfsCore {
    pub connection_string: ConnectionString,
    pub database: String,
    pub bucket: String,
    pub chunk_size: u32,
    pub bucket_instance: OnceCell<GridFsBucket>,
}

impl Debug for GridfsCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GridfsCore")
            .field("database", &self.database)
            .field("bucket", &self.bucket)
            .field("chunk_size", &self.chunk_size)
            .finish_non_exhaustive()
    }
}

impl GridfsCore {
    async fn get_bucket(&self) -> Result<&GridFsBucket> {
        self.bucket_instance
            .get_or_try_init(|| async {
                let client_options = ClientOptions::parse(self.connection_string.clone())
                    .await
                    .map_err(parse_mongodb_config_error)?;
                let client = mongodb::Client::with_options(client_options)
                    .map_err(parse_mongodb_config_error)?;
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

    async fn get_one_doc(
        bucket: &GridFsBucket,
        path: &str,
    ) -> Result<Option<FilesCollectionDocument>> {
        let filter = doc! { "filename": path };
        let doc = bucket
            .find_one(filter)
            .sort(doc! { "uploadDate": -1, "_id": -1 })
            .await
            .map_err(parse_mongodb_error)?;
        Ok(doc)
    }

    async fn delete_doc_by_id(bucket: &GridFsBucket, file_id: Bson) -> Result<()> {
        match bucket.delete(file_id).await {
            Ok(()) => Ok(()),
            Err(err) if is_gridfs_file_not_found(&err) => Ok(()),
            Err(err) => Err(parse_mongodb_error(err)),
        }
    }

    async fn delete_superseded_docs(bucket: &GridFsBucket, path: &str) -> Result<()> {
        let mut cursor = bucket
            .find(doc! { "filename": path })
            .sort(doc! { "uploadDate": -1, "_id": -1 })
            .await
            .map_err(parse_mongodb_error)?;

        // Keep the newest revision under `(uploadDate, _id)` so concurrent writers
        // cannot delete each other's latest revision.
        if cursor
            .try_next()
            .await
            .map_err(parse_mongodb_error)?
            .is_none()
        {
            return Ok(());
        }

        while let Some(doc) = cursor.try_next().await.map_err(parse_mongodb_error)? {
            Self::delete_doc_by_id(bucket, doc.id).await?;
        }

        Ok(())
    }

    pub async fn get(&self, path: &str) -> Result<Option<Buffer>> {
        let bucket = self.get_bucket().await?;
        for _ in 0..2 {
            let Some(doc) = Self::get_one_doc(bucket, path).await? else {
                return Ok(None);
            };

            let file_id = doc.id;
            let mut stream = match bucket.open_download_stream(file_id.clone()).await {
                Ok(stream) => stream,
                Err(err) if is_gridfs_file_not_found(&err) => continue,
                Err(err) => return Err(parse_mongodb_error(err)),
            };
            let mut destination = Vec::new();
            match stream.read_to_end(&mut destination).await {
                Ok(_) => return Ok(Some(Buffer::from(destination))),
                Err(err) => {
                    if extract_mongodb_error(&err).is_some_and(is_gridfs_missing_chunk)
                        && bucket
                            .find_one(doc! { "_id": &file_id })
                            .await
                            .map_err(parse_mongodb_error)?
                            .is_none()
                    {
                        continue;
                    }
                    return Err(parse_gridfs_io_error(err));
                }
            }
        }

        Ok(None)
    }

    /// Get the byte length of the file.
    pub async fn get_length(&self, path: &str) -> Result<Option<u64>> {
        let bucket = self.get_bucket().await?;
        let Some(doc) = Self::get_one_doc(bucket, path).await? else {
            return Ok(None);
        };

        Ok(Some(doc.length))
    }

    pub async fn set(&self, path: &str, value: Buffer) -> Result<()> {
        let bucket = self.get_bucket().await?;

        // Upload the new revision before cleaning up older revisions so a failed
        // upload does not lose the previous version.
        let mut upload_stream = bucket
            .open_upload_stream(path)
            .await
            .map_err(parse_mongodb_error)?;
        for chunk in value {
            if let Err(err) = upload_stream.write_all(&chunk).await {
                let _ = upload_stream.abort().await;
                return Err(parse_gridfs_io_error(err));
            }
        }
        upload_stream.close().await.map_err(parse_gridfs_io_error)?;

        Self::delete_superseded_docs(bucket, path).await?;

        Ok(())
    }

    pub async fn delete(&self, path: &str) -> Result<()> {
        let bucket = self.get_bucket().await?;
        let mut cursor = bucket
            .find(doc! { "filename": path })
            .await
            .map_err(parse_mongodb_error)?;

        while let Some(doc) = cursor.try_next().await.map_err(parse_mongodb_error)? {
            Self::delete_doc_by_id(bucket, doc.id).await?;
        }

        Ok(())
    }
}

fn is_gridfs_file_not_found(err: &mongodb::error::Error) -> bool {
    matches!(
        err.kind.as_ref(),
        mongodb::error::ErrorKind::GridFs(mongodb::error::GridFsErrorKind::FileNotFound { .. })
    )
}

fn is_gridfs_missing_chunk(err: &mongodb::error::Error) -> bool {
    matches!(
        err.kind.as_ref(),
        mongodb::error::ErrorKind::GridFs(mongodb::error::GridFsErrorKind::MissingChunk { .. })
    )
}

fn extract_mongodb_error(err: &std::io::Error) -> Option<&mongodb::error::Error> {
    err.get_ref()
        .and_then(|inner| inner.downcast_ref::<mongodb::error::Error>())
}

fn parse_gridfs_io_error(err: std::io::Error) -> Error {
    if extract_mongodb_error(&err).is_some() {
        let mongo_err = err
            .into_inner()
            .expect("mongodb error verified via get_ref")
            .downcast::<mongodb::error::Error>()
            .expect("mongodb error verified via downcast_ref");
        return parse_mongodb_error(*mongo_err);
    }
    new_std_io_error(err)
}

fn is_transient_io_error(err: &std::io::Error) -> bool {
    matches!(
        err.kind(),
        std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::TimedOut
            | std::io::ErrorKind::Interrupted
            | std::io::ErrorKind::WouldBlock
            | std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::NotConnected
            | std::io::ErrorKind::NetworkUnreachable
            | std::io::ErrorKind::HostUnreachable
    )
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
    if matches!(
        err.kind.as_ref(),
        mongodb::error::ErrorKind::GridFs(
            mongodb::error::GridFsErrorKind::FileNotFound { .. }
                | mongodb::error::GridFsErrorKind::RevisionNotFound { .. }
        )
    ) {
        return Error::new(ErrorKind::NotFound, "gridfs file not found").set_source(err);
    }

    let is_temporary = err.contains_label(mongodb::error::TRANSIENT_TRANSACTION_ERROR)
        || err.contains_label(mongodb::error::RETRYABLE_WRITE_ERROR)
        || match err.kind.as_ref() {
            mongodb::error::ErrorKind::Io(io_err) => is_transient_io_error(io_err),
            mongodb::error::ErrorKind::ConnectionPoolCleared { .. }
            | mongodb::error::ErrorKind::ServerSelection { .. }
            | mongodb::error::ErrorKind::DnsResolve { .. } => true,
            _ => false,
        };

    let opendal_err = Error::new(ErrorKind::Unexpected, "mongodb error").set_source(err);
    if is_temporary {
        opendal_err.set_temporary()
    } else {
        opendal_err
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::error::GridFsErrorKind;

    #[test]
    fn test_parse_gridfs_io_error_unwraps_mongodb_error() {
        let mongo_closed: mongodb::error::Error =
            mongodb::error::ErrorKind::GridFs(GridFsErrorKind::UploadStreamClosed).into();
        let err = parse_gridfs_io_error(std::io::Error::other(mongo_closed));
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!err.is_temporary());

        let mongo_transient = mongodb::error::Error::from(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset",
        ));
        let err = parse_gridfs_io_error(std::io::Error::other(mongo_transient));
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(err.is_temporary());

        let plain_io = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let err = parse_gridfs_io_error(plain_io);
        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
    }

    #[test]
    fn test_parse_mongodb_config_error_maps_invalid_argument() {
        let invalid = ConnectionString::parse("not-a-mongodb-uri").unwrap_err();
        let err = parse_mongodb_config_error(invalid);
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    }

    #[test]
    fn test_parse_mongodb_error_io_temporary_classification() {
        for transient_kind in [
            std::io::ErrorKind::ConnectionReset,
            std::io::ErrorKind::ConnectionAborted,
            std::io::ErrorKind::BrokenPipe,
            std::io::ErrorKind::TimedOut,
            std::io::ErrorKind::Interrupted,
            std::io::ErrorKind::WouldBlock,
            std::io::ErrorKind::UnexpectedEof,
            std::io::ErrorKind::ConnectionRefused,
            std::io::ErrorKind::NotConnected,
            std::io::ErrorKind::NetworkUnreachable,
            std::io::ErrorKind::HostUnreachable,
        ] {
            let err = parse_mongodb_error(mongodb::error::Error::from(std::io::Error::new(
                transient_kind,
                "transient io",
            )));
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(
                err.is_temporary(),
                "expected {transient_kind:?} to be temporary"
            );
        }

        for permanent_kind in [
            std::io::ErrorKind::InvalidData,
            std::io::ErrorKind::InvalidInput,
            std::io::ErrorKind::PermissionDenied,
            std::io::ErrorKind::NotFound,
            std::io::ErrorKind::Other,
        ] {
            let err = parse_mongodb_error(mongodb::error::Error::from(std::io::Error::new(
                permanent_kind,
                "permanent io",
            )));
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(
                !err.is_temporary(),
                "expected {permanent_kind:?} to be permanent"
            );
        }
    }

    #[tokio::test]
    #[ignore = "requires a running MongoDB server (OPENDAL_GRIDFS_CONNECTION_STRING)"]
    async fn test_live_parse_mongodb_error_maps_gridfs_file_not_found() {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let conn = std::env::var("OPENDAL_GRIDFS_CONNECTION_STRING")
                .unwrap_or_else(|_| "mongodb://127.0.0.1:27017".to_string());
            let client = mongodb::Client::with_uri_str(&conn).await.unwrap();
            let bucket = client
                .database("opendal_gridfs_err_test")
                .gridfs_bucket(None);

            let missing_id = Bson::ObjectId(mongodb::bson::oid::ObjectId::new());
            let delete_err = bucket.delete(missing_id.clone()).await.unwrap_err();
            assert!(is_gridfs_file_not_found(&delete_err));
            let parsed = parse_mongodb_error(delete_err);
            assert_eq!(parsed.kind(), ErrorKind::NotFound);
            assert!(!parsed.is_temporary());

            let download_err = bucket
                .open_download_stream(missing_id)
                .await
                .err()
                .expect("missing id download must fail");
            assert!(is_gridfs_file_not_found(&download_err));
            let parsed = parse_mongodb_error(download_err);
            assert_eq!(parsed.kind(), ErrorKind::NotFound);

            let by_name_err = bucket
                .open_download_stream_by_name("definitely-missing-file")
                .await
                .err()
                .expect("missing filename download must fail");
            assert!(is_gridfs_file_not_found(&by_name_err));
            let parsed = parse_mongodb_error(by_name_err);
            assert_eq!(parsed.kind(), ErrorKind::NotFound);

            let _ = client.database("opendal_gridfs_err_test").drop().await;
        })
        .await
        .expect("test timed out");
    }
}
