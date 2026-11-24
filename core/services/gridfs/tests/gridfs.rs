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

use std::time::Duration;

use futures::AsyncWriteExt;
use mongodb::bson::DateTime;
use mongodb::bson::Document;
use mongodb::bson::doc;
use mongodb::bson::oid::ObjectId;
use mongodb::options::GridFsBucketOptions;
use opendal_core::EntryMode;
use opendal_core::ErrorKind;
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_gridfs::Gridfs;

fn connection_string() -> String {
    std::env::var("OPENDAL_GRIDFS_CONNECTION_STRING")
        .unwrap_or_else(|_| "mongodb://127.0.0.1:27017".to_string())
}

async fn upload_revision(bucket: &mongodb::gridfs::GridFsBucket, filename: &str, data: &[u8]) {
    let mut stream = bucket.open_upload_stream(filename).await.unwrap();
    stream.write_all(data).await.unwrap();
    stream.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a running MongoDB server (OPENDAL_GRIDFS_CONNECTION_STRING)"]
async fn test_live_duplicate_revisions_read_newest_and_delete_all() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        let conn = connection_string();
        let db_name = format!("opendal_gridfs_dup_{}", ObjectId::new().to_hex());
        let bucket_name = "fs";

        let client = mongodb::Client::with_uri_str(&conn).await.unwrap();
        let db = client.database(&db_name);
        let bucket = db.gridfs_bucket(
            GridFsBucketOptions::builder()
                .bucket_name(Some(bucket_name.to_string()))
                .chunk_size_bytes(Some(16))
                .build(),
        );

        let op = Operator::new(
            Gridfs::default()
                .connection_string(&conn)
                .database(&db_name)
                .bucket(bucket_name)
                .chunk_size(16),
        )?;

        // Upload two revisions with distinct uploadDate values.
        upload_revision(&bucket, "rev.txt", b"older-v1").await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        upload_revision(&bucket, "rev.txt", b"newer-v2-payload-longer").await;

        let files_coll = db.collection::<Document>(&format!("{bucket_name}.files"));
        let chunks_coll = db.collection::<Document>(&format!("{bucket_name}.chunks"));
        assert_eq!(
            files_coll
                .count_documents(doc! { "filename": "rev.txt" })
                .await
                .unwrap(),
            2
        );

        // stat and read must return the newest revision, not the oldest.
        let meta = op.stat("rev.txt").await?;
        assert_eq!(meta.mode(), EntryMode::FILE);
        assert_eq!(
            meta.content_length(),
            b"newer-v2-payload-longer".len() as u64
        );
        assert_eq!(
            op.read("rev.txt").await?.to_bytes(),
            &b"newer-v2-payload-longer"[..]
        );

        // Give the two real SDK uploads the same timestamp to exercise the ID tie-breaker.
        files_coll
            .update_many(
                doc! { "filename": "rev.txt" },
                doc! { "$set": { "uploadDate": DateTime::now() } },
            )
            .await
            .unwrap();
        assert_eq!(
            op.read("rev.txt").await?.to_bytes(),
            &b"newer-v2-payload-longer"[..]
        );

        // Delete must remove both revisions and their chunks.
        op.delete("rev.txt").await?;
        assert_eq!(
            files_coll
                .count_documents(doc! { "filename": "rev.txt" })
                .await
                .unwrap(),
            0
        );
        assert_eq!(chunks_coll.count_documents(doc! {}).await.unwrap(), 0);
        assert_eq!(
            op.stat("rev.txt").await.unwrap_err().kind(),
            ErrorKind::NotFound
        );
        assert_eq!(
            op.read("rev.txt").await.unwrap_err().kind(),
            ErrorKind::NotFound
        );

        // A subsequent overwrite cleans up multiple already committed revisions.
        upload_revision(&bucket, "rev.txt", b"older-before-overwrite").await;
        upload_revision(&bucket, "rev.txt", b"newer-before-overwrite").await;
        op.write("rev.txt", "replaced-v3").await?;
        assert_eq!(op.read("rev.txt").await?.to_bytes(), &b"replaced-v3"[..]);
        assert_eq!(
            files_coll
                .count_documents(doc! { "filename": "rev.txt" })
                .await
                .unwrap(),
            1
        );
        assert_eq!(chunks_coll.count_documents(doc! {}).await.unwrap(), 1);

        let _ = db.drop().await;
        Ok(())
    })
    .await
    .expect("test timed out")
}

#[tokio::test]
#[ignore = "requires a running MongoDB server (OPENDAL_GRIDFS_CONNECTION_STRING)"]
async fn test_live_upload_failure_preserves_existing_revision() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        let conn = connection_string();
        let db_name = format!("opendal_gridfs_fail_{}", ObjectId::new().to_hex());
        let bucket_name = "fs";

        let client = mongodb::Client::with_uri_str(&conn).await.unwrap();
        let db = client.database(&db_name);

        let op = Operator::new(
            Gridfs::default()
                .connection_string(&conn)
                .database(&db_name)
                .bucket(bucket_name)
                .chunk_size(32),
        )?;

        op.write("safe.txt", "original-data").await?;
        assert_eq!(op.read("safe.txt").await?.to_bytes(), &b"original-data"[..]);

        let chunks = db.collection::<Document>("fs.chunks");
        let original_chunks = chunks.count_documents(doc! {}).await.unwrap();

        // Reject the later chunks after accepting the first chunks of the upload.
        db.run_command(doc! {
            "collMod": "fs.chunks",
            "validator": { "n": { "$lt": 2 } },
        })
        .await
        .unwrap();
        let err = op
            .write("safe.txt", vec![b'x'; 128])
            .await
            .expect_err("chunk validator must reject upload");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert_eq!(op.read("safe.txt").await?.to_bytes(), &b"original-data"[..]);
        assert_eq!(
            chunks.count_documents(doc! {}).await.unwrap(),
            original_chunks
        );

        db.run_command(doc! { "collMod": "fs.chunks", "validator": {} })
            .await
            .unwrap();

        // Reject the files document after writing the chunks.
        db.run_command(doc! {
            "collMod": format!("{bucket_name}.files"),
            "validator": { "length": { "$lte": 64 } },
        })
        .await
        .unwrap();

        let err = op
            .write("safe.txt", vec![b'x'; 128])
            .await
            .expect_err("validator must reject oversized upload");
        assert_eq!(err.kind(), ErrorKind::Unexpected);

        assert_eq!(op.read("safe.txt").await?.to_bytes(), &b"original-data"[..]);
        assert_eq!(
            chunks.count_documents(doc! {}).await.unwrap(),
            original_chunks
        );

        let _ = db.drop().await;
        Ok(())
    })
    .await
    .expect("test timed out")
}

#[tokio::test]
#[ignore = "requires a running MongoDB server (OPENDAL_GRIDFS_CONNECTION_STRING)"]
async fn test_live_concurrent_writes_reads_and_deletes() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(60), async {
        let conn = connection_string();
        let db_name = format!("opendal_gridfs_conc_{}", ObjectId::new().to_hex());
        let bucket_name = "fs";

        let client = mongodb::Client::with_uri_str(&conn).await.unwrap();
        let db = client.database(&db_name);

        let op = Operator::new(
            Gridfs::default()
                .connection_string(&conn)
                .database(&db_name)
                .bucket(bucket_name)
                .chunk_size(16),
        )?;

        // Phase A: Concurrent overwrites must never leave 0 documents or fail readers with Unexpected.
        op.write("concurrent_write.txt", vec![b'0'; 48]).await?;
        let mut handles = Vec::new();
        for worker in 0..6u8 {
            let op = op.clone();
            handles.push(tokio::spawn(async move {
                for iter in 0..10u8 {
                    let byte = b'a' + ((worker + iter) % 26);
                    let payload = vec![byte; 40 + usize::from(iter)];
                    op.write("concurrent_write.txt", payload).await?;
                }
                Ok::<_, opendal_core::Error>(())
            }));
        }
        for _ in 0..3 {
            let op = op.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..15 {
                    match op.read("concurrent_write.txt").await {
                        Ok(buf) => {
                            let bytes = buf.to_bytes();
                            assert!((40..=49).contains(&bytes.len()));
                            assert!(bytes.iter().all(|byte| *byte == bytes[0]));
                        }
                        Err(err) if err.kind() == ErrorKind::NotFound => {}
                        Err(err) => return Err(err),
                    }
                    match op.stat("concurrent_write.txt").await {
                        Ok(meta) => assert_eq!(meta.mode(), EntryMode::FILE),
                        Err(err) if err.kind() == ErrorKind::NotFound => {}
                        Err(err) => return Err(err),
                    }
                }
                Ok(())
            }));
        }
        for handle in handles {
            handle.await.unwrap()?;
        }

        let files_coll = db.collection::<Document>(&format!("{bucket_name}.files"));
        assert_eq!(
            files_coll
                .count_documents(doc! { "filename": "concurrent_write.txt" })
                .await
                .unwrap(),
            1,
            "concurrent overwrites must keep exactly one surviving revision"
        );
        assert!(!op.read("concurrent_write.txt").await?.is_empty());

        // Phase B: Concurrent writes + deletes + reads must never fail with Unexpected (FileNotFound).
        let mut churn_handles = Vec::new();
        for worker in 0..4u8 {
            let op = op.clone();
            churn_handles.push(tokio::spawn(async move {
                for iter in 0..15u8 {
                    let payload = vec![b'A' + ((worker + iter) % 26); 48];
                    op.write("concurrent_churn.txt", payload).await?;
                }
                Ok::<_, opendal_core::Error>(())
            }));
        }
        for _ in 0..4 {
            let op = op.clone();
            churn_handles.push(tokio::spawn(async move {
                for _ in 0..15 {
                    op.delete("concurrent_churn.txt").await?;
                }
                Ok(())
            }));
        }
        for _ in 0..4 {
            let op = op.clone();
            churn_handles.push(tokio::spawn(async move {
                for _ in 0..15 {
                    match op.read("concurrent_churn.txt").await {
                        Ok(_) => {}
                        Err(err) if err.kind() == ErrorKind::NotFound => {}
                        Err(err) => return Err(err),
                    }
                }
                Ok(())
            }));
        }
        for handle in churn_handles {
            handle.await.unwrap()?;
        }

        op.delete("concurrent_churn.txt").await?;
        assert_eq!(
            files_coll
                .count_documents(doc! { "filename": "concurrent_churn.txt" })
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            op.stat("concurrent_churn.txt").await.unwrap_err().kind(),
            ErrorKind::NotFound
        );

        let _ = db.drop().await;
        Ok(())
    })
    .await
    .expect("test timed out")
}

#[tokio::test]
#[ignore = "requires a running MongoDB server (OPENDAL_GRIDFS_CONNECTION_STRING)"]
async fn test_live_stat_reports_directory_marker() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        let conn = connection_string();
        let db_name = format!("opendal_gridfs_dir_{}", ObjectId::new().to_hex());
        let client = mongodb::Client::with_uri_str(&conn).await.unwrap();
        let db = client.database(&db_name);
        let bucket = db.gridfs_bucket(None);
        upload_revision(&bucket, "a/", b"").await;

        let op = Operator::new(
            Gridfs::default()
                .connection_string(&conn)
                .database(&db_name),
        )?;
        assert_eq!(op.stat("a/").await?.mode(), EntryMode::DIR);
        assert_eq!(
            op.stat("missing/").await.unwrap_err().kind(),
            ErrorKind::NotFound
        );

        let _ = db.drop().await;
        Ok(())
    })
    .await
    .expect("test timed out")
}
