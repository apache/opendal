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
use std::time::Duration;

use futures::AsyncWriteExt;
use mongodb::bson::Bson;
use mongodb::bson::DateTime;
use mongodb::bson::Document;
use mongodb::bson::doc;
use mongodb::bson::oid::ObjectId;
use mongodb::bson::spec::BinarySubtype;
use mongodb::options::GridFsBucketOptions;
use opendal_core::Builder;
use opendal_core::EntryMode;
use opendal_core::ErrorKind;
use opendal_core::OperationContext;
use opendal_core::Operator;
use opendal_core::Result;
use opendal_core::layers;
use opendal_core::raw::OpStat;
use opendal_core::raw::OpWrite;
use opendal_core::raw::Service;
use opendal_core::raw::Servicer;
use opendal_core::raw::oio::Write as _;
use opendal_service_gridfs::Gridfs;

fn connection_string() -> String {
    std::env::var("OPENDAL_GRIDFS_CONNECTION_STRING")
        .unwrap_or_else(|_| "mongodb://127.0.0.1:27017".to_string())
}

async fn upload_raw_revision(
    bucket: &mongodb::gridfs::GridFsBucket,
    filename: &str,
    data: &[u8],
) -> Bson {
    let mut stream = bucket.open_upload_stream(filename).await.unwrap();
    let id = stream.id().clone();
    stream.write_all(data).await.unwrap();
    stream.close().await.unwrap();
    id
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
        upload_raw_revision(&bucket, "rev.txt", b"older-v1").await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        upload_raw_revision(&bucket, "rev.txt", b"newer-v2-payload-longer").await;

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

        // delete must remove ALL matching revisions and their chunks.
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

        // Same-millisecond tie-breaker: `_id: -1` selects the higher `_id`.
        let ts = DateTime::now();
        let lower_id = ObjectId::from_bytes([0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1]);
        let higher_id = ObjectId::from_bytes([0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2]);
        for (id, payload) in [
            (lower_id, b"tie-loser".as_slice()),
            (higher_id, b"tie-winner-v2".as_slice()),
        ] {
            chunks_coll
                .insert_one(doc! {
                    "files_id": id,
                    "n": 0_i32,
                    "data": mongodb::bson::Binary {
                        subtype: BinarySubtype::Generic,
                        bytes: payload.to_vec(),
                    },
                })
                .await
                .unwrap();
            files_coll
                .insert_one(doc! {
                    "_id": id,
                    "filename": "tie.txt",
                    "length": payload.len() as i64,
                    "chunkSize": 16_i32,
                    "uploadDate": ts,
                })
                .await
                .unwrap();
        }
        assert_eq!(op.read("tie.txt").await?.to_bytes(), &b"tie-winner-v2"[..]);
        assert_eq!(
            op.stat("tie.txt").await?.content_length(),
            b"tie-winner-v2".len() as u64
        );

        // Overwrite cleans up all superseded revisions, leaving only 1 document.
        op.write("tie.txt", "replaced-v3").await?;
        assert_eq!(op.read("tie.txt").await?.to_bytes(), &b"replaced-v3"[..]);
        assert_eq!(
            files_coll
                .count_documents(doc! { "filename": "tie.txt" })
                .await
                .unwrap(),
            1
        );

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

        // Add a server-side collection validator that rejects new files longer than 64 bytes.
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

        // Previous revision must remain intact because upload happens before cleanup.
        assert_eq!(op.read("safe.txt").await?.to_bytes(), &b"original-data"[..]);

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
                        Ok(buf) => assert!(!buf.is_empty()),
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
async fn test_live_root_and_dir_marker_stat_and_writer_metadata() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        let conn = connection_string();
        let db_name = format!("opendal_gridfs_dir_{}", ObjectId::new().to_hex());

        let client = mongodb::Client::with_uri_str(&conn).await.unwrap();
        let db = client.database(&db_name);

        let service = Gridfs::default()
            .connection_string(&conn)
            .database(&db_name)
            .bucket("fs")
            .build()?;
        let ctx = OperationContext::default();

        assert_eq!(
            service
                .stat(&ctx, "/", OpStat::default())
                .await?
                .into_metadata()
                .mode(),
            EntryMode::DIR
        );

        let mut root_writer = service.write(&ctx, "/", OpWrite::default())?;
        let root_meta = root_writer.close().await?;
        assert_eq!(root_meta.mode(), EntryMode::DIR);
        assert_eq!(
            service
                .stat(&ctx, "/", OpStat::default())
                .await?
                .into_metadata()
                .mode(),
            EntryMode::DIR
        );

        let mut dir_writer = service.write(&ctx, "a/", OpWrite::default())?;
        let dir_meta = dir_writer.close().await?;
        assert_eq!(dir_meta.mode(), EntryMode::DIR);
        assert_eq!(
            service
                .stat(&ctx, "a/", OpStat::default())
                .await?
                .into_metadata()
                .mode(),
            EntryMode::DIR
        );

        let mut file_writer = service.write(&ctx, "a/file.txt", OpWrite::default())?;
        file_writer
            .write(opendal_core::Buffer::from("hello"))
            .await?;
        let file_meta = file_writer.close().await?;
        assert_eq!(file_meta.mode(), EntryMode::FILE);
        assert_eq!(file_meta.content_length(), 5);

        let stat_meta = service
            .stat(&ctx, "a/file.txt", OpStat::default())
            .await?
            .into_metadata();
        assert_eq!(stat_meta.mode(), EntryMode::FILE);
        assert_eq!(stat_meta.content_length(), 5);

        // Verify Operator with SimulateLayer::with_stat_dir(false) also observes DIR.
        let srv: Servicer = Arc::new(service);
        let op = Operator::from_parts(OperationContext::default(), srv)
            .layer(layers::SimulateLayer::default().with_stat_dir(false));
        assert_eq!(op.stat("/").await?.mode(), EntryMode::DIR);
        assert_eq!(op.stat("a/").await?.mode(), EntryMode::DIR);
        assert_eq!(op.stat("a/file.txt").await?.mode(), EntryMode::FILE);

        let _ = db.drop().await;
        Ok(())
    })
    .await
    .expect("test timed out")
}
