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

#![cfg(target_os = "linux")]

use std::time::Duration;

use futures::TryStreamExt;
use opendal_core::{ErrorKind, Operator};
use opendal_service_hf3fs::Hf3fs;

fn fixture() -> (tempfile::TempDir, Operator) {
    let root = std::env::var("OPENDAL_HF3FS_ROOT")
        .expect("set OPENDAL_HF3FS_ROOT to a real 3FS test directory");
    let dir = tempfile::tempdir_in(root).unwrap();
    let mut builder = Hf3fs::default()
        .root(dir.path().to_str().unwrap())
        .queue_depth(2)
        .block_size(4096);
    if let Ok(library) = std::env::var("OPENDAL_HF3FS_LIBRARY_PATH") {
        builder = builder.library_path(&library);
    }
    (dir, Operator::new(builder).unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a real 3FS mount and USRBIO library"]
async fn read_write_ranges_and_metadata() {
    let (dir, op) = fixture();
    let data: Vec<u8> = (0..131_137).map(|i| (i % 251) as u8).collect();
    op.write("nested/data", data.clone()).await.unwrap();
    assert_eq!(std::fs::read(dir.path().join("nested/data")).unwrap(), data);
    assert_eq!(
        op.stat("nested/data").await.unwrap().content_length(),
        data.len() as u64
    );

    // Retain more than the entire shared-memory pool without stalling progress.
    let buffer = tokio::time::timeout(Duration::from_secs(10), op.read("nested/data"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(buffer.to_vec(), data);
    let mut stream = op
        .reader("nested/data")
        .await
        .unwrap()
        .into_stream(..)
        .await
        .unwrap();
    let mut streamed = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(buffer) = stream.try_next().await.unwrap() {
            streamed.extend(buffer.to_vec());
        }
    })
    .await
    .unwrap();
    assert_eq!(streamed, data);
    assert_eq!(
        op.read_with("nested/data")
            .range(17..9001)
            .await
            .unwrap()
            .to_vec(),
        data[17..9001]
    );
    assert_eq!(
        op.read_with("nested/data")
            .range(131_130..)
            .await
            .unwrap()
            .to_vec(),
        data[131_130..]
    );

    op.rename("nested/data", "nested/renamed").await.unwrap();
    assert!(
        op.list("nested/")
            .await
            .unwrap()
            .iter()
            .any(|e| e.path() == "nested/renamed")
    );
    op.delete("nested/renamed").await.unwrap();
    op.delete("nested/renamed").await.unwrap();
    assert_eq!(
        op.stat("nested/renamed").await.unwrap_err().kind(),
        ErrorKind::NotFound
    );
    assert_eq!(buffer.to_vec(), data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a real 3FS mount and USRBIO library"]
async fn retained_buffers_survive_reuse_and_operator_drop() {
    let (dir, op) = fixture();
    std::fs::write(dir.path().join("data"), vec![0x31; 4096]).unwrap();
    let first = op.read("data").await.unwrap();
    let clone = first.clone();
    let second = op.read("data").await.unwrap();
    for byte in 0..16u8 {
        op.write("data", vec![byte; 4096]).await.unwrap();
        assert_eq!(op.read("data").await.unwrap().to_vec(), vec![byte; 4096]);
    }
    drop(first);
    drop(op);
    assert_eq!(clone.to_vec(), vec![0x31; 4096]);
    assert_eq!(second.to_vec(), vec![0x31; 4096]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a real 3FS mount and USRBIO library"]
async fn concurrent_reads_and_cancelled_callers() {
    let (dir, op) = fixture();
    let data: Vec<u8> = (0..65_537).map(|i| (i % 251) as u8).collect();
    std::fs::write(dir.path().join("data"), &data).unwrap();
    let reader = op.reader("data").await.unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..64 {
        let reader = reader.clone();
        let task = tasks.spawn(async move { reader.read(0..65_537).await });
        if i % 2 == 0 {
            tokio::task::yield_now().await;
            task.abort();
        }
    }
    tokio::time::timeout(Duration::from_secs(20), async {
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(result) => assert_eq!(result.unwrap().to_vec(), data),
                Err(error) => assert!(error.is_cancelled()),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(op.read("data").await.unwrap().to_vec(), data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a real 3FS mount and USRBIO library"]
async fn overwrite_empty_and_exclusive_writes() {
    let (dir, op) = fixture();
    op.write("data", vec![0x41; 9000]).await.unwrap();
    let err = op
        .write_with("data", "replacement")
        .if_not_exists(true)
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
    assert_eq!(op.read("data").await.unwrap().len(), 9000);
    op.write("data", "short").await.unwrap();
    assert_eq!(std::fs::read(dir.path().join("data")).unwrap(), b"short");
    op.write("data", "").await.unwrap();
    assert_eq!(op.stat("data").await.unwrap().content_length(), 0);
    assert!(op.read("data").await.unwrap().is_empty());
    op.create_dir("empty/").await.unwrap();
    assert!(op.stat("empty/").await.unwrap().is_dir());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a real 3FS mount and USRBIO library"]
async fn concurrent_streaming_writes() {
    let (dir, op) = fixture();
    let mut writer = op
        .writer_with("data")
        .concurrent(4)
        .chunk(4096)
        .await
        .unwrap();
    for byte in 0..32u8 {
        writer.write(vec![byte; 4096]).await.unwrap();
    }
    writer.close().await.unwrap();
    let expected: Vec<u8> = (0..32u8).flat_map(|byte| vec![byte; 4096]).collect();
    assert_eq!(std::fs::read(dir.path().join("data")).unwrap(), expected);
    assert_eq!(op.read("data").await.unwrap().to_vec(), expected);
}
