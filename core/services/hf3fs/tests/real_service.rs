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
use opendal_core::{ErrorKind, Executor, Operator};
use opendal_executor_tokio::TokioExecutor;
use opendal_service_hf3fs::Hf3fs;

fn fixture(block_size: usize) -> (tempfile::TempDir, Operator) {
    let root = std::env::var("OPENDAL_HF3FS_ROOT")
        .expect("set OPENDAL_HF3FS_ROOT to a real 3FS test directory");
    let dir = tempfile::tempdir_in(root).unwrap();
    let mut builder = Hf3fs::default()
        .root(dir.path().to_str().unwrap())
        .queue_depth(2)
        .block_size(block_size);
    if let Ok(library) = std::env::var("OPENDAL_HF3FS_LIBRARY_PATH") {
        builder = builder.library_path(&library);
    }
    let op = Operator::new(builder).unwrap();
    let context = op
        .context()
        .with_executor(Executor::with(TokioExecutor::default()));
    (dir, op.with_context(context))
}

/// List the sorted entry names of `dir`.
fn entry_names(dir: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a real 3FS mount and USRBIO library"]
async fn read_write_ranges_and_metadata() {
    let (dir, op) = fixture(4096);
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
    let (dir, op) = fixture(4096);
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
    let (dir, op) = fixture(4096);
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
    let (dir, op) = fixture(4096);
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
    let (dir, op) = fixture(4096);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a real 3FS mount and USRBIO library"]
async fn concurrent_writes_and_cancelled_callers() {
    let (dir, op) = fixture(1024 * 1024);
    let mut tasks = tokio::task::JoinSet::new();
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(33));
    let mut cancelled = Vec::new();
    for i in 0..32 {
        let op = op.clone();
        let barrier = barrier.clone();
        let task = tasks.spawn(async move {
            let path = format!("concurrent/{i}");
            let data: Vec<u8> = (0..2 * 1024 * 1024 + 137)
                .map(|j| ((i + j) % 251) as u8)
                .collect();
            barrier.wait().await;
            op.write_with(&path, data.clone())
                .chunk(1024 * 1024)
                .concurrent(4)
                .await
                .unwrap();
            (path, data)
        });
        if i % 2 == 0 {
            cancelled.push(task);
        }
    }
    barrier.wait().await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    for task in cancelled {
        task.abort();
    }
    tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok((path, data)) => {
                    assert_eq!(std::fs::read(dir.path().join(path)).unwrap(), data);
                }
                Err(error) => assert!(error.is_cancelled()),
            }
        }
        op.write("after-cancellation", vec![0x73; 65_537])
            .await
            .unwrap();
        assert_eq!(
            op.read("after-cancellation").await.unwrap().to_vec(),
            vec![0x73; 65_537]
        );
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a real 3FS mount and USRBIO library"]
async fn conditional_write_publishes_a_complete_file_at_close() {
    let (dir, op) = fixture(4096);
    let target = dir.path().join("staged/file");
    let mut writer = op
        .writer_with("staged/file")
        .if_not_exists(true)
        .await
        .unwrap();
    let expected: Vec<u8> = (0..32u8).flat_map(|byte| vec![byte; 4096]).collect();
    for byte in 0..32u8 {
        writer.write(vec![byte; 4096]).await.unwrap();
    }

    // Readers see only the staged file while the write is in progress.
    assert!(!target.exists());
    let names = entry_names(&dir.path().join("staged"));
    assert_eq!(names.len(), 1);
    assert!(names[0].starts_with("file."));

    let meta = writer.close().await.unwrap();
    assert_eq!(meta.content_length(), expected.len() as u64);
    assert_eq!(std::fs::read(&target).unwrap(), expected);
    assert_eq!(entry_names(&dir.path().join("staged")), ["file"]);
    assert_eq!(op.read("staged/file").await.unwrap().to_vec(), expected);
    assert_eq!(
        op.stat("staged/file").await.unwrap().content_length(),
        expected.len() as u64
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a real 3FS mount and USRBIO library"]
async fn aborted_conditional_write_leaves_nothing_behind() {
    let (dir, op) = fixture(4096);
    let target = dir.path().join("staged/file");
    let mut writer = op
        .writer_with("staged/file")
        .if_not_exists(true)
        .await
        .unwrap();
    for byte in 0..32u8 {
        writer.write(vec![byte; 4096]).await.unwrap();
    }
    writer.abort().await.unwrap();
    assert!(!target.exists());
    assert!(entry_names(&dir.path().join("staged")).is_empty());

    // The aborted write must not block a later conditional write.
    let data: Vec<u8> = (0..65_537).map(|i| (i % 251) as u8).collect();
    op.write_with("staged/file", data.clone())
        .if_not_exists(true)
        .await
        .unwrap();
    assert_eq!(std::fs::read(&target).unwrap(), data);
    assert_eq!(entry_names(&dir.path().join("staged")), ["file"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a real 3FS mount and USRBIO library"]
async fn concurrent_conditional_writes_publish_exactly_one() {
    let (dir, op) = fixture(4096);
    let target = dir.path().join("staged/file");
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(8));
    let mut tasks = tokio::task::JoinSet::new();
    for byte in 0..8u8 {
        let op = op.clone();
        let barrier = barrier.clone();
        tasks.spawn(async move {
            barrier.wait().await;
            op.write_with("staged/file", vec![byte; 65_537])
                .if_not_exists(true)
                .await
        });
    }
    let mut winners = 0;
    while let Some(result) = tasks.join_next().await {
        match result.unwrap() {
            Ok(_) => winners += 1,
            Err(error) => assert_eq!(error.kind(), ErrorKind::ConditionNotMatch),
        }
    }
    assert_eq!(winners, 1);

    // The target holds the complete content of the single winner, and the
    // losers leave no staged files behind.
    let content = std::fs::read(&target).unwrap();
    assert_eq!(content.len(), 65_537);
    assert!(content.iter().all(|byte| *byte == content[0]));
    assert_eq!(entry_names(&dir.path().join("staged")), ["file"]);
}
