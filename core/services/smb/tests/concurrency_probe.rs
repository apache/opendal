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

// Task-only probe for OpenDAL SMB parent-directory creation.
//
// This file is carried only on a task-only Git branch. The main coordinator
// places it at:
//
//   core/services/smb/tests/concurrency_probe.rs
//
// in a task-only Git branch. It uses only the public Operator API and existing
// Samba configuration variables. It does not change production code or PR 8350.

use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use opendal_core::Configurator;
use opendal_core::Operator;
use opendal_service_smb::SmbConfig;
use tokio::sync::Barrier;

fn config(root: &str) -> SmbConfig {
    let mut config = SmbConfig::from_iter(std::env::vars().filter_map(|(key, value)| {
        key.strip_prefix("OPENDAL_SMB_")
            .map(|key| (key.to_ascii_lowercase(), value))
    }))
    .expect("OPENDAL_SMB_* configuration must be present");
    config.root = Some(root.to_string());
    config
}

fn unique_root(label: &str) -> String {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after the Unix epoch")
        .as_nanos();
    format!("/probe-8350-{}-{label}-{stamp}/", std::process::id())
}

fn operator(root: &str) -> Operator {
    let op = Operator::new(config(root).into_builder()).expect("SMB operator must build");
    assert_eq!(op.info().scheme(), "smb");
    op
}

async fn warm(op: &Operator) {
    // A missing stat initializes the client without creating the probe root.
    let _ = op.stat("warmup").await;
}

async fn cleanup(op: &Operator, paths: &[&str]) {
    for path in paths {
        let _ = op.delete(path).await;
    }
    let _ = op.delete("/").await;
}

async fn run_single_write(label: &str) {
    let root = unique_root(label);
    let op = operator(&root);
    warm(&op).await;

    let result = op.write("parent/a", "a").await;
    println!(
        "case={label} root={root} single={:?}",
        result.as_ref().err().map(ToString::to_string)
    );

    cleanup(&op, &["parent/a", "parent/"]).await;
    assert!(result.is_ok(), "single write failed: case={label}");
}

async fn run_single_create_dir(label: &str) {
    let root = unique_root(label);
    let op = operator(&root);
    warm(&op).await;

    let result = op.create_dir("parent/a/").await;
    println!(
        "case={label} root={root} single={:?}",
        result.as_ref().err().map(ToString::to_string)
    );

    cleanup(&op, &["parent/a/", "parent/"]).await;
    assert!(result.is_ok(), "single create_dir failed: case={label}");
}

async fn run_write_pair(label: &str, precreate_parent: bool) {
    let root = unique_root(label);
    let op1 = operator(&root);
    let op2 = operator(&root);
    warm(&op1).await;
    warm(&op2).await;

    if precreate_parent {
        op1.create_dir("parent/")
            .await
            .expect("precreating parent must succeed");
    }

    let cleanup_op = op1.clone();
    let barrier = Arc::new(Barrier::new(2));
    let barrier1 = barrier.clone();
    let barrier2 = barrier.clone();

    let task1 = tokio::spawn(async move {
        barrier1.wait().await;
        op1.write("parent/a", "a").await
    });
    let task2 = tokio::spawn(async move {
        barrier2.wait().await;
        op2.write("parent/b", "b").await
    });

    let r1 = task1.await.expect("write task a must not panic");
    let r2 = task2.await.expect("write task b must not panic");
    println!(
        "case={label} root={root} precreate_parent={precreate_parent} a={:?} b={:?}",
        r1.as_ref().err().map(ToString::to_string),
        r2.as_ref().err().map(ToString::to_string)
    );

    cleanup(&cleanup_op, &["parent/a", "parent/b", "parent/"]).await;
    assert!(
        r1.is_ok() && r2.is_ok(),
        "concurrent write pair failed: case={label} precreate_parent={precreate_parent}"
    );
}

async fn run_create_dir_pair(label: &str, precreate_parent: bool) {
    let root = unique_root(label);
    let op1 = operator(&root);
    let op2 = operator(&root);
    warm(&op1).await;
    warm(&op2).await;

    if precreate_parent {
        op1.create_dir("parent/")
            .await
            .expect("precreating parent must succeed");
    }

    let barrier = Arc::new(Barrier::new(2));
    let barrier1 = barrier.clone();
    let barrier2 = barrier.clone();

    let task1 = tokio::spawn(async move {
        barrier1.wait().await;
        op1.create_dir("parent/a/").await
    });
    let task2 = tokio::spawn(async move {
        barrier2.wait().await;
        op2.create_dir("parent/b/").await
    });

    let r1 = task1.await.expect("create-dir task a must not panic");
    let r2 = task2.await.expect("create-dir task b must not panic");
    println!(
        "case={label} root={root} precreate_parent={precreate_parent} a={:?} b={:?}",
        r1.as_ref().err().map(ToString::to_string),
        r2.as_ref().err().map(ToString::to_string)
    );

    // The task runner observes the server's directory entries before teardown.
    // Keep the unique tree intact on both success and failure.
    assert!(
        r1.is_ok() && r2.is_ok(),
        "concurrent create-dir pair failed: case={label} precreate_parent={precreate_parent}"
    );
}

#[tokio::test]
#[ignore = "requires the Samba fixture and OPENDAL_SMB_* configuration"]
async fn single_write_with_missing_parent() {
    run_single_write("single-write").await;
}

#[tokio::test]
#[ignore = "requires the Samba fixture and OPENDAL_SMB_* configuration"]
async fn single_create_dir_with_missing_parent() {
    run_single_create_dir("single-mkdir").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the Samba fixture and OPENDAL_SMB_* configuration"]
async fn concurrent_writes_with_missing_parent() {
    run_write_pair("write-missing", false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the Samba fixture and OPENDAL_SMB_* configuration"]
async fn concurrent_writes_with_precreated_parent() {
    run_write_pair("write-precreated", true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the Samba fixture and OPENDAL_SMB_* configuration"]
async fn concurrent_create_dir_with_missing_parent() {
    run_create_dir_pair("mkdir-missing", false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the Samba fixture and OPENDAL_SMB_* configuration"]
async fn concurrent_create_dir_with_precreated_parent() {
    run_create_dir_pair("mkdir-precreated", true).await;
}

fn assert_server_not_a_directory(error: &opendal_core::Error) {
    assert_eq!(error.kind(), opendal_core::ErrorKind::NotADirectory);
    let source = std::error::Error::source(error)
        .and_then(|source| source.downcast_ref::<smb::Error>())
        .expect("NotADirectory must originate from the SMB server response");
    let status = match source {
        smb::Error::UnexpectedMessageStatus(status)
        | smb::Error::ReceivedErrorMessage(status, _) => *status,
        other => panic!("expected a server status, got {other:?}"),
    };
    println!(
        "negative-control kind={:?} server_status={status:#010x}",
        error.kind()
    );
    assert_eq!(status, 0xC0000103);
}

#[tokio::test]
#[ignore = "requires the Samba fixture and OPENDAL_SMB_* configuration"]
async fn create_dir_rejects_target_file() {
    let op = operator(&unique_root("target-file"));
    op.write("target", "keep-target-file")
        .await
        .expect("file setup must succeed");
    let result = op.create_dir("target/").await;
    let data = op
        .read("target")
        .await
        .expect("target file must remain readable")
        .to_vec();
    cleanup(&op, &["target"]).await;
    assert_eq!(data.as_slice(), b"keep-target-file");
    assert_server_not_a_directory(&result.expect_err("an ordinary target file must be rejected"));
}

#[tokio::test]
#[ignore = "requires the Samba fixture and OPENDAL_SMB_* configuration"]
async fn create_dir_rejects_intermediate_file() {
    let op = operator(&unique_root("intermediate-file"));
    op.write("parent", "keep-parent-file")
        .await
        .expect("file setup must succeed");
    let result = op.create_dir("parent/child/").await;
    let data = op
        .read("parent")
        .await
        .expect("parent file must remain readable")
        .to_vec();
    cleanup(&op, &["parent"]).await;
    assert_eq!(data.as_slice(), b"keep-parent-file");
    assert_server_not_a_directory(
        &result.expect_err("an ordinary intermediate file must be rejected"),
    );
}

#[tokio::test]
#[ignore = "requires the Samba fixture and OPENDAL_SMB_* configuration"]
async fn repeated_create_dir_is_idempotent() {
    let op = operator(&unique_root("idempotent-directory"));
    op.create_dir("parent/child/")
        .await
        .expect("first create must succeed");
    op.create_dir("parent/child/")
        .await
        .expect("repeated create must succeed");
    op.write("parent/child/proof", "directory-proof")
        .await
        .expect("directory must accept a child file");
    let data = op
        .read("parent/child/proof")
        .await
        .expect("child file must be readable")
        .to_vec();
    cleanup(&op, &["parent/child/proof", "parent/child/", "parent/"]).await;
    assert_eq!(data.as_slice(), b"directory-proof");
}
