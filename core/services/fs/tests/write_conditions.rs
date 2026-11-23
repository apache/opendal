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

use opendal_core::{ErrorKind, Operator};
use opendal_service_fs::Fs;
use tempfile::TempDir;

fn atomic_fs_operator(root: &TempDir, staging: &TempDir) -> Operator {
    Operator::new(
        Fs::default()
            .root(root.path().to_str().unwrap())
            .atomic_write_dir(staging.path().to_str().unwrap()),
    )
    .unwrap()
}

fn staging_is_empty(staging: &TempDir) -> bool {
    std::fs::read_dir(staging.path()).unwrap().next().is_none()
}

#[tokio::test]
async fn staged_conditional_writers_do_not_overwrite_each_other() {
    for append in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let staging = tempfile::tempdir().unwrap();
        let op = atomic_fs_operator(&root, &staging);
        assert!(op.info().capability().write_with_if_not_exists);
        let mut first = op
            .writer_with("data")
            .if_not_exists(true)
            .append(append)
            .await
            .unwrap();
        let mut second = op
            .writer_with("data")
            .if_not_exists(true)
            .append(append)
            .await
            .unwrap();

        first.write("first writer").await.unwrap();
        second.write("second writer").await.unwrap();
        assert!(!root.path().join("data").exists());
        first.close().await.unwrap();
        let second_result = second.close().await;

        assert_eq!(
            second_result.unwrap_err().kind(),
            ErrorKind::ConditionNotMatch
        );
        assert_eq!(op.read("data").await.unwrap().to_bytes(), "first writer");
        assert!(staging_is_empty(&staging));
    }
}

#[tokio::test]
async fn staged_conditional_write_preserves_existing_file() {
    for append in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let staging = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("data"), b"original").unwrap();
        let op = atomic_fs_operator(&root, &staging);
        assert!(op.info().capability().write_with_if_not_exists);

        let err = op
            .write_with("data", "replacement")
            .if_not_exists(true)
            .append(append)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
        assert_eq!(op.read("data").await.unwrap().to_bytes(), "original");
        assert!(staging_is_empty(&staging));
    }
}

#[tokio::test]
async fn staged_conditional_write_fails_when_destination_appears_before_commit() {
    for append in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let staging = tempfile::tempdir().unwrap();
        let op = atomic_fs_operator(&root, &staging);

        // The destination does not exist when the conditional writer opens,
        // so the check must happen while publishing the staged file.
        let mut writer = op
            .writer_with("data")
            .if_not_exists(true)
            .append(append)
            .await
            .unwrap();
        writer.write("staged content").await.unwrap();

        op.write_with("data", "concurrent winner").await.unwrap();

        let err = writer.close().await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
        assert_eq!(
            op.read("data").await.unwrap().to_bytes(),
            "concurrent winner"
        );
        assert!(staging_is_empty(&staging));
    }
}

#[tokio::test]
async fn staged_conditional_write_creates_missing_destination() {
    for append in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let staging = tempfile::tempdir().unwrap();
        let op = atomic_fs_operator(&root, &staging);

        let mut writer = op
            .writer_with("data")
            .if_not_exists(true)
            .append(append)
            .await
            .unwrap();
        writer.write("staged content").await.unwrap();
        writer.close().await.unwrap();

        assert_eq!(op.read("data").await.unwrap().to_bytes(), "staged content");
        assert!(staging_is_empty(&staging));
    }
}

#[tokio::test]
async fn staged_unconditional_write_replaces_existing_destination() {
    let root = tempfile::tempdir().unwrap();
    let staging = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("data"), b"original").unwrap();
    let op = atomic_fs_operator(&root, &staging);

    op.write_with("data", "replacement").await.unwrap();

    assert_eq!(op.read("data").await.unwrap().to_bytes(), "replacement");
    assert!(staging_is_empty(&staging));
}
