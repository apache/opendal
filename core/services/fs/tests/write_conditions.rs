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

#[cfg(target_os = "linux")]
#[tokio::test]
async fn staged_conditional_writers_do_not_overwrite_each_other() {
    for append in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let staging = tempfile::tempdir().unwrap();
        let op = Operator::new(
            Fs::default()
                .root(root.path().to_str().unwrap())
                .atomic_write_dir(staging.path().to_str().unwrap()),
        )
        .unwrap();
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
        assert!(std::fs::read_dir(staging.path()).unwrap().next().is_none());
    }
}

#[tokio::test]
async fn staged_conditional_write_preserves_existing_file() {
    let root = tempfile::tempdir().unwrap();
    let staging = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("data"), b"original").unwrap();
    let op = Operator::new(
        Fs::default()
            .root(root.path().to_str().unwrap())
            .atomic_write_dir(staging.path().to_str().unwrap()),
    )
    .unwrap();

    let supported = cfg!(target_os = "linux");
    assert_eq!(op.info().capability().write_with_if_not_exists, supported);
    let err = op
        .write_with("data", "replacement")
        .if_not_exists(true)
        .await
        .unwrap_err();
    let expected = if supported {
        ErrorKind::ConditionNotMatch
    } else {
        ErrorKind::Unsupported
    };
    assert_eq!(err.kind(), expected);
    assert_eq!(op.read("data").await.unwrap().to_bytes(), "original");
    assert!(std::fs::read_dir(staging.path()).unwrap().next().is_none());
}
