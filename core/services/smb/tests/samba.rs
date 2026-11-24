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

use futures::AsyncReadExt;
use futures::TryStreamExt;
use opendal_core::Builder;
use opendal_core::Configurator;
use opendal_core::ErrorKind;
use opendal_core::OperationContext;
use opendal_core::Operator;
use opendal_core::OperatorRegistry;
use opendal_core::raw::OpList;
use opendal_core::raw::Service;
use opendal_core::raw::oio::List;
use opendal_service_smb::SmbConfig;
use opendal_service_smb::register_smb_service;

fn config(namespace: &str) -> SmbConfig {
    let mut config = SmbConfig::from_iter(std::env::vars().filter_map(|(key, value)| {
        key.strip_prefix("OPENDAL_SMB_")
            .map(|key| (key.to_ascii_lowercase(), value))
    }))
    .unwrap();
    config.root = Some(format!(
        "{}/{namespace}-{}/",
        config.root.as_deref().unwrap_or("/"),
        std::process::id()
    ));
    config
}

#[tokio::test]
#[ignore = "requires the Samba fixture and OPENDAL_SMB_* configuration"]
async fn independent_operators_create_missing_parent_concurrently() {
    let first = Operator::new(config("concurrent-create").into_builder()).unwrap();
    let second = Operator::new(config("concurrent-create").into_builder()).unwrap();
    let barrier = tokio::sync::Barrier::new(2);
    let (first_result, second_result) = tokio::join!(
        async {
            barrier.wait().await;
            first.create_dir("parent/a/").await
        },
        async {
            barrier.wait().await;
            second.create_dir("parent/b/").await
        },
    );
    first_result.unwrap();
    second_result.unwrap();
    assert!(first.stat("parent/a/").await.unwrap().is_dir());
    assert!(second.stat("parent/b/").await.unwrap().is_dir());
    first.delete("parent/a/").await.unwrap();
    second.delete("parent/b/").await.unwrap();
    first.delete("parent/").await.unwrap();
    first.delete("/").await.unwrap();
}

#[tokio::test]
#[ignore = "requires the Samba fixture and OPENDAL_SMB_* configuration"]
async fn create_dir_rejects_target_file_without_changing_contents() {
    let op = Operator::new(config("target-file").into_builder()).unwrap();
    op.write("target", "unchanged").await.unwrap();
    let error = op.create_dir("target/").await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::NotADirectory);
    assert_eq!(op.read("target").await.unwrap().to_vec(), b"unchanged");
    op.delete("target").await.unwrap();
    op.delete("/").await.unwrap();
}

#[tokio::test]
#[ignore = "requires the Samba fixture and OPENDAL_SMB_* configuration"]
async fn create_dir_rejects_intermediate_file_without_changing_contents() {
    let op = Operator::new(config("intermediate-file").into_builder()).unwrap();
    op.write("parent", "unchanged").await.unwrap();
    let error = op.create_dir("parent/child/").await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::NotADirectory);
    assert_eq!(op.read("parent").await.unwrap().to_vec(), b"unchanged");
    op.delete("parent").await.unwrap();
    op.delete("/").await.unwrap();
}

#[tokio::test]
#[ignore = "requires the Samba fixture and OPENDAL_SMB_* configuration"]
async fn dropped_lister_releases_directory() {
    let op = Operator::new(config("lifecycle").into_builder()).unwrap();
    for i in 0..32 {
        op.write(&format!("drop-lister/{i}"), "data").await.unwrap();
    }
    let mut lister = op.lister("drop-lister/").await.unwrap();
    assert!(lister.try_next().await.unwrap().is_some());
    drop(lister);

    for i in 0..32 {
        op.delete(&format!("drop-lister/{i}")).await.unwrap();
    }
    op.delete("drop-lister/").await.unwrap();
    let error = op.stat("drop-lister/").await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::NotFound);
    op.delete("/").await.unwrap();
}

#[tokio::test]
#[ignore = "requires the Samba fixture and OPENDAL_SMB_* configuration"]
async fn paginated_listing_retains_all_entries() {
    let op = Operator::new(config("pagination").into_builder()).unwrap();
    let suffix = "x".repeat(180);
    for i in 0..200 {
        op.write(&format!("files/{i}-{suffix}"), "data")
            .await
            .unwrap();
    }
    let entries = op.list("files/").await.unwrap();
    assert_eq!(entries.len(), 201);
    for i in 0..200 {
        let path = format!("files/{i}-{suffix}");
        assert!(entries.iter().any(|entry| entry.path() == path));
        op.delete(&path).await.unwrap();
    }
    op.delete("files/").await.unwrap();
    op.delete("/").await.unwrap();
}

#[tokio::test]
#[ignore = "requires the Samba fixture and OPENDAL_SMB_* configuration"]
async fn authentication_failure_is_permission_denied() {
    let mut config = config("authentication");
    config.password = Some("incorrect-password".to_string());
    let op = Operator::new(config.into_builder()).unwrap();
    let error = op.stat("authentication-check").await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    assert!(!format!("{error:?}").contains("incorrect-password"));
}

#[tokio::test]
#[ignore = "requires the Samba fixture and OPENDAL_SMB_* configuration"]
async fn uri_credentials_connect_to_real_share() {
    let config = config("uri");
    register_smb_service(OperatorRegistry::get());
    let uri = format!(
        "smb://{}:{}@{}/{}{}",
        config.user.as_deref().unwrap(),
        config.password.as_deref().unwrap(),
        config.endpoint,
        config.share,
        config.root.as_deref().unwrap()
    );
    let op = Operator::from_uri(uri).unwrap();
    op.write("uri-check", "data").await.unwrap();
    assert_eq!(op.read("uri-check").await.unwrap().to_vec(), b"data");
    op.delete("uri-check").await.unwrap();
    op.delete("/").await.unwrap();
}

#[tokio::test]
#[ignore = "requires the Samba fixture and OPENDAL_SMB_* configuration"]
async fn large_copy_preserves_data_and_enforces_destination_condition() {
    let op = Operator::new(config("large-copy").into_builder()).unwrap();
    let content = (0..(512 * 1024 + 37))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    op.write("source", content.clone()).await.unwrap();
    let metadata = op.copy("source", "nested/target").await.unwrap();
    assert_eq!(metadata.content_length(), content.len() as u64);
    assert_eq!(op.read("nested/target").await.unwrap().to_vec(), content);

    let error = op
        .copy_with("source", "nested/target")
        .if_not_exists(true)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::ConditionNotMatch);
    assert_eq!(op.read("nested/target").await.unwrap().to_vec(), content);

    op.delete("source").await.unwrap();
    op.delete("nested/target").await.unwrap();
    op.delete("nested/").await.unwrap();
    op.delete("/").await.unwrap();
}

#[test]
#[ignore = "requires the Samba fixture and OPENDAL_SMB_* configuration"]
fn resources_can_be_dropped_after_runtime_shutdown() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (op, reader, writer) = runtime.block_on(async {
        let op = Operator::new(config("runtime").into_builder()).unwrap();
        op.write("read-file", vec![1; 128 * 1024]).await.unwrap();
        let mut reader = op
            .reader("read-file")
            .await
            .unwrap()
            .into_futures_async_read(0..)
            .await
            .unwrap();
        let mut byte = [0];
        reader.read_exact(&mut byte).await.unwrap();
        let mut writer = op.writer("write-file").await.unwrap();
        writer.write("partial").await.unwrap();
        (op, reader, writer)
    });
    drop(runtime);
    drop(reader);
    drop(writer);
    drop(op);
}

#[tokio::test]
#[ignore = "requires the Samba fixture and OPENDAL_SMB_* configuration"]
async fn list_without_trailing_slash_preserves_separator() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let cfg = config("list-no-slash");
        let op = Operator::new(cfg.clone().into_builder()).unwrap();
        op.write("sub/file.txt", "data").await.unwrap();
        op.write("sub/child/nested.txt", "nested").await.unwrap();

        let op_entries = op.list("sub").await.unwrap();
        assert!(op_entries.iter().any(|entry| entry.path() == "sub/"));

        let service = cfg.into_builder().build().unwrap();
        let mut lister = service
            .list(&OperationContext::default(), "sub", OpList::default())
            .unwrap();
        let mut paths = Vec::new();
        while let Some(entry) = lister.next().await.unwrap() {
            paths.push(entry.path().to_string());
        }
        paths.sort();
        assert_eq!(paths, vec!["sub/", "sub/child/", "sub/file.txt"]);

        op.delete("sub/child/nested.txt").await.unwrap();
        op.delete("sub/child/").await.unwrap();
        op.delete("sub/file.txt").await.unwrap();
        op.delete("sub/").await.unwrap();
        op.delete("/").await.unwrap();
    })
    .await
    .expect("list_without_trailing_slash_preserves_separator timed out");
}

#[tokio::test]
#[ignore = "requires the Samba fixture and OPENDAL_SMB_* configuration"]
async fn writer_rejects_operations_after_close_without_truncating() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let op = Operator::new(config("writer-close").into_builder()).unwrap();
        let mut writer = op.writer("close-once.txt").await.unwrap();
        writer.write("hello samba").await.unwrap();
        let meta = writer.close().await.unwrap();
        assert_eq!(meta.content_length(), 11);

        let err = writer.close().await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);

        let err = writer.write("more").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);

        assert_eq!(
            op.read("close-once.txt").await.unwrap().to_vec(),
            b"hello samba"
        );

        op.delete("close-once.txt").await.unwrap();
        op.delete("/").await.unwrap();
    })
    .await
    .expect("writer_rejects_operations_after_close_without_truncating timed out");
}

#[tokio::test]
#[ignore = "requires the Samba fixture and OPENDAL_SMB_* configuration"]
async fn rename_does_not_create_destination_parent_when_source_invalid() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let op = Operator::new(config("rename-parent").into_builder()).unwrap();
        op.create_dir("/").await.unwrap();

        let err = op
            .rename("missing", "new-parent/nested/target")
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound);
        assert!(!op.exists("new-parent/").await.unwrap());

        op.create_dir("existing-dir/").await.unwrap();
        let err = op
            .rename("existing-dir", "dir-parent/target")
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::IsADirectory);
        assert!(!op.exists("dir-parent/").await.unwrap());

        op.delete("existing-dir/").await.unwrap();
        op.delete("/").await.unwrap();
    })
    .await
    .expect("rename_does_not_create_destination_parent_when_source_invalid timed out");
}
