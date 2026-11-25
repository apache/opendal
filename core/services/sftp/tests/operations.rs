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

use futures::TryStreamExt;
use opendal_core::{Configurator, ErrorKind, Operator, OperatorUri};
use opendal_service_sftp::{Sftp, SftpConfig};

const POOL_CAPACITY: usize = 64;

// Use the key-auth fixture in fixtures/sftp/docker-compose-sftp.yml.
fn fixture_builder(endpoint: &str) -> Sftp {
    Sftp::default()
        .endpoint(endpoint)
        .root("/upload")
        .user("foo")
        .key(&std::env::var("OPENDAL_SFTP_KEY").unwrap())
        .known_hosts_strategy("accept")
}

fn unique_path(name: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{name}-{}-{nanos}", std::process::id())
}

#[tokio::test]
#[ignore = "requires the real OpenSSH SFTP fixture"]
async fn create_dir_propagates_non_directory_and_permission_errors() {
    let endpoint = std::env::var("OPENDAL_SFTP_ENDPOINT").unwrap();
    let op = Operator::new(fixture_builder(&endpoint)).unwrap();

    let file_path = unique_path("create-dir-existing-file");
    op.write(&file_path, "data").await.unwrap();

    let err = op
        .create_dir(&format!("{file_path}/"))
        .await
        .expect_err("create_dir on an existing regular file must fail");
    assert_eq!(err.kind(), ErrorKind::NotADirectory, "{err:?}");

    op.delete(&file_path).await.unwrap();

    // The fixture home directory (`/`) is chrooted and owned by root (not writable by `foo`).
    let root_op = Operator::new(fixture_builder(&endpoint).root("/")).unwrap();
    let forbidden_dir = format!("{}/", unique_path("forbidden-dir"));
    let err = root_op
        .create_dir(&forbidden_dir)
        .await
        .expect_err("create_dir without write permission must fail");
    assert_eq!(err.kind(), ErrorKind::PermissionDenied, "{err:?}");
}

#[tokio::test]
#[ignore = "requires the real OpenSSH SFTP fixture"]
async fn copy_canonical_alias_self_does_not_truncate_source() {
    let endpoint = std::env::var("OPENDAL_SFTP_ENDPOINT").unwrap();
    let op = Operator::new(fixture_builder(&endpoint)).unwrap();

    let base = unique_path("copy-self-alias");
    let sub_dir = format!("{base}/sub/");
    let file_path = format!("{base}/real.txt");
    let alias_path = format!("{base}/sub/../real.txt");

    op.create_dir(&sub_dir).await.unwrap();
    op.write(&file_path, "precious-content").await.unwrap();

    let err = op
        .copy(&alias_path, &file_path)
        .await
        .expect_err("copying a file onto its canonical alias must fail");
    assert_eq!(err.kind(), ErrorKind::IsSameFile, "{err:?}");

    let content = op.read(&file_path).await.unwrap().to_vec();
    assert_eq!(content, b"precious-content");

    op.delete(&file_path).await.unwrap();
    op.delete(&sub_dir).await.unwrap();
    op.delete(&format!("{base}/")).await.unwrap();
}

#[tokio::test]
#[ignore = "requires the real OpenSSH SFTP fixture"]
async fn from_uri_connects_with_port_and_embedded_username() {
    let endpoint = std::env::var("OPENDAL_SFTP_ENDPOINT").unwrap();
    let authority = endpoint.strip_prefix("ssh://").unwrap_or(&endpoint);
    let key = std::env::var("OPENDAL_SFTP_KEY").unwrap();

    let uri = OperatorUri::new(
        &format!("sftp://foo@{authority}/upload"),
        [
            ("key".to_string(), key),
            ("known_hosts_strategy".to_string(), "accept".to_string()),
        ],
    )
    .unwrap();
    let cfg = SftpConfig::from_uri(&uri).unwrap();
    let op = Operator::new(cfg.into_builder()).unwrap();

    let file_path = unique_path("from-uri-test.txt");
    op.write(&file_path, "hello-uri").await.unwrap();
    assert_eq!(op.read(&file_path).await.unwrap().to_vec(), b"hello-uri");
    op.delete(&file_path).await.unwrap();
}

#[tokio::test]
#[ignore = "requires the real OpenSSH SFTP fixture"]
async fn open_writers_and_listers_hold_pool_capacity_until_closed() {
    let endpoint = std::env::var("OPENDAL_SFTP_ENDPOINT").unwrap();
    let op = Operator::new(
        fixture_builder(&endpoint)
            .acquire_timeout(Duration::from_millis(200))
            .connect_timeout(Duration::from_secs(10)),
    )
    .unwrap();

    let base = unique_path("pool-capacity-test");
    op.create_dir(&format!("{base}/")).await.unwrap();

    // Open POOL_CAPACITY writers sequentially so every pool slot is checked out.
    let mut writers = Vec::with_capacity(POOL_CAPACITY);
    let mut paths = Vec::with_capacity(POOL_CAPACITY);
    for i in 0..POOL_CAPACITY {
        let path = format!("{base}/w-{i}.txt");
        let mut writer = op.writer(&path).await.unwrap();
        writer.write("a").await.unwrap();
        writers.push(writer);
        paths.push(path);
    }

    let err = op
        .stat(&paths[0])
        .await
        .expect_err("pool must be exhausted while all writers are open");
    assert!(err.is_temporary(), "{err:?}");
    assert!(err.to_string().contains("waiting for pool capacity"));

    // Closing one writer must synchronously close its file and release its pool slot.
    let mut last_writer = writers.pop().unwrap();
    last_writer.close().await.unwrap();
    assert_eq!(op.read(paths.last().unwrap()).await.unwrap().to_vec(), b"a");
    drop(last_writer);

    for mut writer in writers {
        writer.close().await.unwrap();
    }

    // Open POOL_CAPACITY listers sequentially so every pool slot is checked out.
    let mut listers = Vec::with_capacity(POOL_CAPACITY);
    for _ in 0..POOL_CAPACITY {
        let mut lister = op.lister(&format!("{base}/")).await.unwrap();
        assert!(lister.try_next().await.unwrap().is_some());
        listers.push(lister);
    }

    let err = op
        .stat(&paths[0])
        .await
        .expect_err("pool must be exhausted while all listers are open");
    assert!(err.is_temporary(), "{err:?}");
    assert!(err.to_string().contains("waiting for pool capacity"));

    drop(listers);
    assert!(op.stat(&paths[0]).await.unwrap().is_file());

    for path in paths {
        op.delete(&path).await.unwrap();
    }
    op.delete(&format!("{base}/")).await.unwrap();
}

#[tokio::test]
#[ignore = "requires the real OpenSSH SFTP fixture"]
async fn concurrent_nested_renames_do_not_deadlock_pool() {
    let endpoint = std::env::var("OPENDAL_SFTP_ENDPOINT").unwrap();
    let op = Operator::new(
        fixture_builder(&endpoint)
            .acquire_timeout(Duration::from_secs(5))
            .connect_timeout(Duration::from_secs(15)),
    )
    .unwrap();

    let base = unique_path("concurrent-rename");
    op.create_dir(&format!("{base}/")).await.unwrap();

    // Create POOL_CAPACITY source files first and warm up the pool so sshd MaxStartups
    // is not hit during the concurrent rename burst.
    let mut srcs = Vec::with_capacity(POOL_CAPACITY);
    let mut dsts = Vec::with_capacity(POOL_CAPACITY);
    let mut warm_writers = Vec::with_capacity(POOL_CAPACITY);
    for i in 0..POOL_CAPACITY {
        let src = format!("{base}/src-{i}.txt");
        let dst = format!("{base}/nested-{i}/dst-{i}.txt");
        let mut w = op.writer(&src).await.unwrap();
        w.write(format!("payload-{i}")).await.unwrap();
        warm_writers.push(w);
        srcs.push(src);
        dsts.push(dst);
    }
    for mut w in warm_writers {
        w.close().await.unwrap();
    }

    let mut handles = Vec::with_capacity(POOL_CAPACITY);
    for (src, dst) in srcs.iter().cloned().zip(dsts.iter().cloned()) {
        let op = op.clone();
        handles.push(tokio::spawn(async move { op.rename(&src, &dst).await }));
    }

    for handle in handles {
        handle.await.unwrap().unwrap();
    }

    for (i, dst) in dsts.iter().enumerate() {
        assert_eq!(
            op.read(dst).await.unwrap().to_vec(),
            format!("payload-{i}").into_bytes()
        );
        op.delete(dst).await.unwrap();
        op.delete(&format!("{base}/nested-{i}/")).await.unwrap();
    }
    op.delete(&format!("{base}/")).await.unwrap();
}
