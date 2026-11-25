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

use opendal_core::Operator;
use opendal_service_hdfs::Hdfs;

fn fixture(test: &str) -> (Hdfs, String) {
    let name_node = std::env::var("OPENDAL_HDFS_NAME_NODE").unwrap();
    let root = std::env::var("OPENDAL_HDFS_ROOT").unwrap();
    let root = format!(
        "{}/{test}-{}",
        root.trim_end_matches('/'),
        std::process::id()
    );
    (Hdfs::default().name_node(&name_node), root)
}

async fn atomic_write_with_dir(test: &str, omit_leading_slash: bool) {
    let (builder, root) = fixture(test);
    let temporary_dir = format!("{root}/temporary");
    let configured_dir = if omit_leading_slash {
        temporary_dir.trim_start_matches('/')
    } else {
        &temporary_dir
    };
    let op = Operator::new(builder.root(&root).atomic_write_dir(configured_dir)).unwrap();
    let mut writer = op.writer("target.txt").await.unwrap();
    writer.write("atomic payload").await.unwrap();

    assert!(!op.exists("target.txt").await.unwrap());
    let entries = op.list("temporary/").await.unwrap();
    assert_eq!(
        entries
            .iter()
            .filter(|entry| entry.metadata().is_file())
            .count(),
        1
    );
    writer.close().await.unwrap();

    assert_eq!(
        op.read("target.txt").await.unwrap().to_vec(),
        b"atomic payload"
    );
    assert_paths(op.list("temporary/").await.unwrap(), &["temporary/"]);
    op.delete_with("/").recursive(true).await.unwrap();
}

#[tokio::test]
#[ignore = "requires the HDFS cluster from fixtures/hdfs/docker-compose-hdfs-cluster.yml"]
async fn atomic_write_dir_without_trailing_slash() {
    atomic_write_with_dir("atomic-absolute", false).await;
}

#[tokio::test]
#[ignore = "requires the HDFS cluster from fixtures/hdfs/docker-compose-hdfs-cluster.yml"]
async fn atomic_write_dir_without_leading_or_trailing_slash() {
    atomic_write_with_dir("atomic-unrooted", true).await;
}

fn assert_paths(entries: Vec<opendal_core::Entry>, expected: &[&str]) {
    let mut paths = entries.iter().map(|entry| entry.path()).collect::<Vec<_>>();
    paths.sort_unstable();
    assert_eq!(paths, expected);
}

#[tokio::test]
#[ignore = "requires the HDFS cluster from fixtures/hdfs/docker-compose-hdfs-cluster.yml"]
async fn list_directory_without_trailing_slash() {
    let (builder, root) = fixture("list-noslash");
    let op = Operator::new(builder.root(&root)).unwrap();
    op.create_dir("sub/child/").await.unwrap();
    op.write("sub/file.txt", "data").await.unwrap();
    op.write("sub/child/nested.txt", "nested").await.unwrap();

    for path in ["sub", "sub/"] {
        let entries = op.list(path).await.unwrap();
        assert_paths(entries, &["sub/", "sub/child/", "sub/file.txt"]);
    }
    op.delete_with("/").recursive(true).await.unwrap();
}
