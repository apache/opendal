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

use opendal_core::ErrorKind;
use opendal_core::Operator;
use opendal_service_hdfs::Hdfs;

fn fixture_operator() -> Operator {
    let root = std::env::var("OPENDAL_HDFS_ROOT").unwrap();
    let name_node = std::env::var("OPENDAL_HDFS_NAME_NODE").unwrap();

    Operator::new(Hdfs::default().root(&root).name_node(&name_node)).unwrap()
}

fn unique_path(name: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{name}-{}-{nanos}", std::process::id())
}

#[tokio::test]
#[ignore = "requires the HDFS cluster from fixtures/hdfs/docker-compose-hdfs-cluster.yml"]
async fn abort_unused_writer_preserves_existing_file() {
    let op = fixture_operator();
    let dir = unique_path("abort-unused-existing");
    let path = format!("{dir}/file.txt");
    op.create_dir(&format!("{dir}/")).await.unwrap();
    op.write(&path, "original").await.unwrap();

    let mut writer = op.writer(&path).await.unwrap();

    let err = writer.abort().await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Unsupported);
    assert_eq!(op.read(&path).await.unwrap().to_vec(), b"original");

    op.delete_with(&dir).recursive(true).await.unwrap();
}

#[tokio::test]
#[ignore = "requires the HDFS cluster from fixtures/hdfs/docker-compose-hdfs-cluster.yml"]
async fn abort_unused_writer_does_not_create_path() {
    let op = fixture_operator();
    let dir = unique_path("abort-unused-missing");
    let path = format!("{dir}/new-dir/nested/file.txt");

    let mut writer = op.writer(&path).await.unwrap();

    let err = writer.abort().await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Unsupported);
    assert!(!op.exists(&dir).await.unwrap());
}
