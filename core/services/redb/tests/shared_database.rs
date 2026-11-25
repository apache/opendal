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

use opendal_core::Operator;
use opendal_service_redb::Redb;

const TABLE: redb::TableDefinition<&str, &[u8]> = redb::TableDefinition::new("opendal");

#[tokio::test]
async fn shared_database_supports_write_read_and_list() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(redb::Database::create(dir.path().join("opendal.redb")).unwrap());

    let op = Operator::new(
        Redb::default()
            .database(db.clone())
            .table("opendal")
            .root("/"),
    )
    .unwrap();

    op.write("dir/a", "a").await.unwrap();
    op.write("dir/b", "bb").await.unwrap();
    op.write("other/c", "ccc").await.unwrap();

    assert_eq!(op.read("dir/b").await.unwrap().to_vec(), b"bb".to_vec());
    assert_eq!(list_paths(&op, "dir/").await, vec!["dir/a", "dir/b"]);

    let another = Operator::new(Redb::default().database(db).table("another").root("/")).unwrap();
    another.write("dir/a", "another table").await.unwrap();

    assert_eq!(
        another.read("dir/a").await.unwrap().to_vec(),
        b"another table".to_vec()
    );
    assert_eq!(list_paths(&op, "dir/").await, vec!["dir/a", "dir/b"]);
}

#[tokio::test]
async fn rows_written_through_redb_are_listed_and_readable() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(redb::Database::create(dir.path().join("opendal.redb")).unwrap());

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE).unwrap();
        table
            .insert("dir/external", b"external".as_slice())
            .unwrap();
    }
    write_txn.commit().unwrap();

    let op = Operator::new(Redb::default().database(db).table("opendal").root("/")).unwrap();

    assert_eq!(
        op.read("dir/external").await.unwrap().to_vec(),
        b"external".to_vec()
    );
    assert_eq!(list_paths(&op, "dir/").await, vec!["dir/external"]);
}

async fn list_paths(op: &Operator, path: &str) -> Vec<String> {
    let mut paths = op
        .list(path)
        .await
        .unwrap()
        .into_iter()
        .map(|entry| entry.path().to_string())
        .collect::<Vec<_>>();
    paths.sort();
    paths
}
