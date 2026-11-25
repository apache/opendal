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

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use opendal_core::Operator;
use opendal_service_persy::Persy;

/// Temporary directory removed together with its contents when dropped.
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);

        let path = std::env::temp_dir().join(format!(
            "opendal-persy-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).expect("create temporary directory");

        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn build_operator(datafile: &Path, segment: &str, index: &str) -> Operator {
    Operator::new(
        Persy::default()
            .datafile(datafile.to_str().expect("datafile path is UTF-8"))
            .segment(segment)
            .index(index),
    )
    .expect("build operator")
}

#[tokio::test]
async fn overwrite_replaces_record_instead_of_leaking() {
    let dir = TempDir::new();
    let datafile = dir.path().join("db.persy");
    let op = build_operator(&datafile, "data", "index");

    for round in 0..20 {
        op.write("k", format!("value-{round}")).await.unwrap();
    }
    assert_eq!(op.read("k").await.unwrap().to_vec(), b"value-19");

    drop(op);

    let persy = persy::OpenOptions::new().open(&datafile).unwrap();
    assert_eq!(persy.scan("data").unwrap().count(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_delete_is_idempotent() {
    let dir = TempDir::new();
    let datafile = dir.path().join("db.persy");
    let op = Arc::new(build_operator(&datafile, "data", "index"));

    for round in 0..8 {
        op.write("k", format!("value-{round}")).await.unwrap();

        let mut deletes = Vec::new();
        for _ in 0..16 {
            let op = op.clone();
            deletes.push(tokio::spawn(async move { op.delete("k").await }));
        }
        for delete in deletes {
            delete.await.unwrap().unwrap();
        }
    }
}

#[tokio::test]
async fn build_initializes_new_segment_and_index_in_existing_datafile() {
    let dir = TempDir::new();
    let datafile = dir.path().join("db.persy");

    {
        let op = build_operator(&datafile, "s1", "i1");
        op.write("k", "value-1").await.unwrap();
        assert_eq!(op.read("k").await.unwrap().to_vec(), b"value-1");
    }

    {
        let op = build_operator(&datafile, "s2", "i2");
        op.write("k", "value-2").await.unwrap();
        assert_eq!(op.read("k").await.unwrap().to_vec(), b"value-2");
    }
}
