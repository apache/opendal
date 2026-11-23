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

#![cfg(unix)]

use opendal_core::Operator;
use opendal_service_fs::Fs;

#[tokio::test]
async fn read_preserves_whitespace_in_file_names() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("report.txt"), b"plain name").unwrap();
    std::fs::write(root.path().join("report.txt "), b"trailing space").unwrap();
    std::fs::write(root.path().join(" report.txt"), b"leading space").unwrap();
    let op = Operator::new(Fs::default().root(root.path().to_str().unwrap())).unwrap();

    assert_eq!(
        op.read("report.txt ").await.unwrap().to_vec(),
        b"trailing space"
    );
    assert_eq!(
        op.read(" report.txt").await.unwrap().to_vec(),
        b"leading space"
    );
}
