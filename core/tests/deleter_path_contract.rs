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

use std::fs;

use opendal::Operator;
use opendal::services::Fs;

#[tokio::test]
async fn deleter_normalizes_like_operator_delete() {
    let root = std::env::temp_dir().join(format!("opendal-7999-path-{}", uuid::Uuid::new_v4()));
    fs::create_dir(&root).unwrap();
    let target = root.join("a.txt");
    let spaced = root.join(" a.txt ");
    fs::write(&target, b"target").unwrap();
    fs::write(&spaced, b"unrelated sentinel").unwrap();
    let op = Operator::new(Fs::default().root(root.to_str().unwrap())).unwrap();

    op.delete(" a.txt ").await.unwrap();
    let operator_observed = (target.exists(), spaced.exists());
    assert_eq!(operator_observed, (false, true));

    fs::write(&target, b"target").unwrap();
    let mut deleter = op.deleter().await.unwrap();
    deleter.delete(" a.txt ").await.unwrap();
    deleter.close().await.unwrap();
    let deleter_observed = (target.exists(), spaced.exists());
    println!("PATH_CONTRACT_OBSERVED operator={operator_observed:?} deleter={deleter_observed:?}");
    fs::remove_dir_all(&root).unwrap();

    assert_eq!(
        deleter_observed, operator_observed,
        "Deleter must normalize exactly like Operator::delete"
    );
}
