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
use opendal_service_fs::Fs;

#[tokio::test]
async fn writer_copy_from_same_file_is_rejected_without_truncation() {
    let root = tempfile::tempdir().unwrap();
    let op = Operator::new(Fs::default().root(root.path().to_str().unwrap())).unwrap();
    // Exceed the default 2 MiB positioned-read buffer so an unfixed copy
    // truncates the destination between two source reads.
    let content = vec![b'x'; 3 * 1024 * 1024];
    op.write("f", content.clone()).await.unwrap();

    for source in ["f", "./f", "/f", "//f"] {
        let mut writer = op.writer("f").await.unwrap();
        let err = writer
            .copy_from(source, 0..content.len() as u64)
            .await
            .expect_err("copying a file onto itself must fail");
        assert_eq!(err.kind(), ErrorKind::IsSameFile, "source: {source}");
        writer.abort().await.unwrap();

        assert_eq!(
            op.read("f").await.unwrap().to_vec(),
            content,
            "source must survive copy from {source}"
        );
    }
}
