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
use opendal_service_fs::Fs;

#[tokio::test]
async fn test_list_with_start_after() {
    let root = tempfile::tempdir().unwrap();
    let op = Operator::new(Fs::default().root(root.path().to_str().unwrap())).unwrap();
    for path in [
        "z.txt",
        "d/y",
        "d/x",
        "d/a/child",
        "d-early.txt",
        "a.txt",
        "-first.txt",
    ] {
        op.write(path, "data").await.unwrap();
    }

    for (prefix, recursive, marker, expected) in [
        ("", false, "d/x", &["z.txt"][..]),
        ("", true, "d/x", &["d/y", "z.txt"]),
        ("", false, "d", &["d-early.txt", "d/", "z.txt"]),
        (
            "",
            true,
            "d/",
            &["d/a/", "d/a/child", "d/x", "d/y", "z.txt"],
        ),
        ("d/", false, "d/", &["d/a/", "d/x", "d/y"]),
        (
            "d",
            true,
            "d-early.txt",
            &["d/", "d/a/", "d/a/child", "d/x", "d/y"],
        ),
        ("d/", true, "d/w", &["d/x", "d/y"]),
        ("d", false, "d-early.txt", &["d/"]),
        (
            "",
            false,
            "",
            &["-first.txt", "/", "a.txt", "d-early.txt", "d/", "z.txt"],
        ),
        (
            "",
            true,
            "",
            &[
                "-first.txt",
                "/",
                "a.txt",
                "d-early.txt",
                "d/",
                "d/a/",
                "d/a/child",
                "d/x",
                "d/y",
                "z.txt",
            ],
        ),
        ("", true, "z.txt", &[]),
    ] {
        let entries = op
            .list_with(prefix)
            .recursive(recursive)
            .start_after(marker)
            .await
            .unwrap();
        let paths: Vec<_> = entries.iter().map(|entry| entry.path()).collect();
        assert_eq!(
            paths, expected,
            "{prefix:?}, recursive={recursive}, marker={marker:?}"
        );
    }

    let capability = op.info().capability();
    assert!(capability.list_with_start_after);
    assert!(capability.list_with_recursive);
}
