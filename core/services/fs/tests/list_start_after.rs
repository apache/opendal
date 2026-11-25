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

#[cfg(unix)]
#[tokio::test]
async fn test_list_with_start_after_directory_symlink() {
    use opendal_core::EntryMode;

    let root = tempfile::tempdir().unwrap();
    let root_path = root.path();
    std::fs::create_dir(root_path.join("realdir")).unwrap();
    std::fs::write(root_path.join("realdir/inside.txt"), b"data").unwrap();
    std::fs::write(root_path.join("link-file"), b"data").unwrap();
    std::os::unix::fs::symlink("realdir", root_path.join("link")).unwrap();
    std::os::unix::fs::symlink("link-file", root_path.join("file-link")).unwrap();
    std::os::unix::fs::symlink("nonexistent", root_path.join("dangle")).unwrap();

    let op = Operator::new(Fs::default().root(root_path.to_str().unwrap())).unwrap();

    // start_after("link") must keep directory symlink "link/" ("link/" > "link")
    // and maintain strict lexicographical order ('-' < '/').
    let entries = op.list_with("/").start_after("link").await.unwrap();
    let paths: Vec<_> = entries
        .iter()
        .map(|entry| (entry.path(), entry.metadata().mode()))
        .collect();
    assert_eq!(
        paths,
        vec![
            ("link-file", EntryMode::FILE),
            ("link/", EntryMode::DIR),
            ("realdir/", EntryMode::DIR),
        ]
    );

    // Full sorted listing and cursor pagination must neither reorder nor skip entries.
    let entries = op.list_with("/").start_after("a").await.unwrap();
    let paths: Vec<_> = entries
        .iter()
        .map(|entry| (entry.path(), entry.metadata().mode()))
        .collect();
    assert_eq!(
        paths,
        vec![
            ("dangle", EntryMode::Unknown),
            ("file-link", EntryMode::FILE),
            ("link-file", EntryMode::FILE),
            ("link/", EntryMode::DIR),
            ("realdir/", EntryMode::DIR),
        ]
    );

    let after_link_file: Vec<_> = op
        .list_with("/")
        .start_after("link-file")
        .await
        .unwrap()
        .into_iter()
        .map(|entry| entry.path().to_string())
        .collect();
    assert_eq!(after_link_file, vec!["link/", "realdir/"]);

    let after_link_dir: Vec<_> = op
        .list_with("/")
        .start_after("link/")
        .await
        .unwrap()
        .into_iter()
        .map(|entry| entry.path().to_string())
        .collect();
    assert_eq!(after_link_dir, vec!["realdir/"]);

    // Recursive listing must not descend into directory symlinks and must skip
    // "link/" when start_after is beneath "link/".
    let recursive: Vec<_> = op
        .list_with("/")
        .recursive(true)
        .start_after("link")
        .await
        .unwrap()
        .into_iter()
        .map(|entry| entry.path().to_string())
        .collect();
    assert_eq!(
        recursive,
        vec!["link-file", "link/", "realdir/", "realdir/inside.txt"]
    );

    let recursive_after_link_child: Vec<_> = op
        .list_with("/")
        .recursive(true)
        .start_after("link/a")
        .await
        .unwrap()
        .into_iter()
        .map(|entry| entry.path().to_string())
        .collect();
    assert_eq!(
        recursive_after_link_child,
        vec!["realdir/", "realdir/inside.txt"]
    );

    // Circular directory symlink must not trigger infinite recursion.
    std::os::unix::fs::symlink(".", root_path.join("self-loop")).unwrap();
    let with_loop: Vec<_> = op
        .list_with("/")
        .recursive(true)
        .start_after("realdir/z")
        .await
        .unwrap()
        .into_iter()
        .map(|entry| (entry.path().to_string(), entry.metadata().mode()))
        .collect();
    assert_eq!(with_loop, vec![("self-loop/".to_string(), EntryMode::DIR)]);
}
