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

use anyhow::Result;
use assert_cmd::Command;
use std::fs;

#[tokio::test]
async fn test_basic_mv() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let src_path = dir.path().join("src.txt");
    let dst_path = dir.path().join("dst.txt");
    let expect = "hello";
    fs::write(&src_path, expect)?;

    let mut cmd = Command::cargo_bin("oli")?;
    cmd.arg("mv")
        .arg(src_path.as_os_str())
        .arg(dst_path.as_os_str());
    cmd.assert().success();

    let actual = fs::read_to_string(&dst_path)?;
    assert_eq!(actual, expect);

    assert!(!fs::exists(&src_path)?);

    Ok(())
}

#[tokio::test]
async fn test_move_a_file_to_a_dir() -> Result<()> {
    let src_dir = tempfile::tempdir()?;
    let src_path = src_dir.path().join("src.txt");
    let expect = "hello";
    fs::write(&src_path, expect)?;

    let dst_dir = tempfile::tempdir()?;
    let dst_path = dst_dir.path().join("dir/");

    let mut cmd = Command::cargo_bin("oli")?;
    cmd.arg("mv")
        .arg(src_path.as_os_str())
        .arg(dst_path.as_os_str());
    cmd.assert().success();

    let dst_path = dst_path.join("src.txt");
    let actual = fs::read_to_string(&dst_path)?;
    assert_eq!(actual, expect);

    assert!(!fs::exists(&src_path)?);

    Ok(())
}

#[tokio::test]
async fn test_mv_with_recursive() -> Result<()> {
    let src_root = tempfile::tempdir()?;
    let src_path = src_root.path().join("src/");
    fs::create_dir(&src_path)?;

    let src_file1 = src_path.as_path().join("file1.txt");
    let file1_content = "file1";
    fs::write(&src_file1, file1_content).expect("write file1 error");

    let src_dir = src_path.join("dir/");
    fs::create_dir(&src_dir)?;
    let src_file2 = src_dir.as_path().join("file2.txt");
    let file2_content = "file2";
    fs::write(&src_file2, file2_content).expect("write file2 error");

    let src_empty_dir = src_path.join("empty_dir/");
    fs::create_dir(&src_empty_dir)?;

    let dst_path = tempfile::tempdir()?;

    let mut cmd = Command::cargo_bin("oli")?;
    cmd.arg("mv")
        .arg(src_path.as_os_str())
        .arg(dst_path.path().as_os_str())
        .arg("-r");
    cmd.assert().success();

    let dst_file1_content =
        fs::read_to_string(dst_path.path().join("file1.txt")).expect("read file1 error");
    assert_eq!(dst_file1_content, file1_content);
    let dst_file2_content =
        fs::read_to_string(dst_path.path().join("dir/file2.txt")).expect("read dir/file2 error");
    assert_eq!(dst_file2_content, file2_content);
    assert!(fs::exists(dst_path.path().join("empty_dir/"))?);

    assert!(!fs::exists(&src_path)?);

    Ok(())
}
