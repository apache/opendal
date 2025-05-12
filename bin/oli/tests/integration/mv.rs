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

use crate::test_utils::*;
use anyhow::Result;
use std::fs;

#[tokio::test]
async fn test_basic_mv() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let src_path = dir.path().join("src.txt");
    let dst_path = dir.path().join("dst.txt");
    let expect = "hello";
    fs::write(&src_path, expect)?;

    assert_snapshot!(directory_snapshot(dir.path()).with_content(true), @r"
    +--------------------+------+--------------+---------+
    | Path               | Type | Size (bytes) | Content |
    +====================================================+
    | [TEMP_DIR]         | DIR  | 96           |         |
    |--------------------+------+--------------+---------|
    | [TEMP_DIR]/src.txt | FILE | 5            | hello   |
    +--------------------+------+--------------+---------+
    ");

    oli()
        .arg("mv")
        .arg(&src_path)
        .arg(&dst_path)
        .assert()
        .success();

    assert_snapshot!(directory_snapshot(dir.path()).with_content(true), @r"
    +--------------------+------+--------------+---------+
    | Path               | Type | Size (bytes) | Content |
    +====================================================+
    | [TEMP_DIR]         | DIR  | 96           |         |
    |--------------------+------+--------------+---------|
    | [TEMP_DIR]/dst.txt | FILE | 5            | hello   |
    +--------------------+------+--------------+---------+
    ");
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

    oli()
        .arg("mv")
        .arg(src_path)
        .arg(dst_path)
        .assert()
        .success();

    assert_snapshot!(directory_snapshot(src_dir.path()).with_content(true), @r"
    +------------+------+--------------+---------+
    | Path       | Type | Size (bytes) | Content |
    +============================================+
    | [TEMP_DIR] | DIR  | 64           |         |
    +------------+------+--------------+---------+
    ");
    assert_snapshot!(directory_snapshot(dst_dir.path()).with_content(true), @r"
    +--------------------+------+--------------+---------+
    | Path               | Type | Size (bytes) | Content |
    +====================================================+
    | [TEMP_DIR]         | DIR  | 96           |         |
    |--------------------+------+--------------+---------|
    | [TEMP_DIR]/dir     | DIR  | 96           |         |
    |--------------------+------+--------------+---------|
    | [TEMP_DIR]/src.txt | FILE | 5            | hello   |
    +--------------------+------+--------------+---------+
    ");

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

    insta::assert_snapshot!(directory_snapshot(&src_root), @r"
    +----------------------+------+--------------+
    | Path                 | Type | Size (bytes) |
    +============================================+
    | [TEMP_DIR]           | DIR  | 96           |
    |----------------------+------+--------------|
    | [TEMP_DIR]/src       | DIR  | 160          |
    |----------------------+------+--------------|
    | [TEMP_DIR]/dir       | DIR  | 96           |
    |----------------------+------+--------------|
    | [TEMP_DIR]/file2.txt | FILE | 5            |
    |----------------------+------+--------------|
    | [TEMP_DIR]/empty_dir | DIR  | 64           |
    |----------------------+------+--------------|
    | [TEMP_DIR]/file1.txt | FILE | 5            |
    +----------------------+------+--------------+
    ");

    let dst_path = tempfile::tempdir()?;

    oli()
        .arg("mv")
        .arg(src_path)
        .arg(dst_path.path())
        .arg("-r")
        .assert()
        .success();

    assert_snapshot!(directory_snapshot(&src_root), @r"
    +----------------+------+--------------+
    | Path           | Type | Size (bytes) |
    +======================================+
    | [TEMP_DIR]     | DIR  | 96           |
    |----------------+------+--------------|
    | [TEMP_DIR]/src | DIR  | 64           |
    +----------------+------+--------------+
    ");
    assert_snapshot!(directory_snapshot(&dst_path).with_content(true), @r"
    +----------------------+------+--------------+---------+
    | Path                 | Type | Size (bytes) | Content |
    +======================================================+
    | [TEMP_DIR]           | DIR  | 160          |         |
    |----------------------+------+--------------+---------|
    | [TEMP_DIR]/dir       | DIR  | 96           |         |
    |----------------------+------+--------------+---------|
    | [TEMP_DIR]/file2.txt | FILE | 5            | file2   |
    |----------------------+------+--------------+---------|
    | [TEMP_DIR]/empty_dir | DIR  | 64           |         |
    |----------------------+------+--------------+---------|
    | [TEMP_DIR]/file1.txt | FILE | 5            | file1   |
    +----------------------+------+--------------+---------+
    ");

    Ok(())
}
