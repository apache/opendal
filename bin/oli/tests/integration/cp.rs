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

use crate::test_utils::*;
use anyhow::Result;

#[tokio::test]
async fn test_basic_cp() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let src_path = dir.path().join("src.txt");
    let dst_path = dir.path().join("dst.txt");
    let expect = "hello";
    fs::write(&src_path, expect)?;

    oli()
        .arg("cp")
        .arg(&src_path)
        .arg(&dst_path)
        .assert()
        .success();

    let actual = fs::read_to_string(&dst_path)?;
    assert_eq!(expect, actual);
    Ok(())
}

#[tokio::test]
async fn test_cp_for_path_in_current_dir() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let src_path = dir.path().join("src.txt");
    let expect = "hello";
    fs::write(&src_path, expect)?;

    oli()
        .arg("cp")
        .arg("src.txt")
        .arg("dst.txt")
        .current_dir(dir.path())
        .assert()
        .success();

    assert_snapshot!(directory_snapshot(dir.path()).with_content(true), @r"
    +----------------------------------------------------+
    | Path                 Type   Size (bytes)   Content |
    +====================================================+
    | [TEMP_DIR]           DIR    128                    |
    | [TEMP_DIR]/dst.txt   FILE   5              hello   |
    | [TEMP_DIR]/src.txt   FILE   5              hello   |
    +----------------------------------------------------+
    ");
    Ok(())
}

#[tokio::test]
async fn test_cp_file_to_existing_dir() -> Result<()> {
    let temp_dir = tempdir()?;
    let source_dir = temp_dir.path().join("source");
    let dest_dir = temp_dir.path().join("dest");
    fs::create_dir_all(&source_dir)?;
    fs::create_dir_all(&dest_dir)?;

    let source_file_name = "test_file.txt";
    let source_file_path = source_dir.join(source_file_name);
    let source_content = "hello";
    fs::write(&source_file_path, source_content)?;

    // Use paths directly as arguments for local fs operations
    let source_arg = source_file_path.to_str().unwrap();
    let dest_arg = dest_dir.to_str().unwrap();

    let mut cmd = Command::cargo_bin("oli")?;
    cmd.arg("cp").arg(source_arg).arg(dest_arg);

    cmd.assert().success();

    // Verify the file was copied into the destination directory
    let expected_dest_file_path = dest_dir.join(source_file_name);
    assert!(
        expected_dest_file_path.exists(),
        "Destination file should exist at: {:?}",
        expected_dest_file_path
    );

    // Verify content
    let dest_content = fs::read_to_string(&expected_dest_file_path)?;
    assert_eq!(source_content, dest_content);

    Ok(())
}
