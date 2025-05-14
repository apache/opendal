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
+-------------------------------------+
| Path                 Type   Content |
+=====================================+
| [TEMP_DIR]           DIR            |
| [TEMP_DIR]/dst.txt   FILE   hello   |
| [TEMP_DIR]/src.txt   FILE   hello   |
+-------------------------------------+
    ");
    Ok(())
}

#[tokio::test]
async fn test_cp_file_to_existing_dir() -> Result<()> {
    let dir = tempdir()?;
    let source_dir = dir.path().join("source");
    let dest_dir = dir.path().join("dest");
    fs::create_dir_all(&source_dir)?;
    fs::create_dir_all(&dest_dir)?;

    let source_file_name = "test_file.txt";
    let source_file_path = source_dir.join(source_file_name);
    let source_content = "hello";
    fs::write(&source_file_path, source_content)?;

    // Use paths directly as arguments for local fs operations
    let source_arg = source_file_path.to_str().unwrap();
    let dest_arg = dest_dir.to_str().unwrap();

    oli()
        .arg("cp")
        .arg(source_arg)
        .arg(dest_arg)
        .assert()
        .success();

    assert_snapshot!(directory_snapshot(dir.path()).with_content(true), @r"
+--------------------------------------------------+
| Path                              Type   Content |
+==================================================+
| [TEMP_DIR]                        DIR            |
| [TEMP_DIR]/dest                   DIR            |
| [TEMP_DIR]/dest/test_file.txt     FILE   hello   |
| [TEMP_DIR]/source                 DIR            |
| [TEMP_DIR]/source/test_file.txt   FILE   hello   |
+--------------------------------------------------+
    ");
    Ok(())
}

#[tokio::test]
async fn test_recursive_cp_dir_to_new_dir() -> Result<()> {
    let dir = tempdir()?;
    let source_base_dir = dir.path().join("source_root");
    let dest_base_dir = dir.path().join("dest_root");

    // Create source directory structure
    let source_dir = source_base_dir.join("source_dir");
    fs::create_dir_all(&source_dir)?;

    let file1_path = source_dir.join("file1.txt");
    fs::write(&file1_path, "file1_content")?;

    let sub_dir_path = source_dir.join("sub_dir");
    fs::create_dir(&sub_dir_path)?;

    let file2_path = sub_dir_path.join("file2.txt");
    fs::write(&file2_path, "file2_content")?;

    let file3_path = source_dir.join("file3.txt");
    fs::write(&file3_path, "file3_content")?;

    // Define destination path (should not exist yet)
    let dest_dir_path = dest_base_dir.join("dest_dir");

    oli()
        .arg("cp")
        .arg("-r")
        .arg(source_dir.to_str().unwrap())
        .arg(dest_dir_path.to_str().unwrap())
        .assert()
        .success();
    assert_snapshot!(directory_snapshot(dir.path()).with_content(true), @r"
+----------------------------------------------------------------------------+
| Path                                                  Type   Content       |
+============================================================================+
| [TEMP_DIR]                                            DIR                  |
| [TEMP_DIR]/dest_root                                  DIR                  |
| [TEMP_DIR]/dest_root/dest_dir                         DIR                  |
| [TEMP_DIR]/dest_root/dest_dir/file1.txt               FILE   file1_content |
| [TEMP_DIR]/dest_root/dest_dir/file3.txt               FILE   file3_content |
| [TEMP_DIR]/dest_root/dest_dir/sub_dir                 DIR                  |
| [TEMP_DIR]/dest_root/dest_dir/sub_dir/file2.txt       FILE   file2_content |
| [TEMP_DIR]/source_root                                DIR                  |
| [TEMP_DIR]/source_root/source_dir                     DIR                  |
| [TEMP_DIR]/source_root/source_dir/file1.txt           FILE   file1_content |
| [TEMP_DIR]/source_root/source_dir/file3.txt           FILE   file3_content |
| [TEMP_DIR]/source_root/source_dir/sub_dir             DIR                  |
| [TEMP_DIR]/source_root/source_dir/sub_dir/file2.txt   FILE   file2_content |
+----------------------------------------------------------------------------+
    ");
    Ok(())
}
