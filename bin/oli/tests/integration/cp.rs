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
