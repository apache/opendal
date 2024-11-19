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
use std::process::Command;

use anyhow::Result;
use assert_cmd::prelude::*;

#[tokio::test]
async fn test_basic_cp() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let src_path = dir.path().join("src.txt");
    let dst_path = dir.path().join("dst.txt");
    let expect = "hello";
    fs::write(&src_path, expect)?;

    let mut cmd = Command::cargo_bin("oli")?;

    cmd.arg("cp")
        .arg(src_path.as_os_str())
        .arg(dst_path.as_os_str());
    cmd.assert().success();

    let actual = fs::read_to_string(&dst_path)?;
    assert_eq!(expect, actual);
    Ok(())
}

#[tokio::test]
async fn test_cp_for_path_in_current_dir() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let src_path = dir.path().join("src.txt");
    let dst_path = dir.path().join("dst.txt");
    let expect = "hello";
    fs::write(src_path, expect)?;

    let mut cmd = Command::cargo_bin("oli")?;

    cmd.arg("cp")
        .arg("src.txt")
        .arg("dst.txt")
        .current_dir(dir.path());
    cmd.assert().success();

    let actual = fs::read_to_string(dst_path)?;
    assert_eq!(expect, actual);
    Ok(())
}
