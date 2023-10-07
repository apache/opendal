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
async fn test_basic_cat() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let dst_path = dir.path().join("dst.txt");
    let expect = "hello";
    fs::write(&dst_path, expect)?;

    let mut cmd = Command::cargo_bin("oli")?;

    cmd.arg("cat").arg(dst_path.as_os_str());
    let actual = fs::read_to_string(&dst_path)?;
    let res = cmd.assert().success();
    let output = res.get_output().stdout.clone();

    let output_stdout = String::from_utf8(output)?;

    assert_eq!(output_stdout, actual);
    Ok(())
}

#[tokio::test]
async fn test_cat_for_path_in_current_dir() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let dst_path = dir.path().join("dst.txt");
    let expect = "hello";
    fs::write(&dst_path, expect)?;

    let mut cmd = Command::cargo_bin("oli")?;

    cmd.arg("cat").arg("dst.txt").current_dir(dir.path());
    let actual = fs::read_to_string(&dst_path)?;
    let res = cmd.assert().success();
    let output = res.get_output().stdout.clone();

    let output_stdout = String::from_utf8(output)?;

    assert_eq!(output_stdout, actual);
    Ok(())
}
