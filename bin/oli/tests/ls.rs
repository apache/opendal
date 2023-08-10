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
async fn test_basic_ls() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let dst_path_1 = dir.path().join("dst_1.txt");
    let dst_path_2 = dir.path().join("dst_2.txt");
    let dst_path_3 = dir.path().join("dst_3.txt");

    let expect = "hello";
    fs::write(dst_path_1, expect)?;
    fs::write(dst_path_2, expect)?;
    fs::write(dst_path_3, expect)?;

    let mut cmd = Command::cargo_bin("oli")?;

    let current_dir = dir.path().to_string_lossy().to_string() + "/";

    cmd.arg("ls").arg(current_dir);
    let res = cmd.assert().success();
    let output = res.get_output().stdout.clone();

    let output_stdout = String::from_utf8(output)?;

    assert!(output_stdout.contains("dst_1.txt"));
    assert!(output_stdout.contains("dst_2.txt"));
    assert!(output_stdout.contains("dst_3.txt"));

    Ok(())
}
