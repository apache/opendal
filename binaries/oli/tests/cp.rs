// Copyright 2022 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::env;
use std::fs;
use std::path::Path;
use std::process::Command;

use anyhow::Result;
use assert_cmd::prelude::*;

#[tokio::test]
async fn test_basic_cp() -> Result<()> {
    let dir = env::temp_dir();
    fs::create_dir_all(dir.clone())?;
    let src_path = Path::new(&dir).join("src.txt");
    let dst_path = Path::new(&dir).join("dst.txt");
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
