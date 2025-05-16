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
use tempfile::TempDir;
use uuid::Uuid;

#[tokio::test]
async fn test_tee_basic() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let source_path = temp_dir.path().join("source.txt");
    let dest_path = temp_dir.path().join("dest.txt");

    let test_content = "Hello, OpenDAL!";
    fs::write(&source_path, test_content)?;

    let mut cmd = Command::cargo_bin("oli")?;
    cmd.arg("tee")
        .arg(format!("fs:///{}", source_path.to_str().unwrap()))
        .arg(format!("fs:///{}", dest_path.to_str().unwrap()));

    let output = cmd.assert().success();
    let stdout_output = String::from_utf8(output.get_output().stdout.clone())?;
    
    // Check stdout
    assert!(stdout_output.contains(test_content), "Stdout should contain the test content.");
    assert!(stdout_output.contains(&format!("File written to fs:///{} and stdout.", dest_path.to_str().unwrap())), "Stdout should contain the success message.");


    // Check destination file content
    let dest_content = fs::read_to_string(&dest_path)?;
    assert_eq!(test_content, dest_content, "Destination file content should match source.");

    Ok(())
}

#[tokio::test]
async fn test_tee_non_existent_source() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let source_path = temp_dir.path().join(Uuid::new_v4().to_string()); // Non-existent
    let dest_path = temp_dir.path().join("dest.txt");

    let mut cmd = Command::cargo_bin("oli")?;
    cmd.arg("tee")
        .arg(format!("fs:///{}", source_path.to_str().unwrap()))
        .arg(format!("fs:///{}", dest_path.to_str().unwrap()));

    cmd.assert().failure();

    Ok(())
}

#[tokio::test]
async fn test_tee_destination_already_exists() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let source_path = temp_dir.path().join("source.txt");
    let dest_path = temp_dir.path().join("dest.txt");

    let source_content = "Source content";
    let initial_dest_content = "Initial dest content";

    fs::write(&source_path, source_content)?;
    fs::write(&dest_path, initial_dest_content)?;

    let mut cmd = Command::cargo_bin("oli")?;
    cmd.arg("tee")
        .arg(format!("fs:///{}", source_path.to_str().unwrap()))
        .arg(format!("fs:///{}", dest_path.to_str().unwrap()));
    
    let output = cmd.assert().success();
    let stdout_output = String::from_utf8(output.get_output().stdout.clone())?;

    // Check stdout
    assert!(stdout_output.contains(source_content), "Stdout should contain the source content.");
    assert!(stdout_output.contains(&format!("File written to fs:///{} and stdout.", dest_path.to_str().unwrap())), "Stdout should contain the success message.");


    // Check that destination file is overwritten
    let dest_content_after_tee = fs::read_to_string(&dest_path)?;
    assert_eq!(source_content, dest_content_after_tee, "Destination file should be overwritten with source content.");

    Ok(())
}
