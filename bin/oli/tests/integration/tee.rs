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
use std::io::Write;
use tempfile::TempDir;
use uuid::Uuid;

#[tokio::test]
async fn test_tee_basic() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let source_path = temp_dir.path().join("source.txt");
    let dest_path = temp_dir.path().join("dest.txt");

    let test_content = "Hello, OpenDAL!";
    fs::write(&source_path, test_content)?;

    let output = oli()
        .arg("tee")
        .arg(&dest_path)
        .arg(&source_path)
        .assert()
        .success();

    let stdout_output = String::from_utf8(output.get_output().stdout.clone())?;
    assert_eq!(stdout_output, test_content);
    let dest_content = fs::read_to_string(&dest_path)?;
    assert_eq!(dest_content, test_content);

    Ok(())
}

#[tokio::test]
async fn test_tee_non_existent_source() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let source_path = temp_dir.path().join(Uuid::new_v4().to_string()); // Non-existent
    let dest_path = temp_dir.path().join("dest.txt");

    oli()
        .arg("tee")
        .arg(dest_path.to_str().unwrap())
        .arg(source_path.to_str().unwrap())
        .assert()
        .failure();

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

    let output = oli()
        .arg("tee")
        .arg(dest_path.to_str().unwrap())
        .arg(source_path.to_str().unwrap())
        .assert()
        .success();
    let stdout_output = String::from_utf8(output.get_output().stdout.clone())?;
    assert_eq!(stdout_output, source_content);
    let dest_content_after_tee = fs::read_to_string(&dest_path)?;
    assert_eq!(dest_content_after_tee, source_content);

    Ok(())
}

#[tokio::test]
async fn test_tee_stdin() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let dest_path = temp_dir.path().join("dest_stdin.txt");

    let test_content = "Hello from stdin!";

    let mut cmd = oli();
    cmd.arg("tee").arg(&dest_path);
    // No source arg, should default to "-" (stdin)

    cmd.stdin(std::process::Stdio::piped());
    cmd.stdout(std::process::Stdio::piped()); // Ensure stdout is piped for capture
    let mut child = cmd.spawn()?;
    let mut stdin = child.stdin.take().expect("Failed to open stdin");

    // Write content to stdin in a separate thread
    let content_to_write = test_content.to_string();
    std::thread::spawn(move || {
        stdin
            .write_all(content_to_write.as_bytes())
            .expect("Failed to write to stdin");
    });

    let output = child.wait_with_output()?;

    assert!(output.status.success());

    let stdout_output = String::from_utf8(output.stdout.clone())?;
    assert_eq!(stdout_output, test_content);
    let dest_content = fs::read_to_string(&dest_path)?;
    assert_eq!(dest_content, test_content);

    Ok(())
}
