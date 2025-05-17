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

#[tokio::test]
async fn test_tee_destination_already_exists() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let dest_path = temp_dir.path().join("dest.txt");

    let source_content = "Source content";
    let initial_dest_content = "Initial dest content";

    fs::write(&dest_path, initial_dest_content)?;

    let mut cmd = oli();
    cmd.arg("tee").arg(dest_path.to_str().unwrap());

    cmd.stdin(std::process::Stdio::piped());
    cmd.stdout(std::process::Stdio::piped());
    let mut child = cmd.spawn()?;
    let mut stdin = child.stdin.take().expect("Failed to open stdin");

    let content_to_write = source_content.to_string();
    std::thread::spawn(move || {
        stdin
            .write_all(content_to_write.as_bytes())
            .expect("Failed to write to stdin");
    });

    let output = child.wait_with_output()?;
    assert!(output.status.success());

    let stdout_output = String::from_utf8(output.stdout.clone())?;
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

    cmd.stdin(std::process::Stdio::piped());
    cmd.stdout(std::process::Stdio::piped());
    let mut child = cmd.spawn()?;
    let mut stdin = child.stdin.take().expect("Failed to open stdin");

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

#[test]
fn test_tee_non_existing_file() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let dst_path = temp_dir.path().join("non_existing_file.txt");
    let dst_path_str = dst_path.as_os_str().to_str().unwrap();

    let mut cmd: assert_cmd::Command = std::process::Command::cargo_bin("oli")?.into();
    cmd.args(["tee", dst_path_str]);
    cmd.write_stdin("Hello, world!");
    cmd.assert().success();

    let content = fs::read_to_string(dst_path)?;
    assert_eq!(content, "Hello, world!");

    Ok(())
}

#[test]
fn test_tee_append_succeed() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let dst_path = temp_dir.path().join("test_append.txt");
    let dst_path_str = dst_path.as_os_str().to_str().unwrap();

    // Initial content
    fs::write(&dst_path, "Hello, ")?;

    let mut cmd: assert_cmd::Command = std::process::Command::cargo_bin("oli")?.into();
    cmd.args(["tee", "-a", dst_path_str]);
    cmd.write_stdin("world!");
    cmd.assert().success();

    let content = fs::read_to_string(dst_path)?;
    assert_eq!(content, "Hello, world!");

    Ok(())
}

#[test]
fn test_tee_append_file_not_found() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let file_path = temp_dir.path().join("test_append_not_found.txt");
    let file_path_str = file_path.to_str().unwrap();

    let mut cmd: assert_cmd::Command = std::process::Command::cargo_bin("oli")?.into();
    cmd.arg("tee")
        .arg("-a")
        .arg(file_path_str)
        .write_stdin("append data")
        .assert()
        .success();

    let content = fs::read_to_string(file_path)?;
    assert_eq!(content, "append data");

    Ok(())
}

#[test]
fn test_tee_overwrite_existing_file() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let file_path = temp_dir.path().join("test_overwrite.txt");
    let file_path_str = file_path.to_str().unwrap();

    // Create an existing file with some content
    fs::write(&file_path, "initial data")?;

    let mut cmd: assert_cmd::Command = std::process::Command::cargo_bin("oli")?.into();
    cmd.arg("tee").arg(file_path_str).write_stdin("new data");
    cmd.assert().success();

    let content = fs::read_to_string(file_path)?;
    assert_eq!(content, "new data");

    Ok(())
}
