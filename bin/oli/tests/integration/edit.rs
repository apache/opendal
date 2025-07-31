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

use std::env;
use std::fs;

use crate::test_utils::*;
use anyhow::Result;

/// Test helper that creates a mock editor script that doesn't modify files
fn create_no_change_editor(dir: &std::path::Path) -> Result<std::path::PathBuf> {
    let editor_path = dir.join("no_change_editor.sh");
    fs::write(
        &editor_path,
        "#!/bin/bash\n# Mock editor that exits successfully without modifying the file\nexit 0\n",
    )?;

    // Make the script executable
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&editor_path)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&editor_path, perms)?;
    }

    Ok(editor_path)
}

/// Test helper that creates a mock editor script that adds content to files
fn create_modifying_editor(
    dir: &std::path::Path,
    content_to_add: &str,
) -> Result<std::path::PathBuf> {
    let editor_path = dir.join("modifying_editor.sh");
    let script_content = format!(
        "#!/bin/bash\n# Mock editor that adds content to the file\necho '{content_to_add}' >> \"$1\"\nexit 0\n"
    );
    fs::write(&editor_path, script_content)?;

    // Make the script executable
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&editor_path)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&editor_path, perms)?;
    }

    Ok(editor_path)
}

/// Test helper that creates a mock editor script that replaces file content
fn create_replacing_editor(dir: &std::path::Path, new_content: &str) -> Result<std::path::PathBuf> {
    let editor_path = dir.join("replacing_editor.sh");
    let script_content = format!(
        "#!/bin/bash\n# Mock editor that replaces file content\necho '{new_content}' > \"$1\"\nexit 0\n"
    );
    fs::write(&editor_path, script_content)?;

    // Make the script executable
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&editor_path)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&editor_path, perms)?;
    }

    Ok(editor_path)
}

#[tokio::test]
async fn test_edit_existing_file_no_changes() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let file_path = dir.path().join("test_file.txt");
    let original_content = "Hello, World!";
    fs::write(&file_path, original_content)?;

    // Create a mock editor that doesn't change the file
    let editor_path = create_no_change_editor(dir.path())?;

    // Set the EDITOR environment variable
    let mut cmd = oli();
    cmd.env("EDITOR", editor_path.to_str().unwrap())
        .arg("edit")
        .arg(&file_path);

    assert_cmd_snapshot!(cmd, @r#"
    success: true
    exit_code: 0
    ----- stdout -----
    No changes detected.

    ----- stderr -----
    "#);

    // Verify the file content wasn't changed
    let actual_content = fs::read_to_string(&file_path)?;
    assert_eq!(original_content, actual_content);

    Ok(())
}

#[tokio::test]
async fn test_edit_existing_file_with_changes() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let file_path = dir.path().join("test_file.txt");
    let original_content = "Hello, World!";
    fs::write(&file_path, original_content)?;

    // Create a mock editor that adds content
    let added_content = "Added line";
    let editor_path = create_modifying_editor(dir.path(), added_content)?;

    // Set the EDITOR environment variable
    let mut cmd = oli();
    cmd.env("EDITOR", editor_path.to_str().unwrap())
        .arg("edit")
        .arg(&file_path);

    assert_cmd_snapshot!(cmd, @r#"
    success: true
    exit_code: 0
    ----- stdout -----
    File uploaded successfully to [TEMP_DIR]/test_file.txt

    ----- stderr -----
    "#);

    // Verify the file content was changed
    let actual_content = fs::read_to_string(&file_path)?;
    assert!(actual_content.contains(original_content));
    assert!(actual_content.contains(added_content));

    Ok(())
}

#[tokio::test]
async fn test_edit_new_file() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let file_path = dir.path().join("new_file.txt");

    // Ensure the file doesn't exist
    assert!(!file_path.exists());

    // Create a mock editor that adds content to the new file
    let new_content = "This is a new file";
    let editor_path = create_replacing_editor(dir.path(), new_content)?;

    // Set the EDITOR environment variable
    let mut cmd = oli();
    cmd.env("EDITOR", editor_path.to_str().unwrap())
        .arg("edit")
        .arg(&file_path);

    assert_cmd_snapshot!(cmd, @r#"
    success: true
    exit_code: 0
    ----- stdout -----
    File uploaded successfully to [TEMP_DIR]/new_file.txt

    ----- stderr -----
    "#);

    // Verify the file was created with the expected content
    assert!(file_path.exists());
    let actual_content = fs::read_to_string(&file_path)?;
    assert_eq!(new_content.trim(), actual_content.trim());

    Ok(())
}

#[tokio::test]
async fn test_edit_new_file_no_content() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let file_path = dir.path().join("empty_new_file.txt");

    // Ensure the file doesn't exist
    assert!(!file_path.exists());

    // Create a mock editor that doesn't add any content (leaves file empty)
    let editor_path = create_no_change_editor(dir.path())?;

    // Set the EDITOR environment variable
    let mut cmd = oli();
    cmd.env("EDITOR", editor_path.to_str().unwrap())
        .arg("edit")
        .arg(&file_path);

    assert_cmd_snapshot!(cmd, @r#"
    success: true
    exit_code: 0
    ----- stdout -----
    No changes detected.

    ----- stderr -----
    "#);

    // Since the file would be empty and we detect no changes, it shouldn't be uploaded
    // The local file might exist but the remote shouldn't be created

    Ok(())
}

#[tokio::test]
async fn test_edit_with_config_params() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let file_path = dir.path().join("test_file.txt");
    let original_content = "Hello with config!";
    fs::write(&file_path, original_content)?;

    // Create a mock editor that doesn't change the file
    let editor_path = create_no_change_editor(dir.path())?;

    // Test with config parameter (though we're using local fs, so config might not apply)
    let mut cmd = oli();
    cmd.env("EDITOR", editor_path.to_str().unwrap())
        .arg("edit")
        .arg("--config")
        .arg("nonexistent_config.toml") // This should be handled gracefully
        .arg(&file_path);

    // The command might succeed or fail depending on config handling
    // Let's just run it to see the behavior
    let _output = cmd.output().unwrap();
    // We don't assert success/failure here as it depends on config implementation

    Ok(())
}

#[tokio::test]
async fn test_edit_file_content_replacement() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let file_path = dir.path().join("replace_test.txt");
    let original_content = "Original content that should be replaced";
    fs::write(&file_path, original_content)?;

    // Create a mock editor that completely replaces the content
    let new_content = "Completely new content";
    let editor_path = create_replacing_editor(dir.path(), new_content)?;

    // Set the EDITOR environment variable
    let mut cmd = oli();
    cmd.env("EDITOR", editor_path.to_str().unwrap())
        .arg("edit")
        .arg(&file_path);

    assert_cmd_snapshot!(cmd, @r#"
    success: true
    exit_code: 0
    ----- stdout -----
    File uploaded successfully to [TEMP_DIR]/replace_test.txt

    ----- stderr -----
    "#);

    // Verify the file content was completely replaced
    let actual_content = fs::read_to_string(&file_path)?;
    assert_eq!(new_content.trim(), actual_content.trim());
    assert!(!actual_content.contains("Original content"));

    Ok(())
}
