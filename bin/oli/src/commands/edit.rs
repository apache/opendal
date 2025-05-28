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

use anyhow::{Context, Result};
use opendal::ErrorKind;
use std::env;
use std::fs;
use std::io::Write;
use std::process::Command;
use tempfile::NamedTempFile;

use crate::config::Config;
use crate::make_tokio_runtime;
use crate::params::config::ConfigParams;

#[derive(Debug, clap::Parser)]
#[command(
    name = "edit",
    about = "Edit a file from remote storage using local editor",
    disable_version_flag = true
)]
pub struct EditCmd {
    #[command(flatten)]
    pub config_params: ConfigParams,
    /// In the form of `<profile>:/<path>`.
    #[arg()]
    pub target: String,
}

impl EditCmd {
    pub fn run(self) -> Result<()> {
        make_tokio_runtime(1).block_on(self.do_run())
    }

    async fn do_run(self) -> Result<()> {
        let cfg = Config::load(&self.config_params.config)?;
        let (op, path) = cfg.parse_location(&self.target)?;

        // Create a temporary file
        let temp_file = NamedTempFile::new().context("Failed to create temporary file")?;

        let temp_path = temp_file.path().to_path_buf();

        // Try to read the existing file content
        let original_content_opendal_buffer = match op.read(&path).await {
            Ok(content) => {
                // Write existing content to temp file
                let mut tf_writer = temp_file.as_file();
                tf_writer
                    .write_all(content.to_vec().as_slice())
                    .context("Failed to write initial content to temporary file")?;
                tf_writer
                    .flush()
                    .context("Failed to flush temporary file after initial write")?;
                Some(content)
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {
                // File doesn't exist, start with empty content
                None
            }
            Err(e) => {
                return Err(
                    anyhow::Error::from(e).context("Failed to read file from remote storage")
                );
            }
        };

        // Get the editor command
        let editor = env::var("EDITOR").unwrap_or_else(|_| "vim".to_string());

        // Before launching the editor, we must ensure that our NamedTempFile object is dropped,
        // so that the editor has exclusive access and our later fs::read gets the freshest content.
        let temp_file_persister = temp_file.into_temp_path();

        let status = Command::new(&editor)
            .arg(&temp_path)
            .status()
            .context("Failed to start editor")?;

        if !status.success() {
            // Editor failed or exited with non-zero. We might want to offer to save the temp file.
            // For now, just error out.
            // To save: temp_file_persister.keep().context("Failed to persist temp file after editor failure")?;
            return Err(anyhow::anyhow!(
                "Editor exited with non-zero status: {}",
                status
            ));
        }

        // temp_file_persister is still in scope. If we do nothing, it will be dropped and the file deleted.
        // This is fine if we upload successfully. If upload fails, we need to .keep() it.

        let new_content_vec =
            fs::read(&temp_path).context("Failed to read temporary file after editing")?;

        let content_changed = match &original_content_opendal_buffer {
            Some(original_buffer) => {
                original_buffer.to_vec().as_slice() != new_content_vec.as_slice()
            }
            None => !new_content_vec.is_empty(), // If original was None (new file), changed if new content is not empty
        };

        if content_changed {
            // Try to upload the modified content
            match op.write(&path, new_content_vec.clone()).await {
                Ok(_) => {
                    println!("File uploaded successfully to {}", path);
                    // Successfully uploaded, temp_file_persister can be dropped, deleting the temp file.
                }
                Err(e) => {
                    // Upload failed, offer to save temp file
                    eprintln!("Error uploading file: {}", e);
                    let kept_path = temp_file_persister
                        .keep()
                        .context("Failed to preserve temporary file after upload error")?;
                    eprintln!("Your changes have been saved to: {}", kept_path.display());
                    eprintln!("You can retry uploading manually or re-edit.");
                    return Err(e.into()); // Propagate the original upload error
                }
            }
        } else {
            println!("No changes detected.");
            // No changes, temp_file_persister can be dropped, deleting the temp file.
        }

        Ok(())
    }
}
