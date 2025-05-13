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

use anyhow::bail;
use futures::AsyncBufReadExt;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use opendal::ErrorKind;
use opendal::Metadata;
use std::path::Path;

use anyhow::Context;
use anyhow::Result;
use futures::AsyncWriteExt;
use futures::TryStreamExt;

use crate::config::Config;
use crate::make_tokio_runtime;
use crate::params::config::ConfigParams;

/// Template for the progress bar display.
///
/// The template includes:
/// - `{spinner:.green}`: A green spinner to indicate ongoing progress.
/// - `{elapsed_precise}`: The precise elapsed time.
/// - `{bar:40.cyan/blue}`: A progress bar with a width of 40 characters,
///   cyan for the completed portion and blue for the remaining portion.
/// - `{bytes}/{total_bytes}`: The number of bytes copied so far and the total bytes to be copied.
/// - `{eta}`: The estimated time of arrival (completion).
const PROGRESS_BAR_TEMPLATE: &str =
    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})";

const PROGRESS_CHARS: &str = "#>-";

#[derive(Debug, clap::Parser)]
#[command(name = "cp", about = "Copy object", disable_version_flag = true)]
pub struct CopyCmd {
    #[command(flatten)]
    pub config_params: ConfigParams,
    /// In the form of `<profile>:/<path>`.
    #[arg()]
    pub source: String,
    /// In the form of `<profile>:/<path>`.
    #[arg()]
    pub destination: String,
    /// Copy objects recursively.
    #[arg(short = 'r', long)]
    pub recursive: bool,
}

impl CopyCmd {
    pub fn run(self) -> Result<()> {
        make_tokio_runtime(1).block_on(self.do_run())
    }

    async fn do_run(self) -> Result<()> {
        let cfg = Config::load(&self.config_params.config)?;

        let (src_op, src_path) = cfg.parse_location(&self.source)?;
        let (dst_op, dst_path) = cfg.parse_location(&self.destination)?;

        let final_dst_path = match dst_op.stat(&dst_path).await {
            Ok(dst_meta) if dst_meta.mode().is_dir() => {
                if self.recursive {
                    dst_path.clone()
                } else if let Some(filename) = Path::new(&src_path).file_name() {
                    Path::new(&dst_path)
                        .join(filename)
                        .to_string_lossy()
                        .to_string()
                } else {
                    bail!(
                        "Cannot copy source '{}' into directory '{}': Source has no filename.",
                        src_path,
                        dst_path
                    );
                }
            }
            Ok(_) => {
                // Destination exists but is a file. Overwrite it (non-recursive)
                // or error (recursive, handled below).
                if self.recursive {
                    bail!(
                        "Recursive copy destination '{}' exists but is not a directory.",
                        dst_path
                    );
                }
                dst_path.clone()
            }
            Err(e) if e.kind() == ErrorKind::NotFound => dst_path.clone(),
            Err(e) => {
                return Err(e.into());
            }
        };

        if !self.recursive {
            // Non-recursive copy: Use the final_dst_path directly.
            let mut dst_w = dst_op
                .writer(&final_dst_path)
                .await?
                .into_futures_async_write();
            let src_meta = src_op.stat(&src_path).await?;
            let reader = src_op.reader_with(&src_path).chunk(8 * 1024 * 1024).await?;
            let buf_reader = reader
                .into_futures_async_read(0..src_meta.content_length())
                .await?;

            let copy_progress = CopyProgress::new(&src_meta, src_path.clone());
            copy_progress.copy(buf_reader, &mut dst_w).await?;
            dst_w.close().await?;
            return Ok(());
        }

        // Recursive copy: Ensure the base destination directory exists or create it.
        // Note: final_dst_path here refers to the original dst_path if it was a dir or didn't exist.
        match dst_op.stat(&final_dst_path).await {
            Ok(meta) if meta.mode().is_dir() => {
                // Base destination directory exists.
            }
            Ok(_) => {
                bail!(
                    "Recursive copy destination '{}' exists but is not a directory.",
                    final_dst_path
                );
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {
                dst_op.create_dir(&final_dst_path).await?;
            }
            Err(e) => {
                // Another error occurred trying to stat the base destination.
                return Err(e.into());
            }
        }

        // Proceed with recursive copy logic. dst_root is the target directory.
        let dst_root = Path::new(&final_dst_path);
        let mut ds = src_op.lister_with(&src_path).recursive(true).await?;
        let prefix = src_path.strip_prefix('/').unwrap_or(src_path.as_str());

        while let Some(de) = ds.try_next().await? {
            let meta = de.metadata();
            let depath = de.path();

            // Calculate relative path from the source root
            let relative_path = depath.strip_prefix(prefix).with_context(|| {
                anyhow::anyhow!(
                    "Internal error: Failed to strip prefix '{}' from path '{}'",
                    prefix,
                    depath
                )
            })?;
            let relative_path = relative_path.strip_prefix('/').unwrap_or(relative_path);

            let current_dst_path = dst_root.join(relative_path).to_string_lossy().to_string();

            if meta.mode().is_dir() {
                dst_op.create_dir(&current_dst_path).await?;
                continue;
            }

            if let Some(parent) = Path::new(&current_dst_path).parent() {
                if parent != dst_root {
                    dst_op.create_dir(&parent.to_string_lossy()).await?;
                }
            }

            let reader = src_op.reader_with(de.path()).chunk(8 * 1024 * 1024).await?;
            let buf_reader = reader
                .into_futures_async_read(0..meta.content_length())
                .await?;

            let copy_progress = CopyProgress::new(meta, de.path().to_string());
            let mut writer = dst_op
                .writer(&current_dst_path) // Use the calculated path
                .await?
                .into_futures_async_write();

            copy_progress.copy(buf_reader, &mut writer).await?;
            writer.close().await?;
        }
        Ok(())
    }
}

/// Helper struct to display progress of a copy operation.
struct CopyProgress {
    progress_bar: ProgressBar,
    path: String,
}

impl CopyProgress {
    fn new(meta: &Metadata, path: String) -> Self {
        let pb = ProgressBar::new(meta.content_length());
        pb.set_style(
            ProgressStyle::default_bar()
                .template(PROGRESS_BAR_TEMPLATE)
                .expect("invalid template")
                .progress_chars(PROGRESS_CHARS),
        );
        Self {
            progress_bar: pb,
            path,
        }
    }

    async fn copy<R, W>(&self, mut reader: R, writer: &mut W) -> std::io::Result<u64>
    where
        R: futures::AsyncBufRead + Unpin,
        W: futures::AsyncWrite + Unpin + ?Sized,
    {
        let mut written = 0;
        loop {
            let buf = reader.fill_buf().await?;
            if buf.is_empty() {
                break;
            }
            writer.write_all(buf).await?;
            let len = buf.len();
            reader.consume_unpin(len);
            written += len as u64;
            self.progress_bar.inc(len as u64);
        }
        self.progress_bar.finish_and_clear();
        println!("Finish {}", self.path);
        Ok(written)
    }
}
