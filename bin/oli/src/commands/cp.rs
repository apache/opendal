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

use futures::AsyncBufReadExt;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use opendal::Metadata;
use std::path::Path;

use anyhow::Result;
use futures::AsyncWriteExt;
use futures::TryStreamExt;

use crate::config::Config;
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
    #[arg()]
    pub source: String,
    #[arg()]
    pub destination: String,
    /// Copy objects recursively.
    #[arg(short = 'r', long)]
    pub recursive: bool,
}

impl CopyCmd {
    pub async fn run(&self) -> Result<()> {
        let cfg = Config::load(&self.config_params.config)?;

        let (src_op, src_path) = cfg.parse_location(&self.source)?;

        let (dst_op, dst_path) = cfg.parse_location(&self.destination)?;

        if !self.recursive {
            let mut dst_w = dst_op.writer(&dst_path).await?.into_futures_async_write();
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

        let dst_root = Path::new(&dst_path);
        let mut ds = src_op.lister_with(&src_path).recursive(true).await?;
        let prefix = src_path.strip_prefix('/').unwrap_or(src_path.as_str());
        while let Some(de) = ds.try_next().await? {
            let meta = de.metadata();
            if meta.mode().is_dir() {
                continue;
            }
            let depath = de.path();
            let fp = depath
                .strip_prefix('/')
                .unwrap_or(depath)
                .strip_prefix(prefix)
                .expect("invalid path");
            let reader = src_op.reader_with(de.path()).chunk(8 * 1024 * 1024).await?;
            let buf_reader = reader
                .into_futures_async_read(0..meta.content_length())
                .await?;

            let copy_progress = CopyProgress::new(&meta, de.path().to_string());
            let mut writer = dst_op
                .writer(&dst_root.join(fp).to_string_lossy())
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
