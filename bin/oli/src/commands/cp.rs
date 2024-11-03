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

use std::path::Path;

use anyhow::Result;
use futures::AsyncWriteExt;
use futures::TryStreamExt;

use crate::config::Config;
use crate::params::config::ConfigParams;

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
            futures::io::copy_buf(buf_reader, &mut dst_w).await?;
            // flush data
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

            let mut writer = dst_op
                .writer(&dst_root.join(fp).to_string_lossy())
                .await?
                .into_futures_async_write();

            println!("Copying {}", de.path());
            futures::io::copy_buf(buf_reader, &mut writer).await?;
            writer.close().await?;
        }
        Ok(())
    }
}
