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

use crate::config::Config;
use crate::params::config::ConfigParams;
use anyhow::{Error, Result};
use futures::{AsyncWriteExt, TryStreamExt};
use opendal::Operator;
use std::path::Path;

#[derive(Debug, clap::Parser)]
#[command(name = "mv", about = "Move object", disable_version_flag = true)]
pub struct MoveCmd {
    #[command(flatten)]
    pub config_params: ConfigParams,
    #[arg()]
    pub source: String,
    #[arg()]
    pub destination: String,
    /// Move objects recursively.
    #[arg(short = 'r', long)]
    pub recursive: bool,
}

impl MoveCmd {
    pub async fn run(&self) -> Result<()> {
        let cfg = Config::load(&self.config_params.config)?;

        let (src_op, src_path) = cfg.parse_location(&self.source)?;
        let (dst_op, dst_path) = cfg.parse_location(&self.destination)?;

        let src_meta = src_op.stat(&src_path).await?;
        if !self.recursive || src_meta.is_file() {
            if src_meta.is_dir() {
                return Err(Error::msg("can not move a directory in non-recursive mode"));
            }

            let mut actual_dst_path = dst_path.clone();
            if let Ok(meta) = dst_op.stat(&dst_path).await {
                if meta.is_dir() && !dst_path.ends_with("/") {
                    actual_dst_path.push('/');
                }
            }
            if actual_dst_path.is_empty() || actual_dst_path.ends_with("/") {
                let file_name = src_path.rsplit_once("/").unwrap_or(("", &src_path)).1;
                actual_dst_path.push_str(file_name);
            }

            println!("Moving: {}", src_path);
            self.cp_file(
                &src_op,
                &src_path,
                &dst_op,
                &actual_dst_path,
                src_meta.content_length(),
            )
            .await?;
            src_op.delete(&src_path).await?;

            return Ok(());
        }

        let dst_root = Path::new(&dst_path);
        let prefix = src_path.strip_prefix('/').unwrap_or(src_path.as_str());
        let mut lst = src_op.lister_with(&src_path).recursive(true).await?;
        while let Some(entry) = lst.try_next().await? {
            let path = entry.path();
            if path == src_path {
                continue;
            }

            let suffix = path.strip_prefix(prefix).expect("invalid path");
            let depath = dst_root.join(suffix);

            println!("Moving: {}", path);
            let meta = entry.metadata();
            if meta.is_dir() {
                dst_op.create_dir(&depath.to_string_lossy()).await?;
                src_op.delete(path).await?;
                continue;
            }

            let path_metadata = src_op.stat(path).await?;
            self.cp_file(
                &src_op,
                path,
                &dst_op,
                &depath.to_string_lossy(),
                path_metadata.content_length(),
            )
            .await?;

            src_op.delete(path).await?;
        }

        Ok(())
    }

    async fn cp_file(
        &self,
        src_op: &Operator,
        src_path: &str,
        dst_op: &Operator,
        dst_path: &str,
        length: u64,
    ) -> Result<()> {
        let src_reader = src_op
            .reader_with(src_path)
            .chunk(8 * 1024 * 1024)
            .await?
            .into_futures_async_read(0..length)
            .await?;

        let mut dst_writer = dst_op.writer(dst_path).await?.into_futures_async_write();

        futures::io::copy_buf(src_reader, &mut dst_writer).await?;
        dst_writer.close().await?;

        Ok(())
    }
}
