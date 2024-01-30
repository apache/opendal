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
use std::path::PathBuf;

use anyhow::anyhow;
use anyhow::Result;
use clap::Arg;
use clap::ArgAction;
use clap::ArgMatches;
use clap::Command;
use futures::TryStreamExt;

use crate::config::Config;

pub async fn main(args: &ArgMatches) -> Result<()> {
    let config_path = args
        .get_one::<PathBuf>("config")
        .ok_or_else(|| anyhow!("missing config path"))?;
    let cfg = Config::load(config_path)?;
    let recursive = args.get_flag("recursive");

    let src = args
        .get_one::<String>("source")
        .ok_or_else(|| anyhow!("missing source"))?;
    let (src_op, src_path) = cfg.parse_location(src)?;

    let dst = args
        .get_one::<String>("destination")
        .ok_or_else(|| anyhow!("missing target"))?;
    let (dst_op, dst_path) = cfg.parse_location(dst)?;

    if !recursive {
        let mut dst_w = dst_op.writer(&dst_path).await?;
        let reader = src_op.reader(&src_path).await?;
        let buf_reader = futures::io::BufReader::with_capacity(8 * 1024 * 1024, reader);
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
        let reader = src_op.reader(de.path()).await?;
        let buf_reader = futures::io::BufReader::with_capacity(8 * 1024 * 1024, reader);

        let mut writer = dst_op.writer(&dst_root.join(fp).to_string_lossy()).await?;

        println!("Copying {}", de.path());
        futures::io::copy_buf(buf_reader, &mut writer).await?;
        writer.close().await?;
    }
    Ok(())
}

pub fn cli(cmd: Command) -> Command {
    cmd.about("copy")
        .arg(Arg::new("source").required(true))
        .arg(Arg::new("destination").required(true))
        .arg(
            Arg::new("recursive")
                .required(false)
                .long("recursive")
                .short('r')
                .help("Copy files under source recursively to destination")
                .action(ArgAction::SetTrue),
        )
}
