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

use std::path::PathBuf;

use anyhow::anyhow;
use anyhow::Result;
use clap::Arg;
use clap::ArgMatches;
use clap::Command;
use futures::io;

use crate::config::Config;

pub async fn main(args: &ArgMatches) -> Result<()> {
    let config_path = args
        .get_one::<PathBuf>("config")
        .ok_or_else(|| anyhow!("missing config path"))?;
    let cfg = Config::load(config_path)?;

    let target = args
        .get_one::<String>("target")
        .ok_or_else(|| anyhow!("missing target"))?;
    let (op, path) = cfg.parse_location(target)?;

    let reader = op.reader(&path).await?;
    let meta = op.stat(&path).await?;
    let mut buf_reader = reader.into_futures_io_async_read(0..meta.content_length());
    let mut stdout = io::AllowStdIo::new(std::io::stdout());
    io::copy_buf(&mut buf_reader, &mut stdout).await?;
    Ok(())
}

pub fn cli(cmd: Command) -> Command {
    cmd.about("display object content")
        .arg(Arg::new("target").required(true))
}
