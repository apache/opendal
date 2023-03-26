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

use anyhow::{anyhow, Result};
use clap::{Arg, ArgMatches, Command};

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

    let meta = op.stat(&path).await?;
    println!("Path: {path}");
    let size = meta.content_length();
    println!("Size: {size}");
    if let Some(etag) = meta.etag() {
        println!("Etag: {etag}");
    }
    let file_type = meta.mode();
    println!("Type: {file_type}");
    if let Some(content_type) = meta.content_type() {
        println!("Content-Type: {content_type}");
    }
    if let Some(last_modifed) = meta.last_modified() {
        println!("LastModified: {last_modifed}");
    }

    Ok(())
}

pub fn cli(cmd: Command) -> Command {
    cmd.version("0.10.0")
        .about("show object metadata")
        .arg(Arg::new("target").required(true))
}
