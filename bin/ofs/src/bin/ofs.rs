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

use std::collections::HashMap;
use std::str::FromStr;

use anyhow::anyhow;
use anyhow::Context;
use anyhow::Result;
use clap::Parser;
use fuse3::path::Session;
use fuse3::MountOptions;
use ofs::Ofs;
use opendal::Operator;
use opendal::Scheme;
use url::Url;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    fuse().await
}

#[derive(Parser, Debug)]
#[command(version, about)]
struct Config {
    /// fuse mount path
    #[arg(env = "OFS_MOUNT_PATH", index = 1)]
    mount_path: String,

    /// location of opendal service
    /// format: <scheme>://?<key>=<value>&<key>=<value>
    /// example: fs://root=/tmp
    #[arg(env = "OFS_BACKEND", index = 2)]
    backend: String,
}

async fn fuse() -> Result<()> {
    let cfg = Config::try_parse().context("parse command line arguments")?;

    let location = Url::parse(&cfg.backend)?;
    if location.has_host() {
        Err(anyhow!("Host part in a location is not supported."))?;
    }

    let scheme_str = location.scheme();

    let op_args = location
        .query_pairs()
        .into_owned()
        .collect::<HashMap<String, String>>();

    let scheme = Scheme::from_str(scheme_str).context("unsupported scheme")?;
    let op = Operator::via_map(scheme, op_args)?;

    let mut mount_option = MountOptions::default();
    mount_option.uid(nix::unistd::getuid().into());
    mount_option.gid(nix::unistd::getgid().into());

    let ofs = Ofs { op };

    let mounthandle = Session::new(mount_option)
        .mount_with_unprivileged(ofs, cfg.mount_path)
        .await?;

    mounthandle.await?;

    Ok(())
}
