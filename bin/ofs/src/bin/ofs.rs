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

use std::str::FromStr;

use anyhow::Context;
use anyhow::Result;
use fuse3::path::Session;
use fuse3::MountOptions;
use ofs::Config;
use ofs::Ofs;
use opendal::Operator;
use opendal::Scheme;

#[tokio::main]
async fn main() -> Result<()> {
    ofs().await
}

async fn ofs() -> Result<()> {
    let cfg: Config =
        toml::from_str(&std::fs::read_to_string("ofs.toml").context("failed to open ofs.toml")?)?;
    let scheme = Scheme::from_str(&cfg.backend.typ).context("unsupported scheme")?;
    let op = Operator::via_map(scheme, cfg.backend.map.clone())?;

    let mut mount_option = MountOptions::default();
    mount_option.uid(nix::unistd::getuid().into());
    mount_option.gid(nix::unistd::getgid().into());

    let mount_path = cfg.frontends.mount_path.clone();

    let ofs = Ofs { op };

    let mounthandle = Session::new(mount_option)
        .mount_with_unprivileged(ofs, mount_path)
        .await?;

    mounthandle.await?;

    Ok(())
}
