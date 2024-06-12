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

use anyhow::Result;
use clap::Parser;
use url::Url;

#[derive(Parser, Debug)]
#[command(version, about)]
struct Config {
    /// fuse mount path
    #[arg(env = "OFS_MOUNT_PATH", index = 1)]
    mount_path: String,

    /// location of opendal service
    /// format: <scheme>://?<key>=<value>&<key>=<value>
    /// example: fs://?root=/tmp
    #[arg(env = "OFS_BACKEND", index = 2)]
    backend: Url,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let cfg = Config::parse();

    env_logger::init();
    execute(cfg).await
}

#[cfg(any(target_os = "linux", target_os = "freebsd"))]
async fn execute(cfg: Config) -> Result<()> {
    use std::collections::HashMap;
    use std::env;
    use std::str::FromStr;

    use anyhow::anyhow;
    use ofs::fuse3::Fuse;
    use opendal::Operator;
    use opendal::Scheme;

    if cfg.backend.has_host() {
        log::warn!("backend host will be ignored");
    }

    let scheme_str = cfg.backend.scheme();
    let op_args = cfg
        .backend
        .query_pairs()
        .into_owned()
        .collect::<HashMap<String, String>>();

    let scheme = match Scheme::from_str(scheme_str) {
        Ok(Scheme::Custom(_)) | Err(_) => Err(anyhow!("invalid scheme: {}", scheme_str)),
        Ok(s) => Ok(s),
    }?;
    let backend = Operator::via_map(scheme, op_args)?;

    #[cfg(target_os = "linux")]
    let mut mount_handle = if nix::unistd::getuid().is_root() {
        let mut fuse = Fuse::new();
        if let Some(gid) = env::var("SUDO_GID")
            .ok()
            .and_then(|gid_str| gid_str.parse::<u32>().ok())
        {
            fuse = fuse.gid(gid);
        }
        if let Some(uid) = env::var("SUDO_UID")
            .ok()
            .and_then(|gid_str| gid_str.parse::<u32>().ok())
        {
            fuse = fuse.uid(uid);
        }
        fuse.mount(cfg.mount_path, backend).await?
    } else {
        Fuse::new()
            .mount_with_unprivileged(cfg.mount_path, backend)
            .await?
    };

    #[cfg(target_os = "freebsd")]
    let mut mount_handle = Fuse::new().mount(cfg.mount_path, backend).await?;

    let handle = &mut mount_handle;
    tokio::select! {
        res = handle => res?,
        _ = tokio::signal::ctrl_c() => {
            mount_handle.unmount().await?
        }
    }

    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "freebsd")))]
async fn execute(_cfg: Config) -> Result<()> {
    Err(anyhow::anyhow!("platform not supported"))
}
