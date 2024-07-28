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

use anyhow::anyhow;
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

    use fuse3::path::Session;
    use fuse3::MountOptions;
    use opendal::Operator;
    use opendal::Scheme;

    if cfg.backend.has_host() {
        log::warn!("backend host will be ignored");
    }

    let scheme_str = cfg.backend.scheme();
    let op_args = cfg.backend.query_pairs().into_owned();

    let scheme = match Scheme::from_str(scheme_str) {
        Ok(Scheme::Custom(_)) | Err(_) => Err(anyhow!("invalid scheme: {}", scheme_str)),
        Ok(s) => Ok(s),
    }?;
    let backend = Operator::via_iter(scheme, op_args)?;

    let mut mount_options = MountOptions::default();
    let mut gid = nix::unistd::getgid().into();
    mount_options.gid(gid);
    let mut uid = nix::unistd::getuid().into();
    mount_options.uid(uid);

    #[cfg(target_os = "linux")]
    let mut mount_handle = if nix::unistd::getuid().is_root() {
        if let Some(sudo_gid) = env::var("SUDO_GID")
            .ok()
            .and_then(|gid_str| gid_str.parse::<u32>().ok())
        {
            mount_options.gid(sudo_gid);
            gid = sudo_gid;
        }

        if let Some(sudo_uid) = env::var("SUDO_UID")
            .ok()
            .and_then(|gid_str| gid_str.parse::<u32>().ok())
        {
            mount_options.uid(uid);
            uid = sudo_uid;
        }

        let fs = fuse3_opendal::Filesystem::new(backend, uid, gid);
        Session::new(mount_options)
            .mount(fs, cfg.mount_path)
            .await?
    } else {
        let fs = fuse3_opendal::Filesystem::new(backend, uid, gid);
        Session::new(mount_options)
            .mount_with_unprivileged(fs, cfg.mount_path)
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

#[cfg(target_os = "windows")]
async fn execute(cfg: Config) -> Result<()> {
    use std::path::PathBuf;
    use std::str::FromStr;

    use anyhow::Context;
    use cloud_filter::root::HydrationType;
    use cloud_filter::root::PopulationType;
    use cloud_filter::root::SecurityId;
    use cloud_filter::root::Session;
    use cloud_filter::root::SyncRootIdBuilder;
    use cloud_filter::root::SyncRootInfo;
    use opendal::Operator;
    use opendal::Scheme;
    use tokio::runtime::Handle;
    use tokio::signal;

    const PROVIDER_NAME: &str = "ofs";

    if cfg.backend.has_host() {
        log::warn!("backend host will be ignored");
    }

    let scheme_str = cfg.backend.scheme();
    let op_args = cfg.backend.query_pairs().into_owned();

    let scheme = match Scheme::from_str(scheme_str) {
        Ok(Scheme::Custom(_)) | Err(_) => Err(anyhow!("invalid scheme: {}", scheme_str)),
        Ok(s) => Ok(s),
    }?;
    let backend = Operator::via_iter(scheme, op_args).context("invalid arguments")?;

    let sync_root_id = SyncRootIdBuilder::new(PROVIDER_NAME)
        .user_security_id(
            SecurityId::current_user().expect("get current user security id, it might be a bug"),
        )
        .build();

    if !sync_root_id
        .is_registered()
        .expect("check if sync root is registered, it might be a bug")
    {
        sync_root_id
            .register(
                SyncRootInfo::default()
                    .with_display_name(format!("ofs ({scheme_str})"))
                    .with_hydration_type(HydrationType::Full)
                    .with_population_type(PopulationType::Full)
                    .with_icon("%SystemRoot%\\system32\\charmap.exe,0")
                    .with_version(env!("CARGO_PKG_VERSION"))
                    .with_recycle_bin_uri("http://cloudmirror.example.com/recyclebin") // FIXME
                    .unwrap()
                    .with_path(&cfg.mount_path)
                    .context("mount_path is not a folder")?,
            )
            .context("failed to register sync root")?;
    }

    let handle = Handle::current();
    let connection = Session::new()
        .connect_async(
            &cfg.mount_path,
            cloudfilter_opendal::CloudFilter::new(backend, PathBuf::from(&cfg.mount_path)),
            move |f| handle.clone().block_on(f),
        )
        .context("failed to connect to sync root")?;

    signal::ctrl_c().await.unwrap();

    drop(connection);
    sync_root_id
        .unregister()
        .context("failed to unregister sync root")?;

    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "freebsd", target_os = "windows")))]
async fn execute(_cfg: Config) -> Result<()> {
    Err(anyhow!("platform not supported"))
}
