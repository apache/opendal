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

//! Commands provides the implementation of each command.

use std::ffi::OsString;

pub mod cat;
pub mod cp;
pub mod ls;
pub mod rm;
pub mod stat;

fn default_config_path() -> OsString {
    let d = dirs::config_dir().expect("unknown config dir");
    d.join("oli/config.toml").as_os_str().to_owned()
}

#[derive(Debug, clap::Subcommand)]
pub enum OliSubcommand {
    Cat(cat::CatCmd),
    Cp(cp::CopyCmd),
    Ls(ls::LsCmd),
    Rm(rm::RmCmd),
    Stat(stat::StatCmd),
}

impl OliSubcommand {
    pub async fn run(&self) -> anyhow::Result<()> {
        match self {
            Self::Cat(cmd) => cmd.run().await,
            Self::Cp(cmd) => cmd.run().await,
            Self::Ls(cmd) => cmd.run().await,
            Self::Rm(cmd) => cmd.run().await,
            Self::Stat(cmd) => cmd.run().await,
        }
    }
}
