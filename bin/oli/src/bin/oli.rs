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

//! The main oli command-line interface
//!
//! The oli binary is a chimera, changing its behavior based on the
//! name of the binary. It works like `rustup`: when the binary is called
//! 'oli' or 'oli.exe' it offers the oli command-line interface, and
//! when it is called 'ocp' it behaves as a proxy to 'oli cp'.

use std::ffi::OsString;
use std::path::PathBuf;

use anyhow::Result;
use clap::value_parser;
use oli::commands::OliSubcommand;

#[derive(Debug, clap::Parser)]
#[command(about, version)]
pub struct Oli {
    /// Path to the config file.
    #[arg(
        long,
        global = true,
        default_value = default_config_path(),
        value_parser = value_parser!(PathBuf)
    )]
    config: Option<PathBuf>,

    #[command(subcommand)]
    subcommand: OliSubcommand,
}

fn default_config_path() -> OsString {
    let d = dirs::config_dir().expect("unknown config dir");
    d.join("oli/config.toml").as_os_str().to_owned()
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli: Oli = clap::Parser::parse();
    cli.subcommand.run().await?;
    Ok(())
}
