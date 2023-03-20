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

use std::env;
use std::ffi::OsStr;
use std::path::PathBuf;

use anyhow::anyhow;
use anyhow::Result;
use clap::{value_parser, Arg, Command};
use dirs::home_dir;

fn new_cmd(name: &'static str) -> Result<Command> {
    let home = home_dir().ok_or_else(|| anyhow!("unknown home dir"))?;
    let default_config_path = home.join(".config/oli/config.toml").as_os_str().to_owned();

    Ok(Command::new(name).arg(
        Arg::new("config")
            .long("config")
            .help("Path to the config file")
            .global(true)
            .default_value(default_config_path)
            .value_parser(value_parser!(PathBuf))
            .required(false),
    ))
}

#[tokio::main]
async fn main() -> Result<()> {
    // Guard against infinite proxy recursion. This mostly happens due to
    // bugs in oli.
    do_recursion_guard()?;

    match env::args()
        .next()
        .map(PathBuf::from)
        .as_ref()
        .and_then(|a| a.file_stem())
        .and_then(OsStr::to_str)
    {
        Some("oli") => {
            let cmd = oli::commands::cli::cli(new_cmd("oli")?);
            oli::commands::cli::main(&cmd.get_matches()).await?;
        }
        Some("ocp") => {
            let cmd = oli::commands::cp::cli(new_cmd("ocp")?);
            oli::commands::cp::main(&cmd.get_matches()).await?;
        }
        Some(v) => {
            println!("{v} is not supported")
        }
        None => return Err(anyhow!("couldn't determine self executable name")),
    }

    Ok(())
}

fn do_recursion_guard() -> Result<()> {
    static OLI_RECURSION_COUNT_MAX: i32 = 20;

    let recursion_count = env::var("OLI_RECURSION_COUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    if recursion_count > OLI_RECURSION_COUNT_MAX {
        return Err(anyhow!("infinite recursion detected"));
    }

    Ok(())
}
