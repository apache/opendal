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
use clap::ArgMatches;
use clap::Command;

pub async fn main(args: &ArgMatches) -> Result<()> {
    match args.subcommand() {
        Some(("cat", sub_args)) => super::cat::main(sub_args).await?,
        Some(("cp", sub_args)) => super::cp::main(sub_args).await?,
        Some(("ls", sub_args)) => super::ls::main(sub_args).await?,
        Some(("rm", sub_args)) => super::rm::main(sub_args).await?,
        _ => return Err(anyhow!("not handled")),
    }

    Ok(())
}

pub fn cli(cmd: Command) -> Command {
    cmd.version("0.10.0")
        .about("OpenDAL Command Line Interface")
        .subcommand(super::cat::cli(Command::new("cat")))
        .subcommand(super::cp::cli(Command::new("cp")))
        .subcommand(super::ls::cli(Command::new("ls")))
        .subcommand(super::rm::cli(Command::new("rm")))
}
