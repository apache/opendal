// Copyright 2022 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use anyhow::anyhow;
use anyhow::Result;
use clap::Command;

pub async fn main() -> Result<()> {
    match cli().get_matches().subcommand() {
        Some(("cp", args)) => super::cp::main(Some(args.clone())).await?,
        _ => return Err(anyhow!("not handled")),
    }

    Ok(())
}

fn cli() -> Command {
    Command::new("oli")
        .version("0.10.0")
        .about("OpenDAL Command Line Interface")
        .subcommand(super::cp::cli("cp"))
}
