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

use std::env;
use anyhow::anyhow;
use anyhow::Result;
use clap::crate_description;
use clap::crate_name;
use clap::crate_version;
use clap::Command;

pub async fn main() -> Result<()> {
    env_logger::init();

    match cli().get_matches().subcommand() {
        Some(("http", _)) => crate::services::http::Service::new().await?.start().await?,
        _ => return Err(anyhow!("not handled subcommands")),
    }

    Ok(())
}

fn cli() -> Command {
    Command::new(crate_name!())
        .version(crate_version!())
        .about(crate_description!())
        .subcommand(Command::new("http"))
}
