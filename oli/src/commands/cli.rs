// Copyright 2022 Datafuse Labs.
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
use std::path::PathBuf;

use anyhow::anyhow;
use anyhow::Result;
use clap::App;
use clap::AppSettings;

pub fn main() -> Result<()> {
    match cli().get_matches().subcommand() {
        Some(("cp", args)) => {
            // 1. Parse profiles from env and build accessors
            let _ = super::profile::build_accessors()?;
            // 2. parse src and dst file path, and then choose the
            // right accessor to read and write.
            // TODO
            println!(
                "got oli cp, src: {:?}, target: {:?}",
                args.get_one::<PathBuf>("source_file"),
                args.get_one::<PathBuf>("target_file")
            )
        }
        _ => return Err(anyhow!("not handled")),
    }

    Ok(())
}

fn cli() -> App<'static> {
    let app = App::new("oli")
        .version("0.10.0")
        .about("OpenDAL Command Line Interface")
        .setting(AppSettings::DeriveDisplayOrder)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .subcommand(super::cp::cli("cp"));

    app
}
