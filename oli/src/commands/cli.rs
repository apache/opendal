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

use std::str::FromStr;

use anyhow::anyhow;
use anyhow::Result;
use clap::App;
use clap::AppSettings;
use opendal::Object;

pub async fn main() -> Result<()> {
    match cli().get_matches().subcommand() {
        Some(("cp", args)) => {
            let source_path = args
                .get_one::<String>("source_file")
                .ok_or_else(|| anyhow!("missing source_file"))?;

            let source_object = build_object(source_path)?;

            let target_path = args
                .get_one::<String>("target_file")
                .ok_or_else(|| anyhow!("missing target_file"))?;

            let target_object = build_object(target_path)?;

            let size = source_object.metadata().await?.content_length();
            let reader = source_object.reader().await?;
            target_object.write_from(size, reader).await?;
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

fn build_object(path: &str) -> Result<Object> {
    let cp_path = super::profile::CopyPath::from_str(path)?;
    let operator = super::profile::build_operator(&cp_path)?;
    Ok(operator.object(&cp_path.path()))
}
