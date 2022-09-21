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

use std::collections::HashMap;

use anyhow::anyhow;
use anyhow::Result;
use clap::App;
use clap::AppSettings;
use opendal::Object;
use opendal::Operator;

pub fn main() -> Result<()> {
    match cli().get_matches().subcommand() {
        Some(("cp", args)) => {
            // 1. Parse profiles from env and build operators
            let operators = super::profile::build_operators()?;
            let fs_backend = opendal::services::fs::Builder::default().build()?;
            let fs_operator = opendal::Operator::new(fs_backend);

            // 2. parse src and dst file path, and then choose the
            // right operator to read and write.
            let source_path = args
                .get_one::<String>("source_file")
                .ok_or_else(|| anyhow!("missing source_file"))?;

            let source_object = build_object(source_path, &operators, &fs_operator)?;

            let target_path = args
                .get_one::<String>("target_file")
                .ok_or_else(|| anyhow!("missing target_file"))?;

            let target_object = build_object(target_path, &operators, &fs_operator)?;

            tokio::runtime::Builder::new_multi_thread()
                .build()?
                .block_on(async move {
                    // TODO: remove unwrap()
                    let size = source_object.metadata().await.unwrap().content_length();
                    let reader = source_object.reader().await.unwrap();
                    target_object.write_from(size, reader).await.unwrap();
                });
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

enum CopyPath {
    File(String),
    Profile((String, String)),
}

fn build_object(
    cp_path: &str,
    operators: &HashMap<String, Operator>,
    fs_operator: &Operator,
) -> Result<Object> {
    match parse_copy_path(cp_path) {
        CopyPath::File(path) => Ok(fs_operator.object(&path)),
        CopyPath::Profile((name, path)) => {
            let operator = operators
                .get(&name)
                .ok_or_else(|| anyhow!("profile group not found"))?;
            Ok(operator.object(&path))
        }
    }
}

fn parse_copy_path(cp_path: &str) -> CopyPath {
    if cp_path.contains("://") {
        let (name, path) = cp_path.split_once("://").unwrap();
        CopyPath::Profile((name.to_string(), path.to_string()))
    } else {
        CopyPath::File(cp_path.to_string())
    }
}
