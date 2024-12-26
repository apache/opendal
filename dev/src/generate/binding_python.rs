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

use crate::generate::parser::Services;
use anyhow::Result;
use itertools::Itertools;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

use super::parser::ConfigType;

/// TODO: add a common utils to parse enabled features from cargo.toml
fn enabled_service(srv: &str) -> bool {
    match srv {
        // not enabled in bindings/python/Cargo.toml
        "etcd" | "foundationdb" | "ftp" | "hdfs" | "rocksdb" | "tikv" => false,
        _ => true,
    }
}

pub fn generate(project_root: PathBuf, services: &Services) -> Result<()> {
    let mut s = fs::read_to_string(project_root.join("dev/templates/python"))
        .expect("failed to read python template file");

    for (srv, config) in services.clone().into_iter() {
        if !enabled_service(srv.as_str()) {
            continue;
        }

        s.push_str("\n    @overload\n");
        s.push_str("    def __init__(self,\n");
        s.push_str(format!("scheme: Literal[\"{}\"],", srv).as_str());
        s.push_str("\n*,\n");

        for (_, f) in config
            .config
            .into_iter()
            .enumerate()
            .sorted_by_key(|(i, x)| (x.optional, *i))
        {
            if let Some(deprecated) = f.deprecated {
                s.push_str("# deprecated: ");
                s.push_str(deprecated.as_str());
                s.push('\n');
            }

            s.push_str(&f.name);
            s.push_str(": ");
            match f.value {
                ConfigType::Bool => {
                    s.push_str("_bool");
                }
                ConfigType::Duration => {
                    s.push_str("_duration");
                }
                ConfigType::I64
                | ConfigType::Usize
                | ConfigType::U64
                | ConfigType::U32
                | ConfigType::U16 => {
                    s.push_str("_int");
                }
                ConfigType::Vec => {
                    s.push_str("_strings");
                }
                ConfigType::String => {
                    s.push_str("str");
                }
            }
            if f.optional {
                s.push_str(" = ...,\n");
            } else {
                s.push_str(",\n");
            }
        }

        s.push_str(")->None:...\n");
    }

    s.push_str("    @overload\n    def __init__(self, scheme:str, **kwargs: str) -> None: ...\n");

    let output_file: String = project_root
        .join("bindings/python/python/opendal/__base.pyi")
        .to_str()
        .expect("should build output file path")
        .into();

    fs::write(output_file.clone(), s).expect("failed to write result to file");

    Command::new("ruff")
        .arg("format")
        .arg(output_file)
        .output()?;

    Ok(())
}
