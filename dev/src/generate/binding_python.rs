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
use askama::Template;
use itertools::Itertools;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

use super::parser::{ConfigType, Service};

// Using the template in this path, relative
// to the `templates` dir in the crate root
#[derive(Template)]
#[template(path = "python.py.jinja2", escape = "none")]
struct PythonTemplate {
    services: Vec<(String, Service)>,
}

/// TODO: add a common utils to parse enabled features from cargo.toml
fn enabled_service(srv: &str) -> bool {
    match srv {
        // not enabled in bindings/python/Cargo.toml
        "etcd" | "foundationdb" | "ftp" | "hdfs" | "rocksdb" | "tikv" => false,
        _ => true,
    }
}

pub fn generate(project_root: PathBuf, services: &Services) -> Result<()> {
    let mut v = Vec::from_iter(
        services
            .clone()
            .into_iter()
            .filter(|x| enabled_service(x.0.as_str())),
    );

    // move required options at beginning.
    for srv in &mut v {
        srv.1.config = srv
            .1
            .config
            .clone()
            .into_iter()
            .enumerate()
            .sorted_by_key(|(i, x)| (x.optional, *i))
            .map(|(_, e)| e)
            .collect();
    }

    let tmpl = PythonTemplate { services: v };

    let t = tmpl.render().expect("should render template");

    let output_file: String = project_root
        .join("bindings/python/python/opendal/__base.pyi")
        .to_str()
        .expect("should build output file path")
        .into();

    fs::write(output_file.clone(), t).expect("failed to write result to file");

    Command::new("ruff")
        .arg("format")
        .arg(output_file)
        .output()
        .expect("failed to run ruff");

    Ok(())
}

impl ConfigType {
    pub fn python_type(&self) -> String {
        match self {
            ConfigType::Bool => "_bool".into(),
            ConfigType::Duration => "_duration".into(),
            ConfigType::I64
            | ConfigType::Usize
            | ConfigType::U64
            | ConfigType::U32
            | ConfigType::U16 => "_int".into(),
            ConfigType::Vec => "_strings".into(),
            ConfigType::String => "str".into(),
        }
    }
}
