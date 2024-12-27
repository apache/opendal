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

use anyhow::Result;
use rinja::Template;
use std::fs;
use std::path::PathBuf;

use super::parser::Service;
use crate::generate::parser::Services;

// Using the template in this path, relative
// to the `templates` dir in the crate root
#[derive(Template)]
#[template(path = "types.ts.jinja2", ext = "txt", escape = "none")]
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
    let v = Vec::from_iter(
        services
            .clone()
            .into_iter()
            .filter(|x| enabled_service(x.0.as_str())),
    );

    let tmpl = PythonTemplate { services: v };

    let s = tmpl.render().expect("should render template");

    let output_file: String = project_root
        .join("bindings/nodejs/types.generated.d.ts")
        .to_str()
        .expect("should build output file path")
        .into();

    fs::write(output_file, s).expect("failed to write result to file");

    Ok(())
}
