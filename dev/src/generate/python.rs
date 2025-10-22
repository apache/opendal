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

use crate::generate::parser::{ConfigType, Services, sorted_services};
use anyhow::Result;
use minijinja::value::ViaDeserialize;
use minijinja::{Environment, context};
use std::fs;
use std::path::PathBuf;

fn enabled_service(srv: &str) -> bool {
    match srv {
        // not enabled in bindings/python/Cargo.toml
        "etcd" | "foundationdb" | "ftp" | "hdfs" | "rocksdb" | "tikv" | "sftp" | "github"
        | "cloudflare_kv" | "monoiofs" | "dbfs" | "surrealdb" | "d1" | "opfs" | "compfs"
        | "lakefs" | "pcloud" | "vercel_blob" => false,
        _ => true,
    }
}

pub fn generate(workspace_dir: PathBuf, services: Services) -> Result<()> {
    let srvs = sorted_services(services, enabled_service);
    let mut env = Environment::new();
    env.add_template("python", include_str!("new_python.j2"))?;
    env.add_function("snake_to_kebab_case", snake_to_kebab_case);
    env.add_function("service_to_feature", service_to_feature);
    env.add_function("service_to_pascal", service_to_pascal);
    env.add_function("make_python_type", make_python_type);
    let tmpl = env.get_template("python")?;

    let output = workspace_dir.join("bindings/python/src/services.rs");
    fs::write(output, tmpl.render(context! { srvs => srvs })?)?;
    Ok(())
}

fn snake_to_kebab_case(str: &str) -> String {
    str.replace('_', "-")
}

fn service_to_feature(service: &str) -> String {
    format!("services-{}", snake_to_kebab_case(service))
}

fn service_to_pascal(service: &str) -> String {
    let mut result = String::with_capacity(service.len());
    let mut capitalize = true;

    for &b in service.as_bytes() {
        if b == b'_' || b == b'-' {
            capitalize = true;
        } else {
            if capitalize {
                result.push((b as char).to_ascii_uppercase());
                capitalize = false;
            } else {
                result.push(b as char);
            }
        }
    }

    result
}

fn make_python_type(ty: ViaDeserialize<ConfigType>) -> Result<String, minijinja::Error> {
    Ok(match ty.0 {
        ConfigType::Bool => "builtins.bool",
        ConfigType::Duration => "typing.Any",
        ConfigType::I64
        | ConfigType::Usize
        | ConfigType::U64
        | ConfigType::U32
        | ConfigType::U16 => "builtins.int",
        ConfigType::Vec => "typing.Any",
        ConfigType::String => "builtins.str",
    }
    .to_string())
}
