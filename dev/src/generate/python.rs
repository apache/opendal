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

use crate::generate::parser::{sorted_services, ConfigType, Services};
use anyhow::Result;
use minijinja::value::ViaDeserialize;
use minijinja::{context, Environment};
use std::fs;
use std::path::PathBuf;

fn enabled_service(srv: &str) -> bool {
    match srv {
        // not enabled in bindings/python/Cargo.toml
        "etcd" | "foundationdb" | "ftp" | "hdfs" | "hdfs-native" | "rocksdb" | "tikv" => false,
        _ => true,
    }
}

pub fn generate(workspace_dir: PathBuf, services: Services) -> Result<()> {
    let env = setup_environment()?;
    let services = sorted_services(services, enabled_service);

    generate_typing_hints(&env, &workspace_dir, &services)?;
    generate_mkdocs(&env, &workspace_dir, &services)?;
    Ok(())
}

/// Generates Python typing hints
fn generate_typing_hints(env: &Environment, workspace_dir: &PathBuf, services: &Services) -> Result<()> {
    let template = env.get_template("python-type-hints")?;
    let output = workspace_dir.join("bindings/python/python/opendal/__base.pyi");
    fs::write(output, template.render(context! { services => services })?)?;
    Ok(())
}

/// Generates service documentations for mkdocs website.
fn generate_mkdocs(env: &Environment, workspace_dir: &PathBuf, services: &Services) -> Result<()> {
    let template = env.get_template("python-mkdocs-service")?;
    let output_folder = workspace_dir.join("bindings/python/docs/services/");
    fs::remove_dir_all(&output_folder)?;
    fs::create_dir(&output_folder)?;

    for (name, service) in services.iter() {
        let path = output_folder.join(format!("{}.md", name));
        fs::write(path, template.render(context! { name => name, service => service })?)?;
    }

    let template = env.get_template("python-mkdocs-service-summary")?;
    fs::write(output_folder.join("SUMMARY.md"), template.render(context! { services => services })?)?;
    Ok(())
}

/// Creates `Environment` for rendering
fn setup_environment() -> Result<Environment<'static>> { // includes static template strings
    let mut env = Environment::new();
    env.add_template("python-type-hints", include_str!("python-type-hints.j2"))?;
    // #5457 discussed a better approach generating config types when PyO3 can generate stubs when compiling.
    env.add_template("python-mkdocs-service", include_str!("python-mkdocs-service.j2"))?;
    env.add_template("python-mkdocs-service-summary", include_str!("python-mkdocs-service-summary.j2"))?;
    env.add_function("make_python_type", make_python_type);
    Ok(env)
}

fn make_python_type(ty: ViaDeserialize<ConfigType>) -> Result<String, minijinja::Error> {
    Ok(match ty.0 {
        ConfigType::Bool => "_bool",
        ConfigType::Duration => "_duration",
        ConfigType::I64
        | ConfigType::Usize
        | ConfigType::U64
        | ConfigType::U32
        | ConfigType::U16 => "_int",
        ConfigType::Vec => "_strings",
        ConfigType::String => "str",
    }
    .to_string())
}
