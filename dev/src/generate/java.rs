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

use crate::generate::parser::{sorted_services, Config, ConfigType, Services};
use anyhow::Result;
use minijinja::value::ViaDeserialize;
use minijinja::{context, Environment};
use std::fs;
use std::path::PathBuf;

fn enabled_service(srv: &str) -> bool {
    match srv {
        // not enabled in bindings/java/Cargo.toml
        "foundationdb" | "ftp" | "hdfs" | "rocksdb" | "tikv" => false,
        _ => true,
    }
}

pub fn generate(workspace_dir: PathBuf, services: Services) -> Result<()> {
    let srvs = sorted_services(services, enabled_service);
    let mut env = Environment::new();
    env.add_template("java", include_str!("java.j2"))?;
    env.add_function("make_java_type", make_java_type);
    env.add_function("make_populate_map", make_populate_map);
    env.add_filter("case_java_class_name", case_java_class_name);
    env.add_filter("case_java_field_name", case_java_field_name);
    let tmpl = env.get_template("java")?;

    let output =
        workspace_dir.join("bindings/java/src/main/java/org/apache/opendal/ServiceConfig.java");
    fs::write(output, tmpl.render(context! { srvs => srvs })?)?;
    Ok(())
}

fn case_java_class_name(s: String) -> String {
    heck::AsUpperCamelCase(s).to_string()
}

fn case_java_field_name(s: String) -> String {
    heck::AsLowerCamelCase(s).to_string()
}

fn make_java_type(ty: ViaDeserialize<ConfigType>) -> Result<String, minijinja::Error> {
    Ok(match ty.0 {
        ConfigType::Bool => "boolean",
        ConfigType::Duration => "Duration",
        ConfigType::I64 | ConfigType::U64 | ConfigType::Usize => "long",
        ConfigType::U32 | ConfigType::U16 => "int",
        ConfigType::Vec => "List<String>",
        ConfigType::String => "String",
    }
    .to_string())
}

fn make_populate_map(field: ViaDeserialize<Config>) -> Result<String, minijinja::Error> {
    let is_primitive_type = match field.value {
        ConfigType::U64
        | ConfigType::I64
        | ConfigType::U32
        | ConfigType::U16
        | ConfigType::Usize
        | ConfigType::Bool => true,
        ConfigType::Duration | ConfigType::Vec | ConfigType::String => false,
    };

    let field_name = case_java_field_name(field.name.clone());
    if is_primitive_type {
        return Ok(format!("map.put(\"{}\", String.valueOf({}));", field.name, field_name));
    }

    if field.optional {
        return Ok(format!(
            "if ({} != null) map.put(\"{}\", {});",
            field_name, field.name, field_name
        ));
    }

    Ok(format!("\nmap.put(\"{}\", {});", field.name, field_name))
}
