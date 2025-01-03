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
    env.add_function("make_field", make_field);
    env.add_function("make_populate_map", make_populate_map);
    env.add_filter("case_java_class_name", case_java_class_name);
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

fn make_field(field: ViaDeserialize<Config>) -> Result<String, minijinja::Error> {
    let field_type = if field.optional {
        match field.value {
            ConfigType::Bool => "Boolean",
            ConfigType::String => "String",
            ConfigType::Duration => "Duration",
            ConfigType::Usize | ConfigType::U64 | ConfigType::I64 => "Long",
            ConfigType::U32 | ConfigType::U16 => "Integer",
            ConfigType::Vec => "List<String>",
        }
    } else {
        match field.value {
            ConfigType::Bool => "boolean",
            ConfigType::String => "@NonNull String",
            ConfigType::Duration => "@NonNull Duration",
            ConfigType::Usize | ConfigType::U64 | ConfigType::I64 => "long",
            ConfigType::U32 | ConfigType::U16 => "int",
            ConfigType::Vec => "@NonNull List<String>",
        }
    };

    Ok(format!(
        "private final {} {};",
        field_type,
        case_java_field_name(field.name.clone())
    ))
}

fn make_populate_map(field: ViaDeserialize<Config>) -> Result<String, minijinja::Error> {
    let populate = match field.value {
        ConfigType::Usize
        | ConfigType::U64
        | ConfigType::I64
        | ConfigType::Bool
        | ConfigType::U32
        | ConfigType::U16 => format!(
            "map.put(\"{}\", String.valueOf({}));",
            field.name,
            case_java_field_name(field.name.clone())
        ),
        ConfigType::String => format!(
            "map.put(\"{}\", {});",
            field.name,
            case_java_field_name(field.name.clone())
        ),
        ConfigType::Duration => format!(
            "map.put(\"{}\", {}.toString());",
            field.name,
            case_java_field_name(field.name.clone())
        ),
        ConfigType::Vec => format!(
            "map.put(\"{}\", String.join(\",\", {}));",
            field.name,
            case_java_field_name(field.name.clone())
        ),
    };

    if field.optional {
        Ok(format!(
            "if ({} != null) {{\n    {}\n}}",
            case_java_field_name(field.name.clone()),
            populate
        ))
    } else {
        Ok(populate)
    }
}
