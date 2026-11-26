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

use crate::generate::parser::{Config, ConfigType, Services, sorted_services};
use anyhow::Result;
use minijinja::value::ViaDeserialize;
use minijinja::{Environment, context};
use serde::Serialize;
use std::fs;
use std::path::{Path, PathBuf};

use super::capability;

/// Fields present in the legacy 46-parameter public `Capability` constructor.
const LEGACY_46_CAPABILITY_FIELDS: &[&str] = &[
    "stat",
    "stat_with_if_match",
    "stat_with_if_none_match",
    "stat_with_if_version_match",
    "stat_with_if_version_not_match",
    "stat_with_if_modified_since",
    "stat_with_if_unmodified_since",
    "stat_with_version",
    "read",
    "read_with_if_match",
    "read_with_if_none_match",
    "read_with_override_cache_control",
    "read_with_override_content_disposition",
    "read_with_override_content_type",
    "write",
    "write_can_multi",
    "write_can_append",
    "write_with_content_type",
    "write_with_content_disposition",
    "write_with_cache_control",
    "write_with_if_match",
    "write_with_if_none_match",
    "write_with_if_not_exists",
    "write_with_user_metadata",
    "write_multi_max_size",
    "write_multi_min_size",
    "create_dir",
    "delete",
    "copy",
    "rename",
    "list",
    "list_with_limit",
    "list_with_start_after",
    "list_with_recursive",
    "list_with_versions",
    "list_with_deleted",
    "presign",
    "presign_read",
    "presign_stat",
    "presign_write",
    "shared",
    "read_with_version",
    "read_with_if_version_match",
    "read_with_if_version_not_match",
    "read_with_if_modified_since",
    "read_with_if_unmodified_since",
];

/// Render model handed to the Java capability templates.
#[derive(Serialize)]
struct CapabilityField {
    /// snake_case field name in core `opendal::Capability`.
    name: String,
    /// camelCase field name in Java `org.apache.opendal.Capability`.
    camel: String,
    /// Plain `bool` flag when true, `Option<usize>` limit when false.
    is_bool: bool,
    /// Whether the field is part of the legacy 46-parameter public constructor.
    in_legacy_46: bool,
    /// Single-line Javadoc text from core.
    doc: String,
}

fn enabled_service(srv: &str) -> bool {
    match srv {
        // not enabled in bindings/java/Cargo.toml
        "foundationdb" | "ftp" | "git" | "hdfs" | "hf3fs" | "rocksdb" | "tikv" => false,
        _ => true,
    }
}

pub fn generate(workspace_dir: PathBuf, services: Services) -> Result<()> {
    generate_service_config(&workspace_dir, services)?;
    generate_capability(&workspace_dir)
}

fn generate_service_config(workspace_dir: &Path, services: Services) -> Result<()> {
    let srvs = sorted_services(services, enabled_service);
    let mut env = Environment::new();
    env.add_template("java", include_str!("java.j2"))?;
    env.add_function("make_field", make_field);
    env.add_function("make_populate_map", make_populate_map);
    env.add_filter("case_java_class_name", case_java_class_name);
    let tmpl = env.get_template("java")?;

    let output =
        workspace_dir.join("bindings/java/src/main/java/org/apache/opendal/ServiceConfig.java");
    let rendered = tmpl.render(context! { srvs => srvs })?;
    fs::write(output, format!("{rendered}\n"))?;
    Ok(())
}

fn generate_capability(workspace_dir: &Path) -> Result<()> {
    let source = workspace_dir.join("core/core/src/types/capability.rs");
    let fields: Vec<CapabilityField> = capability::parse(&source)?
        .into_iter()
        .map(|f| CapabilityField {
            camel: case_java_field_name(&f.name),
            in_legacy_46: LEGACY_46_CAPABILITY_FIELDS.contains(&f.name.as_str()),
            name: f.name,
            is_bool: f.is_bool,
            doc: f.doc,
        })
        .collect();
    let jni_sig: String = fields
        .iter()
        .map(|f| if f.is_bool { 'Z' } else { 'J' })
        .collect();

    let mut env = Environment::new();
    env.add_template("capability_java", include_str!("java/capability_java.j2"))?;
    env.add_template("capability_rs", include_str!("java/capability_rs.j2"))?;

    let outputs = [
        (
            "capability_java",
            "bindings/java/src/main/java/org/apache/opendal/Capability.java",
        ),
        ("capability_rs", "bindings/java/src/capability.rs"),
    ];
    for (template, relative) in outputs {
        let tmpl = env.get_template(template)?;
        let rendered = tmpl.render(context! { fields => fields, jni_sig => jni_sig })?;
        fs::write(workspace_dir.join(relative), format!("{rendered}\n"))?;
    }

    Ok(())
}

fn case_java_class_name(s: &str) -> String {
    heck::AsUpperCamelCase(s).to_string()
}

fn case_java_field_name(s: &str) -> String {
    heck::AsLowerCamelCase(s).to_string()
}

fn make_field(field: ViaDeserialize<Config>) -> Result<String, minijinja::Error> {
    use std::fmt::Write;

    let mut result = String::new();
    let w = &mut result;

    // write comment
    let html = markdown::to_html(&field.comments);
    writeln!(w, "/**")?;
    for line in html.lines() {
        writeln!(w, " * {line}")?;
    }
    if let Some(deprecated) = &field.deprecated {
        writeln!(w, " *")?;
        writeln!(w, " * @deprecated {}", deprecated.note)?;
    }
    writeln!(w, " */")?;

    // write field definition
    let field_type = if field.optional {
        match field.value {
            ConfigType::Bool => "Boolean",
            ConfigType::String => "String",
            ConfigType::Duration => "Duration",
            ConfigType::Usize | ConfigType::U64 | ConfigType::I64 => "Long",
            ConfigType::U32 | ConfigType::U16 => "Integer",
            ConfigType::Vec => "List<String>",
            ConfigType::HashMap => "Map<String, String>",
        }
    } else {
        match field.value {
            ConfigType::Bool => "boolean",
            ConfigType::String => "@NonNull String",
            ConfigType::Duration => "@NonNull Duration",
            ConfigType::Usize | ConfigType::U64 | ConfigType::I64 => "long",
            ConfigType::U32 | ConfigType::U16 => "int",
            ConfigType::Vec => "@NonNull List<String>",
            ConfigType::HashMap => "@NonNull Map<String, String>",
        }
    };

    writeln!(
        w,
        "public final {} {};",
        field_type,
        case_java_field_name(&field.name)
    )?;
    Ok(result)
}

fn make_populate_map(field: ViaDeserialize<Config>) -> Result<String, minijinja::Error> {
    use std::fmt::Write;

    let mut result = String::new();
    let w = &mut result;

    let name = case_java_field_name(&field.name);
    let value = match field.value {
        ConfigType::Usize
        | ConfigType::U64
        | ConfigType::I64
        | ConfigType::Bool
        | ConfigType::U32
        | ConfigType::U16 => Some(format!("String.valueOf({name})")),
        ConfigType::String => Some(name.clone()),
        ConfigType::Duration => Some(format!("{name}.toString()")),
        ConfigType::Vec => Some(format!("String.join(\",\", {name})")),
        ConfigType::HashMap => None,
    };

    let indent = if field.optional { "    " } else { "" };
    if field.optional {
        writeln!(w, "if ({name} != null) {{")?;
    }
    if let Some(value) = value {
        let populate = format!("map.put(\"{}\", {value});", field.name);
        // java.j2 adds 12 columns; keep calls within the formatter's 120-column layout.
        if 12 + indent.len() + populate.len() > 120 {
            writeln!(w, "{indent}map.put(")?;
            writeln!(w, "{indent}        \"{}\",", field.name)?;
            writeln!(w, "{indent}        {value});")?;
        } else {
            writeln!(w, "{indent}{populate}")?;
        }
    } else {
        writeln!(w, "{indent}map.putAll({name});")?;
    }
    if field.optional {
        writeln!(w, "}}")?;
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_legacy_46_capability_fields_exist_in_core() {
        let path = crate::workspace_dir().join("core/core/src/types/capability.rs");
        let fields = capability::parse(&path).expect("core capability must parse");

        assert_eq!(LEGACY_46_CAPABILITY_FIELDS.len(), 46);
        for &legacy in LEGACY_46_CAPABILITY_FIELDS {
            assert!(
                fields.iter().any(|f| f.name == legacy),
                "legacy capability field `{legacy}` is missing from core Capability"
            );
        }
    }
}
