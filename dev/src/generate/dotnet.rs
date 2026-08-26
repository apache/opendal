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

//! Generator for the .NET binding capability mirrors and service configs.
//!
//! The capability payload crosses the FFI boundary as a `#[repr(C)]` struct,
//! so the Rust mirror, the C# interop struct, and the public C# surface must
//! agree on field order and on the sentinel that encodes an absent limit.
//! All three files are rendered from `core/core/src/types/capability.rs` so
//! they cannot drift from core or from each other.
//!
//! The typed `*ServiceConfig` classes mirror each service's config struct and
//! are rendered from the same parsed service definitions the Java and Python
//! generators consume.

use anyhow::{Result, bail};
use minijinja::{Environment, context};
use serde::Serialize;
use std::fs;
use std::path::{Path, PathBuf};

use super::capability;
use super::parser::{Config, ConfigType, Services, sorted_services};

/// Render model handed to the capability templates, with every casing
/// precomputed so the templates stay purely structural.
#[derive(Serialize)]
struct CapabilityField {
    /// snake_case, used by the Rust mirror.
    name: String,
    /// PascalCase, used by the public C# properties.
    pascal: String,
    /// camelCase, used by the C# interop struct.
    camel: String,
    /// Plain `bool` flag when true, `Option<usize>` limit when false.
    is_bool: bool,
    /// Field doc from core, XML-escaped for use in C# doc comments.
    doc: String,
}

/// Render model for one field of a `*ServiceConfig` class.
#[derive(Serialize)]
struct ConfigField {
    /// PascalCase property name.
    pascal: String,
    /// snake_case option key passed to native OpenDAL.
    key: String,
    /// The C# property type, e.g. `string?` or `long`.
    cs_type: String,
    /// Whether `ToOptions` guards the entry with a null check.
    check_null: bool,
    /// The expression producing the option string for this property.
    value_expr: String,
    /// Deprecation message for `[System.Obsolete]`, empty when not deprecated.
    deprecated: String,
    /// Field doc from core, single line, XML-escaped. Empty docs omit the block.
    doc: String,
}

pub fn generate(workspace_dir: PathBuf, services: Services) -> Result<()> {
    generate_capability(&workspace_dir)?;
    generate_service_configs(&workspace_dir, services)
}

fn generate_capability(workspace_dir: &Path) -> Result<()> {
    let source = workspace_dir.join("core/core/src/types/capability.rs");
    let fields: Vec<CapabilityField> = capability::parse(&source)?
        .into_iter()
        .map(|f| CapabilityField {
            pascal: heck::AsUpperCamelCase(&f.name).to_string(),
            camel: heck::AsLowerCamelCase(&f.name).to_string(),
            name: f.name,
            is_bool: f.is_bool,
            doc: xml_escape(&f.doc),
        })
        .collect();

    let mut env = Environment::new();
    env.add_template("capability_rs", include_str!("dotnet_capability_rs.j2"))?;
    env.add_template("interop_cs", include_str!("dotnet_interop_cs.j2"))?;
    env.add_template("public_cs", include_str!("dotnet_public_cs.j2"))?;

    let outputs = [
        ("capability_rs", "bindings/dotnet/src/capability.rs"),
        (
            "interop_cs",
            "bindings/dotnet/OpenDAL/Interop/NativeObject/OpenDALCapability.cs",
        ),
        ("public_cs", "bindings/dotnet/OpenDAL/Capability.cs"),
    ];
    for (template, relative) in outputs {
        let tmpl = env.get_template(template)?;
        let rendered = tmpl.render(context! { fields => fields })?;
        write_rendered(&workspace_dir.join(relative), rendered)?;
    }

    Ok(())
}

fn enabled_service(srv: &str) -> bool {
    match srv {
        // not enabled in bindings/dotnet/Cargo.toml
        "foundationdb" | "foyer" | "ftp" | "hdfs" | "rocksdb" => false,
        _ => true,
    }
}

/// The class name and scheme string for a service, allowing the few services
/// whose established .NET surface deviates from the service directory name.
fn service_identity(srv: &str) -> (String, String) {
    match srv {
        // The public class predates codegen and must keep its name.
        "hf" => ("Huggingface".to_string(), "huggingface".to_string()),
        _ => (heck::AsUpperCamelCase(srv).to_string(), srv.to_string()),
    }
}

fn generate_service_configs(workspace_dir: &Path, services: Services) -> Result<()> {
    let srvs = sorted_services(services, enabled_service);

    let mut env = Environment::new();
    env.add_template("service_config", include_str!("dotnet_service_config.j2"))?;
    let tmpl = env.get_template("service_config")?;

    let mut names: Vec<&String> = srvs.keys().collect();
    names.sort();
    for name in names {
        let (class_name, scheme) = service_identity(name);
        let fields = srvs[name]
            .config
            .iter()
            // A HashMap config has no string encoding in the options map, so
            // it cannot be expressed through IServiceConfig.ToOptions.
            .filter(|config| config.value != ConfigType::HashMap)
            .map(config_field)
            .collect::<Result<Vec<ConfigField>>>()?;

        let rendered = tmpl.render(context! {
            service => name,
            class_name => class_name,
            scheme => scheme,
            fields => fields,
        })?;
        let output = workspace_dir.join(format!(
            "bindings/dotnet/OpenDAL/ServiceConfig/{class_name}ServiceConfig.cs"
        ));
        write_rendered(&output, rendered)?;
    }

    Ok(())
}

fn config_field(config: &Config) -> Result<ConfigField> {
    let pascal = heck::AsUpperCamelCase(&config.name).to_string();

    // Strings and durations stay nullable even when core requires them, so a
    // partially built config still converts and native-side validation stays
    // the single source of required-ness. Numeric requiredness is kept because
    // a non-nullable numeric property always has a value to emit.
    let (cs_type, check_null, value_expr) = match config.value {
        ConfigType::Bool => ("bool?", true, to_option_string(&pascal)),
        ConfigType::String | ConfigType::Duration => ("string?", true, to_option_string(&pascal)),
        ConfigType::Usize | ConfigType::U64 | ConfigType::I64 => {
            if config.optional {
                ("long?", true, to_option_string(&pascal))
            } else {
                ("long", false, to_option_string(&pascal))
            }
        }
        ConfigType::U32 | ConfigType::U16 => {
            if config.optional {
                ("int?", true, to_option_string(&pascal))
            } else {
                ("int", false, to_option_string(&pascal))
            }
        }
        ConfigType::Vec => (
            "IReadOnlyList<string>?",
            true,
            format!("string.Join(\",\", {pascal})"),
        ),
        ConfigType::HashMap => bail!(
            "config `{}` is a HashMap and should have been filtered out",
            config.name
        ),
    };

    let deprecated = match &config.deprecated {
        Some(attr) => cs_escape(&pascalize_backticks(&attr.note)),
        None => String::new(),
    };

    Ok(ConfigField {
        pascal,
        key: config.name.clone(),
        cs_type: cs_type.to_string(),
        check_null,
        value_expr,
        deprecated,
        doc: xml_escape(&single_line(&config.comments)),
    })
}

fn to_option_string(pascal: &str) -> String {
    format!("Utilities.ToOptionString({pascal})")
}

/// Collapse a multi-line doc into a single line.
fn single_line(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Rewrite backticked snake_case identifiers as the PascalCase property they
/// correspond to on this class, e.g. `` `skip_signature` `` -> `SkipSignature`.
fn pascalize_backticks(text: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find('`') {
        result.push_str(&rest[..start]);
        let after = &rest[start + 1..];
        match after.find('`') {
            Some(end) if after[..end].chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_') => {
                result.push_str(&heck::AsUpperCamelCase(&after[..end]).to_string());
                rest = &after[end + 1..];
            }
            _ => {
                result.push('`');
                rest = after;
            }
        }
    }
    result.push_str(rest);
    result
}

/// Escape the characters that are special inside C# XML doc comments.
fn xml_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Escape a string for embedding in a C# string literal.
fn cs_escape(text: &str) -> String {
    text.replace('\\', "\\\\").replace('"', "\\\"")
}

fn write_rendered(output: &std::path::Path, mut rendered: String) -> Result<()> {
    if !rendered.ends_with('\n') {
        rendered.push('\n');
    }
    fs::write(output, rendered)?;
    Ok(())
}
