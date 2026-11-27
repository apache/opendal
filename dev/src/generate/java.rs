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
use anyhow::{Result, bail};
use minijinja::value::ViaDeserialize;
use minijinja::{Environment, context};
use serde::Serialize;
use std::fs;
use std::path::{Path, PathBuf};

use super::capability;
use super::metadata::{self, MetadataKind};

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

/// Order and types of the fields in the existing Java Metadata constructors.
const EXISTING_METADATA_FIELDS: &[(&str, MetadataKind)] = &[
    ("mode", MetadataKind::Mode),
    ("content_length", MetadataKind::U64),
    ("content_disposition", MetadataKind::OptionStr),
    ("content_md5", MetadataKind::OptionStr),
    ("content_type", MetadataKind::OptionStr),
    ("cache_control", MetadataKind::OptionStr),
    ("etag", MetadataKind::OptionStr),
    ("last_modified", MetadataKind::OptionTimestamp),
    ("version", MetadataKind::OptionStr),
    ("user_metadata", MetadataKind::OptionUserMetadata),
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

#[derive(Debug, Serialize)]
struct MetadataField {
    camel: String,
    doc: String,
    java_type: &'static str,
    constructor_type: &'static str,
    constructor_value: String,
    jni_type: &'static str,
    rust_binding: String,
    jni_argument: String,
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
    generate_capability(&workspace_dir)?;
    generate_metadata(&workspace_dir)
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

fn select_metadata_fields(mut parsed: Vec<metadata::MetadataField>) -> Result<Vec<MetadataField>> {
    EXISTING_METADATA_FIELDS
        .iter()
        .map(|&(name, expected)| {
            let mut matches = parsed.extract_if(.., |field| field.name == name);
            let Some(field) = matches.next() else {
                bail!("core Metadata is missing Java field `{name}`");
            };
            if matches.next().is_some() {
                bail!("core Metadata contains duplicate Java field `{name}`");
            }
            if field.kind != expected {
                bail!(
                    "core Metadata field `{name}` has kind {:?}; Java requires {expected:?}",
                    field.kind
                );
            }

            let accessor = format!("metadata.{name}()");
            let (java_type, jni_type, rust_binding, jni_argument) = match field.kind {
                MetadataKind::Mode => (
                    "EntryMode",
                    "I",
                    format!("let {name} = convert::entry_mode_to_jint({accessor});"),
                    format!("JValue::Int({name})"),
                ),
                MetadataKind::U64 => (
                    "long",
                    "J",
                    format!("let {name} = {accessor} as jlong;"),
                    format!("JValue::Long({name})"),
                ),
                MetadataKind::OptionStr => (
                    "String",
                    "Ljava/lang/String;",
                    format!("let {name} = convert::string_to_jstring(env, {accessor})?;"),
                    format!("JValue::Object(&{name})"),
                ),
                MetadataKind::OptionTimestamp => (
                    "Instant",
                    "Ljava/time/Instant;",
                    format!("let {name} = convert::timestamp_to_jinstant(env, {accessor})?;"),
                    format!("JValue::Object(&{name})"),
                ),
                MetadataKind::OptionUserMetadata => (
                    "Map<String, String>",
                    "Ljava/util/Map;",
                    format!(
                        "let {name} =\n        convert::hashmap_to_jmap(env, {accessor}.into_iter().flatten())?;"
                    ),
                    format!("JValue::Object(&{name})"),
                ),
                _ => bail!("unsupported Java Metadata kind {expected:?} for `{name}`"),
            };
            let camel = case_java_field_name(name);
            let (constructor_type, constructor_value) = if field.kind == MetadataKind::Mode {
                ("int", format!("EntryMode.of({camel})"))
            } else {
                (java_type, camel.clone())
            };
            let doc = if name == "user_metadata" {
                "User-defined metadata of this entry. Empty when the service returns no user metadata."
            } else {
                &field.doc
            };
            let doc = markdown::to_html(doc);
            let doc = doc
                .strip_prefix("<p>")
                .and_then(|doc| doc.strip_suffix("</p>"))
                .unwrap_or(&doc)
                .to_owned();

            Ok(MetadataField {
                camel,
                doc,
                java_type,
                constructor_type,
                constructor_value,
                jni_type,
                rust_binding,
                jni_argument,
            })
        })
        .collect()
}

fn generate_metadata(workspace_dir: &Path) -> Result<()> {
    let source = workspace_dir.join("core/core/src/types/metadata.rs");
    let fields = select_metadata_fields(metadata::parse(&source)?)?;

    let mut env = Environment::new();
    env.add_template("metadata_java", include_str!("java/metadata_java.j2"))?;
    env.add_template("metadata_rs", include_str!("java/metadata_rs.j2"))?;

    let outputs = [
        (
            "metadata_java",
            "bindings/java/src/main/java/org/apache/opendal/Metadata.java",
        ),
        ("metadata_rs", "bindings/java/src/metadata.rs"),
    ];
    for (template, relative) in outputs {
        let tmpl = env.get_template(template)?;
        let rendered = tmpl.render(context! { fields => fields })?;
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

    fn core_metadata_fields() -> Vec<metadata::MetadataField> {
        let source = crate::workspace_dir().join("core/core/src/types/metadata.rs");
        metadata::parse(&source).expect("core metadata must parse")
    }

    #[test]
    fn test_metadata_fields_follow_existing_java_order() {
        let mut parsed = core_metadata_fields();
        parsed.reverse();
        let fields = select_metadata_fields(parsed).unwrap();
        let names: Vec<_> = fields.iter().map(|field| field.camel.as_str()).collect();
        assert_eq!(
            names,
            [
                "mode",
                "contentLength",
                "contentDisposition",
                "contentMd5",
                "contentType",
                "cacheControl",
                "etag",
                "lastModified",
                "version",
                "userMetadata",
            ]
        );
    }

    #[test]
    fn test_metadata_fields_reject_missing_field() {
        let mut parsed = core_metadata_fields();
        parsed.retain(|field| field.name != "content_type");
        let err = select_metadata_fields(parsed).unwrap_err();
        assert_eq!(
            err.to_string(),
            "core Metadata is missing Java field `content_type`"
        );
    }

    #[test]
    fn test_metadata_fields_reject_duplicate_field() {
        let mut parsed = core_metadata_fields();
        parsed.push(metadata::MetadataField {
            name: "etag".to_owned(),
            kind: MetadataKind::OptionStr,
            doc: String::new(),
        });
        let err = select_metadata_fields(parsed).unwrap_err();
        assert_eq!(
            err.to_string(),
            "core Metadata contains duplicate Java field `etag`"
        );
    }

    #[test]
    fn test_metadata_fields_reject_mismatched_kind() {
        let mut parsed = core_metadata_fields();
        parsed
            .iter_mut()
            .find(|field| field.name == "mode")
            .unwrap()
            .kind = MetadataKind::U64;
        let err = select_metadata_fields(parsed).unwrap_err();
        assert_eq!(
            err.to_string(),
            "core Metadata field `mode` has kind U64; Java requires Mode"
        );
    }

    #[test]
    fn test_metadata_jni_constructor_descriptor() {
        let fields = select_metadata_fields(core_metadata_fields()).unwrap();
        let rendered = Environment::new()
            .render_str(
                include_str!("java/metadata_rs.j2"),
                context! { fields => fields },
            )
            .unwrap();
        assert!(rendered.contains(
            "\"(IJLjava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/time/Instant;Ljava/lang/String;Ljava/util/Map;)V\""
        ));
    }
}
