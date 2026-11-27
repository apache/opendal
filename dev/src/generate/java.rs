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
use regex::{Captures, Regex};
use serde::Serialize;
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use super::capability;
use super::core_options::{self, OptionField, OptionKind, OptionsStruct};
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

struct OptionsSpec {
    name: &'static str,
    fields: &'static [(&'static str, OptionKind)],
}

/// Field order and kinds used by the existing Java constructors and builders.
const EXISTING_OPTIONS_FIELDS: &[OptionsSpec] = {
    use OptionKind::*;

    &[
        OptionsSpec {
            name: "DeleteOptions",
            fields: &[
                ("version", OptionString),
                ("recursive", Bool),
                ("if_match", OptionString),
                ("if_none_match", OptionString),
                ("if_version_match", OptionString),
                ("if_version_not_match", OptionString),
                ("if_not_changed", OptionMetadata),
            ],
        },
        OptionsSpec {
            name: "ListOptions",
            fields: &[
                ("recursive", Bool),
                ("limit", OptionUsize),
                ("start_after", OptionString),
                ("versions", Bool),
                ("deleted", Bool),
            ],
        },
        OptionsSpec {
            name: "ReadOptions",
            fields: &[
                ("range", BytesRange),
                ("version", OptionString),
                ("if_match", OptionString),
                ("if_none_match", OptionString),
                ("if_version_match", OptionString),
                ("if_version_not_match", OptionString),
                ("if_modified_since", OptionTimestamp),
                ("if_unmodified_since", OptionTimestamp),
                ("concurrent", Usize),
                ("chunk", OptionUsize),
                ("content_length_hint", OptionU64),
                ("override_content_type", OptionString),
                ("override_cache_control", OptionString),
                ("override_content_disposition", OptionString),
            ],
        },
        OptionsSpec {
            name: "ReaderOptions",
            fields: &[
                ("version", OptionString),
                ("if_match", OptionString),
                ("if_none_match", OptionString),
                ("if_version_match", OptionString),
                ("if_version_not_match", OptionString),
                ("if_modified_since", OptionTimestamp),
                ("if_unmodified_since", OptionTimestamp),
                ("concurrent", Usize),
                ("chunk", OptionUsize),
                ("prefetch", Usize),
                ("content_length_hint", OptionU64),
            ],
        },
        OptionsSpec {
            name: "StatOptions",
            fields: &[
                ("if_match", OptionString),
                ("if_none_match", OptionString),
                ("if_version_match", OptionString),
                ("if_version_not_match", OptionString),
                ("if_modified_since", OptionTimestamp),
                ("if_unmodified_since", OptionTimestamp),
                ("version", OptionString),
                ("override_content_type", OptionString),
                ("override_cache_control", OptionString),
                ("override_content_disposition", OptionString),
            ],
        },
        OptionsSpec {
            name: "WriteOptions",
            fields: &[
                ("content_type", OptionString),
                ("content_disposition", OptionString),
                ("cache_control", OptionString),
                ("content_encoding", OptionString),
                ("if_match", OptionString),
                ("if_none_match", OptionString),
                ("user_metadata", OptionStringMap),
                ("append", Bool),
                ("if_not_exists", Bool),
                ("concurrent", Usize),
                ("chunk", OptionUsize),
            ],
        },
    ]
};

#[derive(Serialize)]
struct OptionsClass<'a> {
    name: &'a str,
    summary: String,
    imports: BTreeSet<&'static str>,
    properties: Vec<OptionProperty>,
    rs_name: String,
    rs_fields: Vec<OptionRsField<'a>>,
    has_unmirrored_fields: bool,
}

#[derive(Serialize)]
struct OptionRsField<'a> {
    name: &'a str,
    expr: String,
}

#[derive(Serialize)]
struct OptionProperty {
    camel: String,
    java_type: &'static str,
    default_value: Option<&'static str>,
    summary: String,
    details: Vec<String>,
    see_also: Vec<String>,
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
    generate_metadata(&workspace_dir)?;
    generate_options(&workspace_dir)
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

fn generate_options(workspace_dir: &Path) -> Result<()> {
    let source = workspace_dir.join("core/core/src/types/options.rs");
    let parsed = core_options::parse(&source)?;
    let env = Environment::new();
    let template = env.template_from_str(include_str!("java/options_java.j2"))?;
    let java_dir = workspace_dir.join("bindings/java/src/main/java/org/apache/opendal");

    let classes = options_classes(&parsed)?;
    for class in &classes {
        let rendered = template.render(context! { class => class })?;
        fs::write(
            java_dir.join(format!("{}.java", class.name)),
            format!("{rendered}\n"),
        )?;
    }

    let rendered = env.render_str(
        include_str!("java/options_rs.j2"),
        context! { classes => classes },
    )?;
    fs::write(
        workspace_dir.join("bindings/java/src/options.rs"),
        format!("{rendered}\n"),
    )?;
    Ok(())
}

fn options_classes(parsed: &[OptionsStruct]) -> Result<Vec<OptionsClass<'_>>> {
    let mut classes = Vec::new();
    for spec in EXISTING_OPTIONS_FIELDS {
        let Some(options) = parsed.iter().find(|options| options.name == spec.name) else {
            bail!("core options are missing Java struct `{}`", spec.name);
        };
        let fields = select_options_fields(options, spec)?;
        classes.push(options_class(options, spec, &fields)?);
    }
    Ok(classes)
}

fn select_options_fields<'a>(
    options: &'a OptionsStruct,
    spec: &OptionsSpec,
) -> Result<Vec<&'a OptionField>> {
    let mut by_name = HashMap::new();
    for field in &options.fields {
        if by_name.insert(field.name.as_str(), field).is_some() {
            bail!(
                "core {} contains duplicate field `{}`",
                options.name,
                field.name
            );
        }
    }

    spec.fields
        .iter()
        .map(|&(name, expected)| {
            let Some(&field) = by_name.get(name) else {
                bail!("core {} is missing Java field `{name}`", options.name);
            };
            if field.kind != expected {
                bail!(
                    "core {} field `{name}` has kind {:?}; Java requires {expected:?}",
                    options.name,
                    field.kind
                );
            }
            Ok(field)
        })
        .collect()
}

fn options_class<'a>(
    options: &'a OptionsStruct,
    spec: &OptionsSpec,
    fields: &[&OptionField],
) -> Result<OptionsClass<'a>> {
    let mut imports = BTreeSet::new();
    let mut properties = Vec::new();
    for field in fields {
        let name = field.name.as_str();
        let (java_type, default_value) = match field.kind {
            OptionKind::Bool => ("boolean", None),
            OptionKind::Usize if name == "prefetch" => ("int", Some("0")),
            OptionKind::Usize => ("int", Some("1")),
            OptionKind::OptionUsize | OptionKind::OptionU64 => ("long", Some("-1L")),
            OptionKind::OptionString => ("String", None),
            OptionKind::OptionTimestamp => {
                imports.insert("java.time.Instant");
                ("Instant", None)
            }
            OptionKind::OptionStringMap => {
                imports.insert("java.util.Map");
                ("Map<String, String>", None)
            }
            OptionKind::OptionMetadata => ("Metadata", None),
            OptionKind::BytesRange => {
                properties.extend([
                    OptionProperty {
                        camel: "offset".to_owned(),
                        java_type: "long",
                        default_value: None,
                        summary: "The byte offset to start reading from. Defaults to zero.".to_owned(),
                        details: Vec::new(),
                        see_also: Vec::new(),
                    },
                    OptionProperty {
                        camel: "length".to_owned(),
                        java_type: "long",
                        default_value: Some("-1L"),
                        summary: "The number of bytes to read from {@link #offset}. The default of {@code -1L} reads to the end.".to_owned(),
                        details: Vec::new(),
                        see_also: Vec::new(),
                    },
                ]);
                continue;
            }
            OptionKind::OptionDuration => bail!("unsupported Java option {}::{name}", options.name),
        };
        let details = match (options.name.as_str(), name) {
            ("ReadOptions" | "ReaderOptions", "content_length_hint") => vec![
                concat!(
                    "Supply the full object length in bytes, regardless of the requested range. ",
                    "The default of {@code -1L} means the length is unknown. ",
                    "Use {@code 0L} for an empty object. Values below {@code -1L} fail with ",
                    "{@link OpenDALException.Code#ConfigInvalid}."
                )
                .to_owned(),
                concat!(
                    "OpenDAL can use this hint to avoid a metadata request when planning reads. ",
                    "An incorrect hint can truncate reads or cause errors."
                )
                .to_owned(),
            ],
            _ => Vec::new(),
        };
        properties.push(OptionProperty {
            camel: case_java_field_name(name),
            java_type,
            default_value,
            summary: option_javadoc(&field.doc, spec, Some(name)),
            details,
            see_also: option_capability_refs(&field.doc),
        });
    }

    Ok(OptionsClass {
        name: &options.name,
        summary: option_javadoc(&options.doc, spec, None),
        imports,
        properties,
        rs_name: heck::AsSnakeCase(options.name.as_str()).to_string(),
        rs_fields: options
            .fields
            .iter()
            .map(|field| {
                let expr = if fields.iter().any(|exposed| exposed.name == field.name) {
                    option_rs_expr(&options.name, field)?
                } else {
                    match field.kind {
                        OptionKind::Bool => "false",
                        OptionKind::Usize => "0",
                        OptionKind::BytesRange => "Default::default()",
                        OptionKind::OptionUsize
                        | OptionKind::OptionU64
                        | OptionKind::OptionString
                        | OptionKind::OptionTimestamp
                        | OptionKind::OptionDuration
                        | OptionKind::OptionStringMap
                        | OptionKind::OptionMetadata => "None",
                    }
                    .to_owned()
                };
                Ok(OptionRsField {
                    name: &field.name,
                    expr,
                })
            })
            .collect::<Result<_>>()?,
        has_unmirrored_fields: options.has_unmirrored_fields,
    })
}

fn option_rs_expr(options: &str, field: &OptionField) -> Result<String> {
    let name = field.name.as_str();
    let camel = case_java_field_name(name);
    let (converter, suffix) = match field.kind {
        OptionKind::Bool if options != "DeleteOptions" => {
            ("read_bool_field", ".unwrap_or_default()")
        }
        OptionKind::Bool => ("read_bool_field", "?"),
        OptionKind::OptionUsize if matches!(options, "ListOptions" | "WriteOptions") => {
            ("read_jlong_field_to_usize", "?")
        }
        OptionKind::Usize
        | OptionKind::OptionUsize
        | OptionKind::OptionU64
        | OptionKind::OptionMetadata => {
            // These fields are read before building the struct to keep validation precedence.
            return Ok(name.to_owned());
        }
        OptionKind::OptionString => ("read_string_field", "?"),
        OptionKind::OptionTimestamp => ("read_instant_field_to_timestamp", "?"),
        OptionKind::OptionStringMap => ("read_map_field", "?"),
        OptionKind::BytesRange => return Ok("convert::read_range_fields(env, options)?".to_owned()),
        OptionKind::OptionDuration => bail!("unsupported Java option {options}::{name}"),
    };

    let expr = format!("convert::{converter}(env, options, \"{camel}\"){suffix}");
    if 8 + name.len() + 2 + expr.len() + 1 > 100 {
        return Ok(format!(
            "convert::{converter}(\n            env,\n            options,\n            \"{camel}\",\n        ){suffix}"
        ));
    }
    Ok(expr)
}

/// Translate inline Rustdoc links and code spans in the first paragraph.
fn option_javadoc(lines: &[String], spec: &OptionsSpec, current: Option<&str>) -> String {
    static INLINE: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"\[(?P<label>[^\]]+)\](?:\((?P<url>[^)]+)\)|\[(?P<reference>[^\]]+)\])|\[`(?P<path>[^`]+)`\]|`(?P<code>[^`]+)`")
            .expect("valid inline Rustdoc regex")
    });
    let paragraph = lines
        .iter()
        .take_while(|line| !line.is_empty())
        .cloned()
        .collect::<Vec<_>>()
        .join(" ");
    let mut summary = String::new();
    let mut end = 0;
    for captures in INLINE.captures_iter(&paragraph) {
        let token = captures.get(0).expect("matched inline token");
        summary.push_str(&escape_javadoc(&paragraph[end..token.start()]));
        summary.push_str(&option_javadoc_token(&captures, spec, current));
        end = token.end();
    }
    summary.push_str(&escape_javadoc(&paragraph[end..]));
    summary
}

fn option_javadoc_token(
    captures: &Captures<'_>,
    spec: &OptionsSpec,
    current: Option<&str>,
) -> String {
    let group = |name| captures.name(name).map(|value| value.as_str());
    if let Some(url) = group("url")
        && (url.starts_with("https://") || url.starts_with("http://"))
    {
        return format!(
            "<a href=\"{}\">{}</a>",
            escape_javadoc(url).replace('"', "&quot;"),
            escape_javadoc(group("label").expect("link has a label").trim_matches('`')),
        );
    }

    let target = group("url")
        .or_else(|| group("reference"))
        .or_else(|| group("path"))
        .or_else(|| group("code"))
        .expect("inline token has a target");
    let target = target.trim_matches('`');
    let path = target.strip_prefix("crate::").unwrap_or(target);
    let sibling = path.strip_prefix("Self::").unwrap_or(path);
    if current != Some(sibling)
        && spec
            .fields
            .iter()
            .any(|&(name, kind)| name == sibling && kind != OptionKind::BytesRange)
    {
        return format!("{{@link #{}}}", case_java_field_name(sibling));
    }

    let (ty, member) = path.split_once("::").unwrap_or((path, ""));
    match (ty, member) {
        ("Capability" | "Metadata", "") => format!("{{@link {ty}}}"),
        ("Capability", field) => format!("{{@link Capability#{}}}", case_java_field_name(field)),
        ("Metadata", field)
            if EXISTING_METADATA_FIELDS
                .iter()
                .any(|&(name, _)| name == field) =>
        {
            format!("{{@link Metadata#{}}}", case_java_field_name(field))
        }
        ("ErrorKind", "") => "{@link OpenDALException.Code}".to_owned(),
        ("ErrorKind", variant) => format!("{{@link OpenDALException.Code#{variant}}}"),
        (ty, "") if EXISTING_OPTIONS_FIELDS.iter().any(|spec| spec.name == ty) => {
            format!("{{@link {ty}}}")
        }
        (ty, field)
            if EXISTING_OPTIONS_FIELDS.iter().any(|spec| {
                spec.name == ty
                    && spec
                        .fields
                        .iter()
                        .any(|&(name, kind)| name == field && kind != OptionKind::BytesRange)
            }) =>
        {
            format!("{{@link {ty}#{}}}", case_java_field_name(field))
        }
        _ => format!("{{@code {path}}}"),
    }
}

fn escape_javadoc(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn option_capability_refs(lines: &[String]) -> Vec<String> {
    static CAPABILITY: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"\bCapability::([a-z][a-z0-9_]*)").expect("valid capability reference regex")
    });
    let mut refs = Vec::new();
    for captures in lines.iter().flat_map(|line| CAPABILITY.captures_iter(line)) {
        let cref = format!("Capability#{}", case_java_field_name(&captures[1]));
        if !refs.contains(&cref) {
            refs.push(cref);
        }
    }
    refs
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

    fn core_options_structs() -> Vec<OptionsStruct> {
        let path = crate::workspace_dir().join("core/core/src/types/options.rs");
        core_options::parse(&path).expect("core options must parse")
    }

    #[test]
    fn test_options_fields_follow_existing_java_order() {
        let expected = [
            (
                "DeleteOptions",
                "version recursive ifMatch ifNoneMatch ifVersionMatch ifVersionNotMatch ifNotChanged",
            ),
            ("ListOptions", "recursive limit startAfter versions deleted"),
            (
                "ReadOptions",
                "offset length version ifMatch ifNoneMatch ifVersionMatch ifVersionNotMatch ifModifiedSince ifUnmodifiedSince concurrent chunk contentLengthHint overrideContentType overrideCacheControl overrideContentDisposition",
            ),
            (
                "ReaderOptions",
                "version ifMatch ifNoneMatch ifVersionMatch ifVersionNotMatch ifModifiedSince ifUnmodifiedSince concurrent chunk prefetch contentLengthHint",
            ),
            (
                "StatOptions",
                "ifMatch ifNoneMatch ifVersionMatch ifVersionNotMatch ifModifiedSince ifUnmodifiedSince version overrideContentType overrideCacheControl overrideContentDisposition",
            ),
            (
                "WriteOptions",
                "contentType contentDisposition cacheControl contentEncoding ifMatch ifNoneMatch userMetadata append ifNotExists concurrent chunk",
            ),
        ];
        let mut structs = core_options_structs();
        assert_eq!(EXISTING_OPTIONS_FIELDS.len(), expected.len());
        let env = Environment::new();
        let template = env
            .template_from_str(include_str!("java/options_java.j2"))
            .unwrap();

        for (spec, (name, names)) in EXISTING_OPTIONS_FIELDS.iter().zip(expected) {
            assert_eq!(spec.name, name);
            let options = structs
                .iter_mut()
                .find(|options| options.name == name)
                .unwrap();
            options.fields.reverse();
            let fields = select_options_fields(options, spec).unwrap();
            let class = options_class(options, spec, &fields).unwrap();
            let names: Vec<_> = names.split_whitespace().collect();
            assert_eq!(
                class
                    .properties
                    .iter()
                    .map(|p| p.camel.as_str())
                    .collect::<Vec<_>>(),
                names,
                "{name}"
            );

            let rendered = template.render(context! { class => class }).unwrap();
            let (annotations, modifiers) = match name {
                "StatOptions" => ("@Data\n@Builder\npublic class", "private"),
                "ReaderOptions" => ("@Builder\npublic final class", "public final"),
                _ => ("@Builder\npublic class", "public final"),
            };
            assert!(rendered.contains(&format!("{annotations} {name} {{")));
            assert_eq!(rendered.contains("@Data"), name == "StatOptions");
            assert!(!rendered.contains("toBuilder"));
            assert_eq!(
                rendered.matches(&format!("    {modifiers} ")).count(),
                names.len()
            );
            if name == "DeleteOptions" {
                assert!(rendered.contains("Use a trailing slash (for example, {@code \"a/\"})"));
                assert!(rendered.contains(
                    "such as {@code \"a\"} can also match entries under {@code \"ab/\"}"
                ));
            }
        }
    }

    #[test]
    fn test_options_javadoc_notices() {
        let structs = core_options_structs();
        let env = Environment::new();
        let template = env
            .template_from_str(include_str!("java/options_java.j2"))
            .unwrap();

        for class in options_classes(&structs).unwrap() {
            let (declaration, notices): (&str, &[&str]) = match class.name {
                "ReadOptions" | "ReaderOptions" => (
                    "public final long contentLengthHint",
                    &[
                        "full object length in bytes, regardless of the requested range",
                        "{@code -1L} means the length is unknown",
                        "An incorrect hint can truncate reads or cause errors.",
                    ],
                ),
                _ => continue,
            };
            let rendered = template.render(context! { class => class }).unwrap();
            let (preceding, _) = rendered.split_once(declaration).unwrap();
            let (_, javadoc) = preceding.rsplit_once("/**").unwrap();
            for notice in notices {
                assert!(
                    javadoc.contains(notice),
                    "{} is missing {notice}",
                    class.name
                );
            }
        }
    }

    #[test]
    fn test_options_fields_reject_missing_field() {
        let mut options = core_options_structs()
            .into_iter()
            .find(|options| options.name == "ListOptions")
            .unwrap();
        let spec = EXISTING_OPTIONS_FIELDS
            .iter()
            .find(|spec| spec.name == options.name)
            .unwrap();
        options.fields.retain(|field| field.name != "limit");
        let err = select_options_fields(&options, spec).unwrap_err();
        assert_eq!(
            err.to_string(),
            "core ListOptions is missing Java field `limit`"
        );
    }

    #[test]
    fn test_options_fields_reject_duplicate_unexposed_field() {
        let mut options = core_options_structs()
            .into_iter()
            .find(|options| options.name == "ListOptions")
            .unwrap();
        let spec = EXISTING_OPTIONS_FIELDS
            .iter()
            .find(|spec| spec.name == options.name)
            .unwrap();
        options.fields.push(
            options
                .fields
                .iter()
                .find(|field| field.name == "glob")
                .unwrap()
                .clone(),
        );
        let err = select_options_fields(&options, spec).unwrap_err();
        assert_eq!(
            err.to_string(),
            "core ListOptions contains duplicate field `glob`"
        );
    }

    #[test]
    fn test_options_fields_reject_mismatched_kind() {
        let mut options = core_options_structs()
            .into_iter()
            .find(|options| options.name == "ListOptions")
            .unwrap();
        let spec = EXISTING_OPTIONS_FIELDS
            .iter()
            .find(|spec| spec.name == options.name)
            .unwrap();
        options
            .fields
            .iter_mut()
            .find(|field| field.name == "limit")
            .unwrap()
            .kind = OptionKind::OptionU64;
        let err = select_options_fields(&options, spec).unwrap_err();
        assert_eq!(
            err.to_string(),
            "core ListOptions field `limit` has kind OptionU64; Java requires OptionUsize"
        );
    }

    #[test]
    fn test_option_javadoc_translates_inline_links() {
        let spec = EXISTING_OPTIONS_FIELDS
            .iter()
            .find(|spec| spec.name == "ListOptions")
            .unwrap();
        let lines = [
            "Use [`Self::start_after`] & [`crate::Capability::list_with_limit`]",
            "or [the guide](https://example.com/list?a=b&c=d).",
            "See [`Self::glob`], [`ReadOptions::chunk`], [`ReadOptions::gap`],",
            "[`CopyOptions`], [`crate::Reader::fetch`], and [`Metadata::checksum`].",
            "",
            "Check `Capability::list_with_limit` and `Capability::list_with_versions`.",
        ]
        .map(str::to_owned);
        assert_eq!(
            option_javadoc(&lines, spec, None),
            concat!(
                "Use {@link #startAfter} &amp; {@link Capability#listWithLimit} ",
                "or <a href=\"https://example.com/list?a=b&amp;c=d\">the guide</a>. ",
                "See {@code Self::glob}, {@link ReadOptions#chunk}, {@code ReadOptions::gap}, ",
                "{@code CopyOptions}, {@code Reader::fetch}, and {@code Metadata::checksum}."
            )
        );
        assert_eq!(
            option_capability_refs(&lines),
            ["Capability#listWithLimit", "Capability#listWithVersions"]
        );
    }

    #[test]
    fn test_options_jni_defaults_for_unexposed_fields() {
        let parsed = core_options_structs();
        let classes = options_classes(&parsed).unwrap();
        let rendered = Environment::new()
            .render_str(
                include_str!("java/options_rs.j2"),
                context! { classes => classes },
            )
            .unwrap();

        let expected: &[(&str, &[&str])] = &[
            ("delete", &[]),
            ("list", &["glob: None,"]),
            ("read", &["limit: None,", "gap: None,", "checksum: false,"]),
            ("reader", &["gap: None,", "checksum: false,"]),
            ("stat", &["checksum: false,"]),
            (
                "write",
                &[
                    "content_language: None,",
                    "expires: None,",
                    "if_version_match: None,",
                    "if_version_not_match: None,",
                    "if_not_changed: None,",
                ],
            ),
        ];
        let converters: Vec<_> = rendered.split("pub(crate) fn make_").skip(1).collect();
        assert_eq!(converters.len(), expected.len());
        for (converter, &(name, defaults)) in converters.iter().zip(expected) {
            assert!(converter.starts_with(&format!("{name}_options(")));
            for default in defaults {
                assert!(
                    converter.lines().any(|line| line.trim() == *default),
                    "{name} must initialize {default}"
                );
            }
            assert_eq!(converter.contains("..Default::default()"), name == "write");
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
