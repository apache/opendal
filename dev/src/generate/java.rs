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
    details: Vec<String>,
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

const INSTANT_RANGE_JAVADOC: &str = concat!(
    "An {@link Instant} outside the Rust core timestamp range fails with ",
    "{@link OpenDALException.Code#Unexpected}."
);

fn option_class_details(name: &str) -> Vec<String> {
    let mut details = match name {
        "DeleteOptions" => vec![concat!(
            "All supplied conditions must hold for deletion to proceed. ",
            "Unsupported options fail with {@link OpenDALException.Code#Unsupported}. ",
            "Failed conditions fail with {@link OpenDALException.Code#ConditionNotMatch}."
        )],
        "ListOptions" => vec![concat!(
            "Options that require an unsupported service capability fail with ",
            "{@link OpenDALException.Code#Unsupported}. ",
            "{@link #limit} is a pagination hint that services may ignore even when ",
            "{@link Capability#listWithLimit} is supported."
        )],
        "ReadOptions" => vec![
            "Version selection and conditions require service support and apply to every chunk.",
            concat!(
                "All supplied conditions must hold for data to be returned. ",
                "When a direct read or {@link OperatorInputStream} issues a backend read, missing files fail with ",
                "{@link OpenDALException.Code#NotFound} and failed conditions on an existing file fail with ",
                "{@link OpenDALException.Code#ConditionNotMatch}. Unsupported version selection or conditions fail with ",
                "{@link OpenDALException.Code#Unsupported}. ",
                "Presigned read requests validate options and capabilities without executing against storage."
            ),
            concat!(
                "Invalid execution values fail with {@link OpenDALException.Code#ConfigInvalid}. ",
                "Invalid ranges fail with {@link OpenDALException.Code#RangeNotSatisfied}."
            ),
            INSTANT_RANGE_JAVADOC,
        ],
        "ReaderOptions" => vec![
            concat!(
                "All supplied conditions must hold for data to be returned. ",
                "When OpenDAL issues a backend read, missing files fail with ",
                "{@link OpenDALException.Code#NotFound} and failed conditions on an existing file fail with ",
                "{@link OpenDALException.Code#ConditionNotMatch}. Unsupported version selection or conditions fail with ",
                "{@link OpenDALException.Code#Unsupported}."
            ),
            concat!(
                "Depending on the service, errors can surface when creating the reader or while reading. ",
                "Conditions apply to every request, including streams."
            ),
            concat!(
                "Execution options apply independently of each logical range. ",
                "OpenDAL uses {@link #concurrent} and {@link #prefetch} only for chunked reads enabled by ",
                "{@link #chunk}. Invalid execution values fail with ",
                "{@link OpenDALException.Code#ConfigInvalid} when OpenDAL creates the reader."
            ),
            INSTANT_RANGE_JAVADOC,
        ],
        "StatOptions" => vec![
            concat!(
                "All supplied conditions must hold for metadata to be returned. ",
                "For direct stat operations, missing files fail with ",
                "{@link OpenDALException.Code#NotFound} and failed conditions on an existing file fail with ",
                "{@link OpenDALException.Code#ConditionNotMatch}. Unsupported version selection or conditions fail with ",
                "{@link OpenDALException.Code#Unsupported}. ",
                "Presigned stat requests validate options and capabilities without executing against storage."
            ),
            INSTANT_RANGE_JAVADOC,
        ],
        "WriteOptions" => vec![concat!(
            "All supplied conditions must hold for the write to commit. ",
            "Unsupported conditions or {@link #append} fail with {@link OpenDALException.Code#Unsupported}. ",
            "Unsupported metadata options may be ignored. ",
            "Failed conditions fail with {@link OpenDALException.Code#ConditionNotMatch}. ",
            "Presigned write requests validate options and capabilities without executing against storage."
        )],
        _ => Vec::new(),
    };
    details.push("Optional reference fields default to {@code null}.");
    details.into_iter().map(str::to_owned).collect()
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
                        summary: "Byte offset to start reading from.".to_owned(),
                        details: vec![concat!(
                            "Defaults to {@code 0L}. Negative values fail with ",
                            "{@link OpenDALException.Code#RangeNotSatisfied}."
                        )
                        .to_owned()],
                        see_also: Vec::new(),
                    },
                    OptionProperty {
                        camel: "length".to_owned(),
                        java_type: "long",
                        default_value: Some("-1L"),
                        summary: "Number of bytes to read from {@link #offset}.".to_owned(),
                        details: vec![
                            concat!(
                                "The default of {@code -1L} reads to the end, and {@code 0L} selects an empty range. ",
                                "For direct reads and {@link OperatorInputStream} with {@code length == 0L}, ",
                                "OpenDAL validates options and capabilities, then returns an empty result ",
                                "without accessing storage or evaluating conditions against the file. ",
                                "Presigning a read with this value still goes through request generation."
                            )
                            .to_owned(),
                            concat!(
                                "Values below {@code -1L} or an invalid {@code offset + length} range fail with ",
                                "{@link OpenDALException.Code#RangeNotSatisfied}."
                            )
                            .to_owned(),
                        ],
                        see_also: Vec::new(),
                    },
                ]);
                continue;
            }
            OptionKind::OptionDuration => bail!("unsupported Java option {}::{name}", options.name),
        };
        let (summary, details) = option_field_javadoc(spec, field);
        properties.push(OptionProperty {
            camel: case_java_field_name(name),
            java_type,
            default_value,
            summary,
            details,
            see_also: option_capability_refs(&field.doc),
        });
    }

    Ok(OptionsClass {
        name: &options.name,
        summary: match spec.name {
            "ReadOptions" => "Options for a single read or a compatibility {@link OperatorInputStream}.".to_owned(),
            "ReaderOptions" => "Configures version selection, read conditions, and execution options for an {@link OperatorReader}.".to_owned(),
            _ => option_javadoc(&options.doc, spec, None),
        },
        details: option_class_details(spec.name),
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

fn option_field_javadoc(spec: &OptionsSpec, field: &OptionField) -> (String, Vec<String>) {
    let name = field.name.as_str();
    let mut summary = option_javadoc(&field.doc, spec, Some(name));
    let mut details = Vec::new();
    match (spec.name, name) {
        ("DeleteOptions", "if_not_changed") => details.extend(field
            .doc
            .split(|line| line.is_empty())
            .skip(1)
            .filter(|paragraph| !paragraph.is_empty())
            .map(|paragraph| option_javadoc(paragraph, spec, Some(name)))),
        ("DeleteOptions", "recursive")
        | (_, "version" | "if_match" | "if_none_match" | "if_version_match" | "if_version_not_match"
            | "if_modified_since" | "if_unmodified_since" | "if_not_exists"
            | "override_content_type" | "override_cache_control" | "override_content_disposition") => {
            if let Some(paragraph) = field.doc.split(|line| line.is_empty()).nth(1) {
                details.push(option_javadoc(paragraph, spec, Some(name)));
            }
        }
        ("ReadOptions" | "ReaderOptions", "chunk") => {
            summary = "Target size of each internal range request in bytes.".to_owned();
            details.push(concat!(
                "The default of {@code -1L} keeps unchunked streaming. Positive values enable chunked reads ",
                "and must fit the native platform's unsigned pointer-sized integer. ",
                "{@code 0L}, other negative values, and values that do not fit fail with ",
                "{@link OpenDALException.Code#ConfigInvalid}."
            ).to_owned());
        }
        ("ReadOptions" | "ReaderOptions", "concurrent") => {
            summary = "Maximum number of internal chunk requests executed concurrently.".to_owned();
            details.push(concat!(
                "Defaults to {@code 1}. Values less than or equal to {@code 0}, or too large for the ",
                "native platform's unsigned pointer-sized integer, fail with ",
                "{@link OpenDALException.Code#ConfigInvalid}."
            ).to_owned());
            details.push(concat!(
                "This setting applies only to chunked reads enabled by {@link #chunk}. ",
                "Application transfer and Java thread counts are independent of this setting."
            ).to_owned());
        }
        ("ReaderOptions", "prefetch") => details.push(concat!(
            "This option counts completed chunks buffered ahead of consumption, regardless of their size in bytes. ",
            "The default of {@code 0} applies strict backpressure. Negative values fail with ",
            "{@link OpenDALException.Code#ConfigInvalid}. This setting applies only to ",
            "chunked reads enabled by {@link #chunk}."
        ).to_owned()),
        ("ReadOptions" | "ReaderOptions", "content_length_hint") => details.extend([
            concat!(
                "Supply the full object length in bytes, regardless of the requested range. ",
                "The default of {@code -1L} means the length is unknown. ",
                "Use {@code 0L} for an empty object. Values below {@code -1L} fail with ",
                "{@link OpenDALException.Code#ConfigInvalid}."
            )
            .to_owned(),
            concat!(
                "OpenDAL uses this hint only to plan reads and can use it to avoid a metadata request. ",
                "For chunked reads ({@link #chunk} set), a {@code contentLengthHint} no greater than the ",
                "starting offset can result in no backend read requests. When the hint equals the starting offset, ",
                "including {@code 0L} at the start of a file, OpenDAL may return empty data without checking file ",
                "existence or evaluating conditions. ",
                "An incorrect hint can truncate reads or cause errors."
            )
            .to_owned(),
        ]),
        ("ListOptions", "limit") => details.push(concat!(
            "Services may ignore this pagination hint even when {@link Capability#listWithLimit} is supported. ",
            "The default of {@code -1L} leaves the limit unset. Positive values must fit the ",
            "native platform's unsigned pointer-sized integer. {@code 0L}, other negative values, ",
            "and values that do not fit fail with {@link OpenDALException.Code#Unexpected}."
        ).to_owned()),
        ("WriteOptions", "concurrent") => details.push(concat!(
            "Defaults to {@code 1}. Values less than or equal to {@code 0}, or too large for the ",
            "native platform's unsigned pointer-sized integer, fail with ",
            "{@link OpenDALException.Code#Unexpected}."
        ).to_owned()),
        ("WriteOptions", "chunk") => details.push(concat!(
            "The default of {@code -1L} uses the service's default chunk size. ",
            "Positive values set the chunk size in bytes and must fit the native platform's ",
            "unsigned pointer-sized integer. {@code 0L}, other negative values, and values that ",
            "do not fit fail with {@link OpenDALException.Code#Unexpected}."
        ).to_owned()),
        _ => {}
    }
    if field.kind == OptionKind::OptionTimestamp {
        details.push(INSTANT_RANGE_JAVADOC.to_owned());
    }
    if name == "if_version_match"
        && matches!(spec.name, "ReadOptions" | "ReaderOptions" | "StatOptions")
    {
        let version = if spec.name == "StatOptions" {
            "{@code version}"
        } else {
            "{@link #version}"
        };
        details.push(format!(
            "This condition checks the identity of the file selected by {version}."
        ));
    }
    (summary, details)
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
        }
    }

    #[test]
    fn test_options_javadoc_contracts() {
        let structs = core_options_structs();
        let env = Environment::new();
        let template = env
            .template_from_str(include_str!("java/options_java.j2"))
            .unwrap();

        for class in options_classes(&structs).unwrap() {
            let rendered = template.render(context! { class => class }).unwrap();
            let class_doc = rendered
                .split_once("/**")
                .unwrap()
                .1
                .split_once("*/")
                .unwrap()
                .0;
            let assert_doc = |name: &str, javadoc: &str, notices: &[&str]| {
                for notice in notices {
                    assert!(
                        javadoc.contains(notice),
                        "{} {name} is missing {notice}",
                        class.name
                    );
                }
            };
            let assert_field = |field: &str, notices: &[&str]| {
                let declaration = rendered
                    .lines()
                    .find(|line| {
                        let (declaration, _) = line.split_once(['=', ';']).unwrap_or((line, ""));
                        !line.trim_start().starts_with('*')
                            && declaration.split_whitespace().last() == Some(field)
                    })
                    .unwrap();
                let (preceding, _) = rendered.split_once(declaration).unwrap();
                let (_, javadoc) = preceding.rsplit_once("/**").unwrap();
                assert_doc(field, javadoc, notices);
            };

            assert_doc(
                "class",
                class_doc,
                &[
                    "<p>Optional reference fields default to {@code null}.",
                    "fail with {@link OpenDALException.Code#Unsupported}",
                ],
            );
            if class.name != "ListOptions" {
                assert_doc(
                    "class",
                    class_doc,
                    &[
                        "<p>All supplied conditions must hold",
                        "fail with {@link OpenDALException.Code#ConditionNotMatch}",
                    ],
                );
            }

            match class.name {
                "DeleteOptions" => {
                    assert_doc("class", class_doc, &["for deletion to proceed"]);
                    assert_field(
                        "version",
                        &["selects which stored version the delete removes"],
                    );
                    assert_field(
                        "recursive",
                        &[
                            "The default, {@code false}, deletes a single object.",
                            "a trailing slash (for example, {@code \"a/\"}) scopes deletion to a directory and its descendants",
                            "{@code memory}, {@code \"a\"} also matches entries under {@code \"ab/\"}",
                        ],
                    );
                    assert_field(
                        "ifMatch",
                        &[
                            "or a missing file, fails the delete with {@link OpenDALException.Code#ConditionNotMatch}",
                            "a wildcard such as {@code \"*\"} has no portable meaning",
                        ],
                    );
                    assert_field(
                        "ifNoneMatch",
                        &[
                            "deleting a missing file is then a successful no-op",
                            "a wildcard such as {@code \"*\"} has no portable meaning",
                            "fails the delete with {@link OpenDALException.Code#ConditionNotMatch}",
                        ],
                    );
                    assert_field(
                        "ifVersionMatch",
                        &[
                            "or a missing file, fails the delete with {@link OpenDALException.Code#ConditionNotMatch}",
                        ],
                    );
                    assert_field(
                        "ifVersionNotMatch",
                        &[
                            "fails the delete with {@link OpenDALException.Code#ConditionNotMatch}",
                            "When no file exists at the delete path, no portable behavior is defined.",
                        ],
                    );
                    assert_field(
                        "ifNotChanged",
                        &[
                            "Pass metadata previously returned by OpenDAL for the same path.",
                            "OpenDAL derives a version match when the service supports version conditions and the metadata contains a version. Otherwise it uses a non-weak ETag if present.",
                            "A changed or missing file fails the delete with {@link OpenDALException.Code#ConditionNotMatch}, as does combining this option with a conflicting {@link #ifMatch} or {@link #ifVersionMatch} value.",
                            "{@link OpenDALException.Code#Unsupported} when the service does not support the derived primitive condition",
                            "{@link OpenDALException.Code#ConfigInvalid} when the metadata contains neither a version nor a non-weak ETag.",
                        ],
                    );
                }
                "ListOptions" => {
                    assert_doc(
                        "class",
                        class_doc,
                        &[
                            "Options that require an unsupported service capability fail with {@link OpenDALException.Code#Unsupported}.",
                            "{@link #limit} is a pagination hint that services may ignore even when {@link Capability#listWithLimit} is supported.",
                        ],
                    );
                    assert_field(
                        "limit",
                        &[
                            "Services may ignore this pagination hint even when {@link Capability#listWithLimit} is supported.",
                            "{@code -1L} leaves the limit unset",
                            "native platform's unsigned pointer-sized integer",
                            "{@code 0L}, other negative values, and values that do not fit fail with {@link OpenDALException.Code#Unexpected}",
                        ],
                    );
                }
                "ReadOptions" | "ReaderOptions" => {
                    assert_doc(
                        "class",
                        class_doc,
                        &[
                            "for data to be returned",
                            "Invalid execution values fail with {@link OpenDALException.Code#ConfigInvalid}",
                        ],
                    );
                    assert_field(
                        "chunk",
                        &[
                            "Target size of each internal range request in bytes.",
                            "{@code -1L} keeps unchunked streaming. Positive values enable chunked reads",
                            "native platform's unsigned pointer-sized integer",
                            "{@code 0L}, other negative values, and values that do not fit fail with {@link OpenDALException.Code#ConfigInvalid}",
                        ],
                    );
                    assert_field(
                        "concurrent",
                        &[
                            "Maximum number of internal chunk requests executed concurrently.",
                            "Defaults to {@code 1}. Values less than or equal to {@code 0}",
                            "native platform's unsigned pointer-sized integer, fail with {@link OpenDALException.Code#ConfigInvalid}",
                            "only to chunked reads enabled by {@link #chunk}",
                            "Application transfer and Java thread counts are independent",
                        ],
                    );
                    assert_field(
                        "contentLengthHint",
                        &[
                            "full object length in bytes, regardless of the requested range",
                            "{@code -1L} means the length is unknown",
                            "{@code 0L} for an empty object",
                            "Values below {@code -1L} fail with {@link OpenDALException.Code#ConfigInvalid}",
                            "OpenDAL uses this hint only to plan reads and can use it to avoid a metadata request.",
                            "For chunked reads ({@link #chunk} set), a {@code contentLengthHint} no greater than the starting offset can result in no backend read requests.",
                            "When the hint equals the starting offset, including {@code 0L} at the start of a file, OpenDAL may return empty data without checking file existence or evaluating conditions.",
                            "An incorrect hint can truncate reads or cause errors.",
                        ],
                    );
                    if class.name == "ReadOptions" {
                        assert_doc(
                            "class",
                            class_doc,
                            &[
                                "a single read or a compatibility {@link OperatorInputStream}",
                                "Version selection and conditions require service support and apply to every chunk.",
                                "When a direct read or {@link OperatorInputStream} issues a backend read, missing files fail with {@link OpenDALException.Code#NotFound}",
                                "Presigned read requests validate options and capabilities without executing against storage.",
                                "Invalid ranges fail with {@link OpenDALException.Code#RangeNotSatisfied}",
                            ],
                        );
                        assert_field(
                            "offset",
                            &[
                                "Byte offset to start reading from.",
                                "Defaults to {@code 0L}. Negative values fail with {@link OpenDALException.Code#RangeNotSatisfied}",
                            ],
                        );
                        assert_field(
                            "length",
                            &[
                                "Number of bytes to read from {@link #offset}",
                                "{@code -1L} reads to the end",
                                "{@code 0L} selects an empty range",
                                "For direct reads and {@link OperatorInputStream} with {@code length == 0L}, OpenDAL validates options and capabilities",
                                "returns an empty result without accessing storage or evaluating conditions against the file",
                                "Presigning a read with this value still goes through request generation.",
                                "Values below {@code -1L} or an invalid {@code offset + length} range fail with {@link OpenDALException.Code#RangeNotSatisfied}.",
                            ],
                        );
                    } else {
                        assert_doc(
                            "class",
                            class_doc,
                            &[
                                "execution options for an {@link OperatorReader}",
                                "When OpenDAL issues a backend read, missing files fail with {@link OpenDALException.Code#NotFound}",
                                "errors can surface when creating the reader or while reading",
                                "Conditions apply to every request, including streams.",
                                "Execution options apply independently of each logical range.",
                                "{@link #concurrent} and {@link #prefetch} only for chunked reads enabled by {@link #chunk}",
                                "{@link OpenDALException.Code#ConfigInvalid} when OpenDAL creates the reader",
                            ],
                        );
                        assert_field(
                            "prefetch",
                            &[
                                "counts completed chunks buffered ahead of consumption, regardless of their size in bytes",
                                "{@code 0} applies strict backpressure",
                                "Negative values fail with {@link OpenDALException.Code#ConfigInvalid}",
                                "chunked reads enabled by {@link #chunk}",
                            ],
                        );
                    }
                }
                "StatOptions" => assert_doc(
                    "class",
                    class_doc,
                    &[
                        "for metadata to be returned",
                        "For direct stat operations, missing files fail with {@link OpenDALException.Code#NotFound}",
                        "Presigned stat requests validate options and capabilities without executing against storage.",
                    ],
                ),
                "WriteOptions" => {
                    assert_doc(
                        "class",
                        class_doc,
                        &[
                            "for the write to commit",
                            "Unsupported conditions or {@link #append} fail with {@link OpenDALException.Code#Unsupported}",
                            "Unsupported metadata options may be ignored.",
                            "Presigned write requests validate options and capabilities without executing against storage.",
                        ],
                    );
                    assert_field(
                        "ifNotExists",
                        &[
                            "An existing file fails the write with {@link OpenDALException.Code#ConditionNotMatch}",
                        ],
                    );
                    assert_field(
                        "concurrent",
                        &[
                            "Defaults to {@code 1}. Values less than or equal to {@code 0}",
                            "native platform's unsigned pointer-sized integer, fail with {@link OpenDALException.Code#Unexpected}",
                        ],
                    );
                    assert_field(
                        "chunk",
                        &[
                            "{@code -1L} uses the service's default chunk size",
                            "Positive values set the chunk size in bytes",
                            "unsigned pointer-sized integer",
                            "{@code 0L}, other negative values, and values that do not fit fail with {@link OpenDALException.Code#Unexpected}",
                        ],
                    );
                }
                name => panic!("missing Javadoc expectations for {name}"),
            }

            if matches!(class.name, "ReadOptions" | "ReaderOptions" | "StatOptions") {
                let timestamp = "An {@link Instant} outside the Rust core timestamp range fails with {@link OpenDALException.Code#Unexpected}.";
                assert_doc(
                    "class",
                    class_doc,
                    &[
                        "and failed conditions on an existing file fail with {@link OpenDALException.Code#ConditionNotMatch}",
                        "Unsupported version selection or conditions fail with {@link OpenDALException.Code#Unsupported}",
                        timestamp,
                    ],
                );
                assert_field(
                    "version",
                    &[
                        "selects which stored version",
                        "A version that does not exist returns {@link OpenDALException.Code#NotFound}",
                    ],
                );
                assert_field(
                    "ifVersionMatch",
                    &[if class.name == "StatOptions" {
                        "checks the identity of the file selected by {@code version}"
                    } else {
                        "checks the identity of the file selected by {@link #version}"
                    }],
                );
                for field in ["ifModifiedSince", "ifUnmodifiedSince"] {
                    assert_field(
                        field,
                        &[
                            "a missing file returns {@link OpenDALException.Code#NotFound}",
                            timestamp,
                        ],
                    );
                }
            }
            if matches!(class.name, "ReadOptions" | "StatOptions") {
                for field in [
                    "overrideContentType",
                    "overrideCacheControl",
                    "overrideContentDisposition",
                ] {
                    assert_field(
                        field,
                        &["<p>This option is only meaningful when used along with presign."],
                    );
                }
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
