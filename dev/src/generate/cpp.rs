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

//! Generator for the C++ binding.
//!
//! Capability crosses three layers in C++: the cxx shared struct, the Rust
//! conversion from core, and the public `opendal::Capability` class plus the
//! copy between them. All three are rendered from the same field list.
//! `opendal::Metadata` crosses the same three layers and is rendered from the
//! core accessor list.
//!
//! Operation options cross four layers: the public struct, the cxx shared
//! struct, the C++ `ToFfiOptions` conversion, and the Rust conversion into
//! core. All four are rendered from `OPTIONS`.

use super::capability;
use super::core_options::{self, OptionKind};
use super::metadata::{self, MetadataKind};
use super::rustdoc::rewrite_code_spans;
use anyhow::{Context, Result, bail};
use minijinja::value::Serde;
use minijinja::{Environment, context};
use serde::Serialize;
use std::fs;
use std::path::Path;

/// Render model handed to the C++ capability templates.
#[derive(Serialize)]
struct CapabilityField {
    /// snake_case field name in core `opendal::Capability`.
    name: String,
    /// Field name on the C++ side, escaped when it is a C++ keyword.
    cpp_name: String,
    /// Plain `bool` flag when true, `Option<usize>` limit when false.
    is_bool: bool,
    /// Single-line doc text from core.
    doc: String,
    /// `doc` split into lines that fit the header's `///` comments within
    /// clang-format's column limit, so the generated header is format-stable.
    hpp_doc_lines: Vec<String>,
}

/// clang-format `ColumnLimit` for the Google style used by `bindings/cpp`.
const HPP_COLUMN_LIMIT: usize = 80;
/// Prefix of a member doc comment in the generated header.
const HPP_DOC_PREFIX: &str = "  /// ";

/// Greedily wrap `text` at word boundaries so each line fits after `prefix`
/// within `limit` columns, matching clang-format's comment reflow.
fn wrap_doc(text: &str, prefix: &str, limit: usize) -> Vec<String> {
    let width = limit - prefix.len();
    let mut lines = Vec::new();
    let mut line = String::new();
    for word in text.split_whitespace() {
        if !line.is_empty() && line.len() + 1 + word.len() > width {
            lines.push(std::mem::take(&mut line));
        }
        if !line.is_empty() {
            line.push(' ');
        }
        line.push_str(word);
    }
    if !line.is_empty() {
        lines.push(line);
    }
    lines
}

/// Core names of the public `opendal::Capability` members, in their released
/// declaration order.
///
/// The public class is an aggregate, so callers may brace-initialize it by
/// position. This order is append-only: generation fails until every core
/// field is listed, so append each new core field to the end of this list.
const FROZEN_PUBLIC_ORDER: &[&str] = &[
    "stat",
    "stat_with_if_match",
    "stat_with_if_none_match",
    "read",
    "read_with_if_match",
    "read_with_if_none_match",
    "read_with_override_cache_control",
    "read_with_override_content_disposition",
    "read_with_override_content_type",
    "write",
    "write_can_multi",
    "write_can_empty",
    "write_can_append",
    "write_with_content_type",
    "write_with_content_disposition",
    "write_with_cache_control",
    "write_multi_max_size",
    "write_multi_min_size",
    "write_total_max_size",
    "create_dir",
    "delete",
    "copy",
    "rename",
    "list",
    "list_with_limit",
    "list_with_start_after",
    "list_with_recursive",
    "presign",
    "presign_read",
    "presign_stat",
    "presign_write",
    "shared",
    "stat_with_if_version_match",
    "stat_with_if_version_not_match",
    "stat_with_if_modified_since",
    "stat_with_if_unmodified_since",
    "stat_with_override_cache_control",
    "stat_with_override_content_disposition",
    "stat_with_override_content_type",
    "stat_with_version",
    "read_with_if_version_match",
    "read_with_if_version_not_match",
    "read_with_if_modified_since",
    "read_with_if_unmodified_since",
    "read_with_version",
    "read_with_suffix",
    "read_with_checksum",
    "write_can_copy_from",
    "write_with_content_encoding",
    "write_with_content_language",
    "write_with_if_match",
    "write_with_if_none_match",
    "write_with_if_version_match",
    "write_with_if_version_not_match",
    "write_with_if_not_exists",
    "write_with_user_metadata",
    "write_with_expires",
    "write_with_checksum",
    "delete_with_version",
    "delete_with_recursive",
    "delete_with_if_match",
    "delete_with_if_none_match",
    "delete_with_if_version_match",
    "delete_with_if_version_not_match",
    "delete_max_size",
    "copy_with_if_not_exists",
    "copy_with_if_match",
    "copy_with_if_none_match",
    "copy_with_if_version_match",
    "copy_with_if_version_not_match",
    "copy_with_source_version",
    "copy_with_user_metadata",
    "copy_with_content_type",
    "copy_with_content_language",
    "copy_can_multi",
    "copy_multi_max_size",
    "copy_multi_min_size",
    "compose",
    "compose_with_content_type",
    "compose_with_content_disposition",
    "compose_with_content_encoding",
    "compose_with_cache_control",
    "compose_with_user_metadata",
    "compose_with_if_match",
    "compose_with_if_none_match",
    "compose_with_if_version_match",
    "compose_with_if_version_not_match",
    "compose_with_if_not_exists",
    "compose_with_source_version",
    "compose_with_source_if_match",
    "restore",
    "restore_with_version",
    "restore_with_if_not_exists",
    "rename_with_if_not_exists",
    "list_with_versions",
    "list_with_deleted",
    "list_with_glob",
    "presign_delete",
];

pub fn generate(workspace_dir: &Path) -> Result<()> {
    generate_capability(workspace_dir)?;
    generate_metadata(workspace_dir)?;
    generate_options(workspace_dir)
}

/// Order core names for a public C++ class by its append-only `frozen` list,
/// named `list` in errors.
///
/// Fails if a core name is not listed, so a new member cannot take an
/// unreviewed position, and if a listed member no longer exists in core,
/// because dropping it would silently break source compatibility.
fn public_order<T>(
    frozen: &[&str],
    list: &str,
    fields: &[T],
    name_of: impl Fn(&T) -> &str,
) -> Result<Vec<usize>> {
    if let Some(field) = fields.iter().find(|f| !frozen.contains(&name_of(f))) {
        bail!(
            "new core member `{}` must be appended to {list} in dev/src/generate/cpp.rs",
            name_of(field)
        );
    }
    frozen
        .iter()
        .map(|member| {
            fields
                .iter()
                .position(|f| name_of(f) == *member)
                .with_context(|| format!("{list} member `{member}` is missing from core"))
        })
        .collect()
}

fn generate_capability(workspace_dir: &Path) -> Result<()> {
    for (relative, rendered) in render_capability(workspace_dir)? {
        fs::write(workspace_dir.join(relative), format!("{rendered}\n"))?;
    }
    Ok(())
}

/// Render every capability output as `(path relative to workspace, content)`.
fn render_capability(workspace_dir: &Path) -> Result<Vec<(&'static str, String)>> {
    let source = workspace_dir.join("core/core/src/types/capability.rs");
    // Core order. The cxx bridge and the Rust conversion use this order; the
    // converter copies by name, so it does not need to match the public class.
    let fields: Vec<CapabilityField> = capability::parse(&source)?
        .into_iter()
        .map(|f| CapabilityField {
            cpp_name: cpp_name(&f.name),
            name: f.name,
            is_bool: f.is_bool,
            hpp_doc_lines: wrap_doc(&f.doc, HPP_DOC_PREFIX, HPP_COLUMN_LIMIT),
            doc: f.doc,
        })
        .collect();
    let public_fields: Vec<&CapabilityField> =
        public_order(FROZEN_PUBLIC_ORDER, "FROZEN_PUBLIC_ORDER", &fields, |f| {
            f.name.as_str()
        })?
        .into_iter()
        .map(|i| &fields[i])
        .collect();

    let mut env = Environment::new();
    env.add_template("capability_rs", include_str!("cpp/capability_rs.j2"))?;
    env.add_template("capability_hpp", include_str!("cpp/capability_hpp.j2"))?;
    env.add_template("capability_cpp", include_str!("cpp/capability_cpp.j2"))?;

    let outputs = [
        ("capability_rs", "bindings/cpp/src/capability.rs"),
        ("capability_hpp", "bindings/cpp/include/capability.hpp"),
        (
            "capability_cpp",
            "bindings/cpp/src/utils/capability_converter.cpp",
        ),
    ];
    let mut rendered = Vec::with_capacity(outputs.len());
    for (template, relative) in outputs {
        let content = env.get_template(template)?.render(
            context! { fields => Serde(&fields), public_fields => Serde(&public_fields) },
        )?;
        rendered.push((relative, content));
    }
    Ok(rendered)
}

/// Map a core field name to a valid C++ member name.
///
/// `delete` is a C++ keyword; the binding has always exposed it as
/// `delete_feature`, so keep that spelling for compatibility.
fn cpp_name(name: &str) -> String {
    match name {
        "delete" => "delete_feature".to_string(),
        _ => name.to_string(),
    }
}

/// Core accessor names of the public `opendal::Metadata` members, in their
/// released declaration order.
///
/// Before C++20 the public class is an aggregate, so callers may
/// brace-initialize it by position. This order is append-only: generation
/// fails until every core accessor is listed, so append each new core accessor
/// to the end of this list.
const METADATA_FROZEN_PUBLIC_ORDER: &[&str] = &[
    "mode",
    "content_length",
    "cache_control",
    "content_disposition",
    "content_md5",
    "content_type",
    "content_encoding",
    "etag",
    "last_modified",
    "version",
    "is_current",
    "is_deleted",
    "content_language",
    "user_metadata",
];

/// Number of leading `METADATA_FROZEN_PUBLIC_ORDER` members that the
/// hand-written class declared. Every later member gets a default member
/// initializer, so a positional initializer of the hand-written members alone
/// stays complete under `-Wmissing-field-initializers`.
const METADATA_HAND_WRITTEN_MEMBERS: usize = 12;

/// Render model handed to the C++ metadata templates. Every per-kind decision
/// is made in `metadata_field`, so the templates stay structural.
#[derive(Serialize)]
struct MetadataField {
    /// Accessor name in core `opendal::Metadata`, also the cxx bridge field.
    name: String,
    /// Single-line doc text from core.
    doc: String,
    /// `doc` wrapped for the header's `///` comments.
    hpp_doc_lines: Vec<String>,
    /// Field type in the cxx bridge struct.
    bridge_type: &'static str,
    /// Expression converting the core accessor into the bridge field.
    rust_value: String,
    /// Declaration of the public C++ member.
    hpp_member: String,
    /// Doc comment and definition of the public C++ accessor, laid out as
    /// clang-format does.
    hpp_accessor: String,
    /// Statement copying the bridge field into the public C++ member.
    cpp_copy: String,
}

/// Map one core accessor to its bridge field, conversions, and public C++
/// member and accessor.
///
/// A new return type in core fails parsing in `metadata::parse`, and a new
/// `MetadataKind` fails this exhaustive match, so an unsupported accessor
/// cannot be rendered silently.
fn metadata_field(f: metadata::MetadataField) -> MetadataField {
    let name = f.name;
    let mut doc = f.doc;
    // The binding has always exposed `mode` as the `type` member.
    let member = if name == "mode" {
        "type".to_string()
    } else {
        name.clone()
    };
    let plain = format!("meta.{name}()");
    let into = format!("meta.{name}().into()");
    let copy = format!("metadata.{member} = meta.{name};");
    let parse = format!("metadata.{member} = parse_optional(meta.{name});");
    // (bridge type, public type, member initializer, returned by reference,
    // Rust conversion, C++ copy)
    let (bridge_type, cpp_type, cpp_init, by_ref, rust_value, cpp_copy) = match f.kind {
        MetadataKind::Mode => (
            "EntryMode",
            "EntryMode",
            "{EntryMode::UNKNOWN}",
            false,
            into,
            format!("metadata.{member} = static_cast<EntryMode>(meta.{name});"),
        ),
        MetadataKind::Bool => ("bool", "bool", "{false}", false, plain, copy),
        MetadataKind::U64 => ("u64", "std::uint64_t", "{0}", false, plain, copy),
        MetadataKind::OptionBool => ("OptionalBool", "std::optional<bool>", "", true, into, parse),
        MetadataKind::OptionStr => (
            "OptionalString",
            "std::optional<std::string>",
            "",
            true,
            into,
            parse,
        ),
        MetadataKind::OptionTimestamp => (
            "OptionalTimestamp",
            "std::optional<std::chrono::system_clock::time_point>",
            "",
            true,
            into,
            parse,
        ),
        MetadataKind::OptionUserMetadata => {
            // Core returns a borrowed view, while C++ receives an owned copy,
            // so the core doc would be misleading here.
            doc = format!("Return this entry's {}.", name.replace('_', " "));
            (
                "OptionalUserMetadata",
                "std::optional<std::unordered_map<std::string, std::string>>",
                "",
                true,
                into,
                parse,
            )
        }
    };
    let hand_written =
        METADATA_FROZEN_PUBLIC_ORDER[..METADATA_HAND_WRITTEN_MEMBERS].contains(&name.as_str());
    let cpp_init = if cpp_init.is_empty() && !hand_written {
        "{}"
    } else {
        cpp_init
    };
    let accessor_type = if by_ref {
        format!("const {cpp_type}&")
    } else {
        cpp_type.to_string()
    };
    let hpp_doc_lines = wrap_doc(&doc, HPP_DOC_PREFIX, HPP_COLUMN_LIMIT);
    let hpp_accessor = hpp_doc_lines
        .iter()
        .map(|line| format!("{HPP_DOC_PREFIX}{line}"))
        .chain(accessor_lines(&accessor_type, &to_pascal(&name), &member))
        .collect::<Vec<_>>()
        .join("\n");
    MetadataField {
        hpp_member: format!("{cpp_type} {member}{cpp_init};"),
        hpp_accessor,
        hpp_doc_lines,
        doc,
        bridge_type,
        rust_value,
        cpp_copy,
        name,
    }
}

/// Lay out an inline accessor the way clang-format does for the header's
/// Google style, so the generated header is format-stable.
fn accessor_lines(ty: &str, method: &str, member: &str) -> Vec<String> {
    let one_line = format!("  {ty} {method}() const {{ return {member}; }}");
    if one_line.len() <= HPP_COLUMN_LIMIT {
        return vec![one_line];
    }
    let opening = format!("  {ty} {method}() const {{");
    let signature = format!("  {ty} {method}()");
    let mut lines = if opening.len() <= HPP_COLUMN_LIMIT {
        vec![opening]
    } else if signature.len() <= HPP_COLUMN_LIMIT {
        vec![signature, "      const {".to_string()]
    } else {
        vec![format!("  {ty}"), format!("  {method}() const {{")]
    };
    lines.push(format!("    return {member};"));
    lines.push("  }".to_string());
    lines
}

fn to_pascal(snake: &str) -> String {
    snake
        .split('_')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => first.to_ascii_uppercase().to_string() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

fn generate_metadata(workspace_dir: &Path) -> Result<()> {
    for (relative, rendered) in render_metadata(workspace_dir)? {
        fs::write(workspace_dir.join(relative), format!("{rendered}\n"))?;
    }
    Ok(())
}

/// Render every metadata output as `(path relative to workspace, content)`.
fn render_metadata(workspace_dir: &Path) -> Result<Vec<(&'static str, String)>> {
    let source = workspace_dir.join("core/core/src/types/metadata.rs");
    // Core order. The cxx bridge and the Rust conversion use this order; the
    // converter copies by name, so it does not need to match the public class.
    let fields: Vec<MetadataField> = metadata::parse(&source)?
        .into_iter()
        .map(metadata_field)
        .collect();
    let public_fields: Vec<&MetadataField> = public_order(
        METADATA_FROZEN_PUBLIC_ORDER,
        "METADATA_FROZEN_PUBLIC_ORDER",
        &fields,
        |f| f.name.as_str(),
    )?
    .into_iter()
    .map(|i| &fields[i])
    .collect();

    let mut env = Environment::new();
    env.add_template("metadata_rs", include_str!("cpp/metadata_rs.j2"))?;
    env.add_template("metadata_hpp", include_str!("cpp/metadata_hpp.j2"))?;
    env.add_template("metadata_cpp", include_str!("cpp/metadata_cpp.j2"))?;

    let outputs = [
        ("metadata_rs", "bindings/cpp/src/metadata.rs"),
        ("metadata_hpp", "bindings/cpp/include/metadata.hpp"),
        (
            "metadata_cpp",
            "bindings/cpp/src/utils/metadata_converter.cpp",
        ),
    ];
    let mut rendered = Vec::with_capacity(outputs.len());
    for (template, relative) in outputs {
        let content = env.get_template(template)?.render(
            context! { fields => Serde(&fields), public_fields => Serde(&public_fields) },
        )?;
        rendered.push((relative, content));
    }
    Ok(rendered)
}

/// One core options struct that crosses into C++, and the fields it carries.
struct OptionsSpec {
    /// Core struct name, also the name of the public C++ struct.
    name: &'static str,
    /// `@brief` of the public C++ struct.
    brief: &'static str,
    /// Carried core fields and their kinds, in the declaration order of the
    /// public C++ struct.
    ///
    /// Callers may brace-initialize the public struct by position or with
    /// designated initializers, so this order is append-only. A core field
    /// that is not listed is not exposed in C++ and keeps its core default.
    fields: &'static [(&'static str, OptionKind)],
}

/// Options structs generated into `include/options.hpp`. The other options
/// structs are still hand-written in `include/data_structure.hpp`.
const OPTIONS: &[OptionsSpec] = {
    use OptionKind::*;

    &[OptionsSpec {
        name: "ListOptions",
        brief: "Options for list and lister operations.",
        fields: &[
            ("limit", OptionUsize),
            ("start_after", OptionString),
            ("recursive", Bool),
            ("versions", Bool),
            ("deleted", Bool),
        ],
    }]
};

/// Render model of one options struct. Every per-kind decision is made in
/// `option_member`, so the templates stay structural.
#[derive(Debug, Serialize)]
struct OptionsModel {
    /// Core struct name, also the public C++ struct name.
    name: &'static str,
    /// Name of the cxx bridge struct.
    bridge_name: String,
    /// `@brief` of the public C++ struct.
    brief: &'static str,
    /// Carried fields, in `OptionsSpec::fields` order.
    members: Vec<OptionMember>,
    /// Core fields that C++ does not expose, in core order.
    unexposed: Vec<String>,
    /// Whether the Rust conversion leaves core fields to `Default::default()`.
    fills_defaults: bool,
}

#[derive(Debug, PartialEq, Serialize)]
struct OptionMember {
    /// Field name in core, the bridge struct, and the public C++ struct.
    name: String,
    /// Core field doc with rustdoc links rendered as code spans, each
    /// paragraph wrapped for the header's `///` comments. An empty line
    /// separates paragraphs.
    hpp_doc_lines: Vec<String>,
    /// Declaration of the public C++ member.
    hpp_member: String,
    /// Field type in the cxx bridge struct.
    bridge_type: &'static str,
    /// Expression converting the public C++ member into the bridge field.
    cpp_value: String,
    /// Expression converting the bridge field into the core field.
    rust_value: String,
}

/// Map one core option field to its public C++ member, bridge field, and
/// conversions.
///
/// Fails for a kind the bridge cannot carry yet, so such a field cannot be
/// listed in `OPTIONS` by mistake.
fn option_member(
    options: &str,
    field: &core_options::OptionField,
    kind: OptionKind,
    capabilities: &[capability::CapabilityField],
) -> Result<OptionMember> {
    let name = field.name.as_str();
    let member = format!("options.{name}");
    let bridge_field = format!("opts.{name}");
    // (public type, member initializer, bridge type, C++ conversion, Rust
    // conversion)
    let (cpp_type, cpp_init, bridge_type, cpp_value, rust_value) = match kind {
        OptionKind::Bool => ("bool", "{false}", "bool", member, bridge_field),
        OptionKind::OptionUsize => (
            "std::optional<std::size_t>",
            "",
            "OptionalUsize",
            format!("ToFfiOptionalUsize({member})"),
            format!("optional_usize({bridge_field})"),
        ),
        OptionKind::OptionString => (
            "std::optional<std::string>",
            "",
            "OptionalString",
            format!("ToFfiOptionalString({member})"),
            format!("optional_string({bridge_field})"),
        ),
        _ => bail!("the C++ bridge cannot carry `{options}::{name}` of kind {kind:?} yet"),
    };
    Ok(OptionMember {
        name: name.to_string(),
        hpp_doc_lines: option_doc_lines(&field.doc, capabilities),
        hpp_member: format!("{cpp_type} {name}{cpp_init};"),
        bridge_type,
        cpp_value,
        rust_value,
    })
}

/// The core doc comment of an option field as it reads in the C++ header.
///
/// Each paragraph is wrapped to fit the header's `///` comments, and an empty
/// line separates paragraphs. Rustdoc links become plain code spans, because
/// Doxygen cannot resolve Rust paths:
/// ``[`Capability::list_with_limit`](crate::Capability::list_with_limit)``
/// and ``[`crate::layers::SimulateLayer`]`` keep only their code span. Code
/// spans that name a C++ member then use its C++ name (see `cpp_code_span`);
/// other code spans stay as written.
fn option_doc_lines(doc: &[String], capabilities: &[capability::CapabilityField]) -> Vec<String> {
    let mut lines = Vec::new();
    for paragraph in doc.split(|line| line.trim().is_empty()) {
        if paragraph.is_empty() {
            continue;
        }
        if !lines.is_empty() {
            lines.push(String::new());
        }
        let text = paragraph
            .iter()
            .map(|line| rewrite_code_spans(line.trim(), |code| cpp_code_span(code, capabilities)))
            .collect::<Vec<_>>()
            .join(" ");
        lines.extend(wrap_doc(&text, HPP_DOC_PREFIX, HPP_COLUMN_LIMIT));
    }
    lines
}

/// The C++ name of a code span that names a member of the C++ surface, or
/// `None` to keep the code span as written.
///
/// A core capability such as `Capability::list_with_start_after` reads
/// `opendal::Capability::list_with_start_after`, the member of the
/// `opendal::Capability` that `Operator::Info()` returns, spelled by
/// `cpp_name`. Option members keep their core names in C++, so a span such as
/// `start_after` stays as written.
fn cpp_code_span(code: &str, capabilities: &[capability::CapabilityField]) -> Option<String> {
    let name = code
        .strip_prefix("crate::")
        .unwrap_or(code)
        .strip_prefix("Capability::")?;
    capabilities
        .iter()
        .any(|f| f.name == name)
        .then(|| format!("opendal::Capability::{}", cpp_name(name)))
}

/// Check `specs` against the parsed core structs and build their render
/// models.
///
/// Fails if a listed struct or field is missing from core, because dropping it
/// would silently break source compatibility, if a field is listed twice, or
/// if a field's kind in core differs from its listed kind, because that would
/// change the public C++ member type.
fn options_models(
    parsed: &[core_options::OptionsStruct],
    specs: &[OptionsSpec],
    capabilities: &[capability::CapabilityField],
) -> Result<Vec<OptionsModel>> {
    let mut models = Vec::with_capacity(specs.len());
    for spec in specs {
        let core = parsed
            .iter()
            .find(|s| s.name == spec.name)
            .with_context(|| format!("OPTIONS struct `{}` is missing from core", spec.name))?;
        let mut members = Vec::with_capacity(spec.fields.len());
        for (i, &(name, kind)) in spec.fields.iter().enumerate() {
            if spec.fields[..i].iter().any(|(listed, _)| *listed == name) {
                bail!("OPTIONS lists `{}::{name}` twice", spec.name);
            }
            let field = core
                .fields
                .iter()
                .find(|f| f.name == name)
                .with_context(|| {
                    format!(
                        "OPTIONS member `{}::{name}` is missing from core",
                        spec.name
                    )
                })?;
            if field.kind != kind {
                bail!(
                    "core `{}::{name}` has kind {:?}, but OPTIONS lists {kind:?}",
                    spec.name,
                    field.kind
                );
            }
            members.push(option_member(spec.name, field, kind, capabilities)?);
        }
        let unexposed: Vec<String> = core
            .fields
            .iter()
            .filter(|f| !spec.fields.iter().any(|(listed, _)| *listed == f.name))
            .map(|f| f.name.clone())
            .collect();
        models.push(OptionsModel {
            name: spec.name,
            bridge_name: format!("Ffi{}", spec.name),
            brief: spec.brief,
            fills_defaults: !unexposed.is_empty() || core.has_unmirrored_fields,
            members,
            unexposed,
        });
    }
    Ok(models)
}

fn generate_options(workspace_dir: &Path) -> Result<()> {
    for (relative, rendered) in render_options(workspace_dir)? {
        fs::write(workspace_dir.join(relative), format!("{rendered}\n"))?;
    }
    Ok(())
}

/// Render every options output as `(path relative to workspace, content)`.
fn render_options(workspace_dir: &Path) -> Result<Vec<(&'static str, String)>> {
    let parsed = core_options::parse(&workspace_dir.join("core/core/src/types/options.rs"))?;
    let capabilities = capability::parse(&workspace_dir.join("core/core/src/types/capability.rs"))?;
    let options = options_models(&parsed, OPTIONS, &capabilities)?;

    let mut env = Environment::new();
    env.add_template("options_hpp", include_str!("cpp/options_hpp.j2"))?;
    env.add_template("options_rs", include_str!("cpp/options_rs.j2"))?;
    env.add_template(
        "options_converter_hpp",
        include_str!("cpp/options_converter_hpp.j2"),
    )?;
    env.add_template(
        "options_converter_cpp",
        include_str!("cpp/options_converter_cpp.j2"),
    )?;

    let outputs = [
        ("options_hpp", "bindings/cpp/include/options.hpp"),
        ("options_rs", "bindings/cpp/src/options.rs"),
        (
            "options_converter_hpp",
            "bindings/cpp/src/utils/options_converter.hpp",
        ),
        (
            "options_converter_cpp",
            "bindings/cpp/src/utils/options_converter.cpp",
        ),
    ];
    let mut rendered = Vec::with_capacity(outputs.len());
    for (template, relative) in outputs {
        let content = env
            .get_template(template)?
            .render(context! { options => Serde(&options) })?;
        rendered.push((relative, content));
    }
    Ok(rendered)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn core_fields() -> Vec<capability::CapabilityField> {
        capability::parse(&workspace_dir().join("core/core/src/types/capability.rs"))
            .expect("parse core capability")
    }

    fn workspace_dir() -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
    }

    fn rendered(path: &str) -> String {
        render_capability(&workspace_dir())
            .unwrap()
            .into_iter()
            .find(|(relative, _)| *relative == path)
            .map(|(_, content)| content)
            .expect("rendered output")
    }

    #[test]
    fn test_rendered_header_declares_frozen_order() {
        let header = rendered("bindings/cpp/include/capability.hpp");
        let mut last = 0;
        for name in FROZEN_PUBLIC_ORDER {
            let decl = format!(" {}{{", cpp_name(name));
            let pos = header
                .find(&decl)
                .unwrap_or_else(|| panic!("header lacks `{decl}`"));
            assert!(pos > last, "`{decl}` is out of order in the header");
            last = pos;
        }
    }

    #[test]
    fn test_rendered_bridge_maps_every_core_field() {
        let bridge = rendered("bindings/cpp/src/capability.rs");
        let converter = rendered("bindings/cpp/src/utils/capability_converter.cpp");
        for f in core_fields() {
            let cpp = cpp_name(&f.name);
            assert!(
                bridge.contains(&format!("        {cpp}: ")),
                "bridge lacks `{cpp}`"
            );
            assert!(
                bridge.contains(&format!("{cpp}: cap.{},", f.name))
                    || bridge.contains(&format!("{cpp}: cap.{}.unwrap_or(0),", f.name)),
                "From<opendal::Capability> does not map `{}`",
                f.name
            );
            assert!(
                converter.contains(&format!("capability.{cpp} = cap.{cpp};")),
                "converter does not copy `{cpp}`"
            );
        }
    }

    #[test]
    fn test_public_order_rejects_unlisted_core_field() {
        let mut fields = FROZEN_PUBLIC_ORDER.to_vec();
        fields.insert(fields.len() / 2, "new_mid");
        let err = public_order(FROZEN_PUBLIC_ORDER, "FROZEN_PUBLIC_ORDER", &fields, |f| f)
            .unwrap_err()
            .to_string();
        assert!(err.contains("`new_mid` must be appended to FROZEN_PUBLIC_ORDER"));
    }

    #[test]
    fn test_public_order_covers_every_core_field_once() {
        let fields = core_fields();
        let mut order = public_order(FROZEN_PUBLIC_ORDER, "FROZEN_PUBLIC_ORDER", &fields, |f| {
            f.name.as_str()
        })
        .unwrap();
        order.sort_unstable();
        assert_eq!(order, (0..fields.len()).collect::<Vec<_>>());
    }

    #[test]
    fn test_public_order_rejects_missing_frozen_member() {
        let fields = ["stat", "read"];
        assert!(public_order(FROZEN_PUBLIC_ORDER, "FROZEN_PUBLIC_ORDER", &fields, |f| f).is_err());
    }

    #[test]
    fn test_wrap_doc_fits_column_limit() {
        let lines = wrap_doc("aaa bbb ccc ddd", "// ", 10);
        assert_eq!(lines, ["aaa bbb", "ccc ddd"]);
        assert_eq!(wrap_doc("short", "// ", 10), ["short"]);
    }

    #[test]
    fn test_cpp_name_escapes_keywords() {
        assert_eq!(cpp_name("delete"), "delete_feature");
        assert_eq!(cpp_name("delete_with_version"), "delete_with_version");
    }

    #[test]
    fn test_metadata_public_order_rejects_unlisted_core_accessor() {
        let mut fields = METADATA_FROZEN_PUBLIC_ORDER.to_vec();
        fields.insert(fields.len() / 2, "new_mid");
        let err = public_order(
            METADATA_FROZEN_PUBLIC_ORDER,
            "METADATA_FROZEN_PUBLIC_ORDER",
            &fields,
            |f| f,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("`new_mid` must be appended to METADATA_FROZEN_PUBLIC_ORDER"));
    }

    #[test]
    fn test_metadata_public_order_rejects_missing_frozen_member() {
        let fields = ["mode", "content_length"];
        let err = public_order(
            METADATA_FROZEN_PUBLIC_ORDER,
            "METADATA_FROZEN_PUBLIC_ORDER",
            &fields,
            |f| f,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("`cache_control` is missing from core"));
    }

    /// `ListOptions` as `bindings/cpp/include/data_structure.hpp` declared it on
    /// `main` before it was generated, copied verbatim. Callers depend on each
    /// member's name, type, default member initializer, and position. The
    /// generated header adds member doc comments, which this text omits.
    const MAIN_LIST_OPTIONS: &str = r#"/**
 * @struct ListOptions
 * @brief Options for list and lister operations.
 */
struct ListOptions {
  std::optional<std::size_t> limit;
  std::optional<std::string> start_after;
  bool recursive{false};
  bool versions{false};
  bool deleted{false};
};"#;

    fn parsed_options() -> Vec<core_options::OptionsStruct> {
        core_options::parse(&workspace_dir().join("core/core/src/types/options.rs"))
            .expect("parse core options")
    }

    fn rendered_options(path: &str) -> String {
        render_options(&workspace_dir())
            .unwrap()
            .into_iter()
            .find(|(relative, _)| *relative == path)
            .map(|(_, content)| content)
            .expect("rendered output")
    }

    #[test]
    fn test_rendered_options_header_keeps_main_list_options() {
        let header = rendered_options("bindings/cpp/include/options.hpp");
        let declarations: String = header
            .lines()
            .filter(|line| !line.starts_with("  ///"))
            .map(|line| format!("{line}\n"))
            .collect();
        assert!(
            declarations.contains(MAIN_LIST_OPTIONS),
            "header no longer declares:\n{MAIN_LIST_OPTIONS}\n\nheader:\n{header}"
        );
        assert_eq!(header.matches("\nstruct ").count(), 1);
    }

    /// Split doc text into paragraphs, each joined into one line of words.
    fn paragraphs<'a>(lines: impl IntoIterator<Item = &'a str>) -> Vec<String> {
        let mut paragraphs = vec![Vec::new()];
        for line in lines {
            match line.trim() {
                "" => paragraphs.push(Vec::new()),
                text => paragraphs.last_mut().unwrap().push(text),
            }
        }
        paragraphs
            .into_iter()
            .filter(|words| !words.is_empty())
            .map(|words| words.join(" "))
            .collect()
    }

    #[test]
    fn test_rendered_options_header_documents_members_with_core_docs() {
        let header = rendered_options("bindings/cpp/include/options.hpp");
        let lines: Vec<&str> = header.lines().collect();
        let list = parsed_options()
            .into_iter()
            .find(|s| s.name == "ListOptions")
            .unwrap();
        for (name, _) in OPTIONS[0].fields {
            // Core's full doc, with `[`code`](target)` and `[`code`]` links
            // reduced to their code span.
            let core_doc = list
                .fields
                .iter()
                .find(|f| f.name == *name)
                .unwrap()
                .doc
                .join("\n");
            let mut plain = String::new();
            let mut rest = core_doc.as_str();
            while let Some(at) = rest.find("](") {
                let close = rest[at..].find(')').unwrap();
                plain.push_str(&rest[..at + 1]);
                rest = &rest[at + close + 1..];
            }
            plain.push_str(rest);
            // C++ callers read capabilities from the `opendal::Capability`
            // that `Operator::Info()` returns.
            let plain = plain
                .replace("[`", "`")
                .replace("`]", "`")
                .replace("`Capability::", "`opendal::Capability::");
            let expected = paragraphs(plain.lines());
            assert!(
                !expected.is_empty(),
                "core `ListOptions::{name}` has no doc"
            );

            let decl = lines
                .iter()
                .position(|line| {
                    !line.starts_with("  ///")
                        && (line.ends_with(&format!(" {name};"))
                            || line.ends_with(&format!(" {name}{{false}};")))
                })
                .unwrap_or_else(|| panic!("header lacks `{name}`"));
            let mut comment: Vec<&str> = lines[..decl]
                .iter()
                .rev()
                .map_while(|line| line.strip_prefix("  ///"))
                .collect();
            comment.reverse();
            assert_eq!(
                paragraphs(comment),
                expected,
                "doc of `ListOptions::{name}`"
            );
        }
        // A rendered intra-doc link, copied from core's `start_after` doc.
        assert!(header.contains(
            "  ///\n  /// Setting this requires `opendal::Capability::list_with_start_after`.\n"
        ));
        assert!(!header.contains("[`"));
        assert!(!header.contains("](crate::"));
        assert!(!header.contains(" `Capability::"));

        // Each referenced capability is a member of the generated C++ class.
        let capability_header = rendered("bindings/cpp/include/capability.hpp");
        let mut referenced = 0;
        for reference in header.split("`opendal::Capability::").skip(1) {
            let member = &reference[..reference.find('`').unwrap()];
            assert!(
                capability_header.contains(&format!("  bool {member}{{false}};\n")),
                "capability.hpp lacks `{member}`"
            );
            referenced += 1;
        }
        assert_eq!(referenced, 4);
    }

    #[test]
    fn test_option_doc_lines_render_rustdoc_links_as_code() {
        let doc = [
            "Requires [`Capability::list_with_limit`](crate::Capability::list_with_limit).",
            "",
            "Rely on [`crate::layers::SimulateLayer`] and [`Self::recursive`].",
            "A [plain](https://opendal.apache.org) link and `code` stay.",
        ]
        .map(String::from);
        assert_eq!(
            option_doc_lines(&doc, &core_fields()),
            [
                "Requires `opendal::Capability::list_with_limit`.",
                "",
                "Rely on `crate::layers::SimulateLayer` and `Self::recursive`. A",
                "[plain](https://opendal.apache.org) link and `code` stay.",
            ]
        );
    }

    #[test]
    fn test_option_doc_lines_use_cpp_names() {
        let doc = [
            "- Pagination: `limit`, `start_after`.",
            "",
            "Requires [`Capability::list_with_start_after`](crate::Capability::list_with_start_after).",
            "",
            "Requires [`crate::Capability::list_with_versions`] and `Capability::delete`.",
            "",
            "Unknown `Capability::no_such_flag`, [`crate::ErrorKind::Unsupported`], `glob`, and prose Capability::list stay.",
        ]
        .map(String::from);
        assert_eq!(
            option_doc_lines(&doc, &core_fields()),
            [
                "- Pagination: `limit`, `start_after`.",
                "",
                "Requires `opendal::Capability::list_with_start_after`.",
                "",
                "Requires `opendal::Capability::list_with_versions` and",
                "`opendal::Capability::delete_feature`.",
                "",
                "Unknown `Capability::no_such_flag`, `crate::ErrorKind::Unsupported`,",
                "`glob`, and prose Capability::list stay.",
            ]
        );
    }

    #[test]
    fn test_new_core_field_stays_unexposed() {
        let mut parsed = parsed_options();
        let before = options_models(&parsed, OPTIONS, &core_fields()).unwrap();
        let list = parsed.iter_mut().find(|s| s.name == "ListOptions").unwrap();
        list.fields.insert(
            1,
            core_options::OptionField {
                name: "new_mid".to_string(),
                kind: OptionKind::OptionString,
                doc: Vec::new(),
            },
        );

        let after = options_models(&parsed, OPTIONS, &core_fields()).unwrap();

        assert_eq!(before[0].members, after[0].members);
        assert_eq!(after[0].unexposed[0], "new_mid");
        assert_eq!(after[0].unexposed[1..], before[0].unexposed[..]);
        assert!(after[0].fills_defaults);
    }

    #[test]
    fn test_options_models_reject_incompatible_specs() {
        use OptionKind::*;

        let parsed = parsed_options();
        let error = |name: &'static str, fields: &'static [(&'static str, OptionKind)]| {
            let spec = OptionsSpec {
                name,
                brief: "",
                fields,
            };
            options_models(&parsed, &[spec], &core_fields())
                .unwrap_err()
                .to_string()
        };

        assert_eq!(
            error("MissingOptions", &[]),
            "OPTIONS struct `MissingOptions` is missing from core"
        );
        assert_eq!(
            error("ListOptions", &[("missing", OptionString)]),
            "OPTIONS member `ListOptions::missing` is missing from core"
        );
        assert_eq!(
            error(
                "ListOptions",
                &[("limit", OptionUsize), ("limit", OptionUsize)]
            ),
            "OPTIONS lists `ListOptions::limit` twice"
        );
        assert_eq!(
            error("ListOptions", &[("limit", OptionU64)]),
            "core `ListOptions::limit` has kind OptionUsize, but OPTIONS lists OptionU64"
        );
        // Core changing a listed field to another kind the bridge carries must
        // fail too, instead of silently turning `start_after` into a `bool`.
        let mut changed = parsed.clone();
        let list = changed
            .iter_mut()
            .find(|s| s.name == "ListOptions")
            .unwrap();
        list.fields
            .iter_mut()
            .find(|f| f.name == "start_after")
            .unwrap()
            .kind = Bool;
        assert_eq!(
            options_models(&changed, OPTIONS, &core_fields())
                .unwrap_err()
                .to_string(),
            "core `ListOptions::start_after` has kind Bool, but OPTIONS lists OptionString"
        );
        assert_eq!(
            error("ReadOptions", &[("if_modified_since", OptionTimestamp)]),
            "the C++ bridge cannot carry `ReadOptions::if_modified_since` of kind OptionTimestamp yet"
        );
    }

    #[test]
    fn test_accessor_lines_match_clang_format() {
        // Layouts that clang-format 18 and 23 produce with bindings/cpp's
        // Google style.
        assert_eq!(
            accessor_lines("EntryMode", "Mode", "type"),
            ["  EntryMode Mode() const { return type; }"]
        );
        assert_eq!(
            accessor_lines(
                "const std::optional<std::string>&",
                "ContentDisposition",
                "content_disposition"
            ),
            [
                "  const std::optional<std::string>& ContentDisposition() const {",
                "    return content_disposition;",
                "  }",
            ]
        );
        assert_eq!(
            accessor_lines(
                "const std::optional<std::chrono::system_clock::time_point>&",
                "LastModified",
                "last_modified"
            ),
            [
                "  const std::optional<std::chrono::system_clock::time_point>& LastModified()",
                "      const {",
                "    return last_modified;",
                "  }",
            ]
        );
        assert_eq!(
            accessor_lines(
                "const std::optional<std::unordered_map<std::string, std::string>>&",
                "UserMetadata",
                "user_metadata"
            ),
            [
                "  const std::optional<std::unordered_map<std::string, std::string>>&",
                "  UserMetadata() const {",
                "    return user_metadata;",
                "  }",
            ]
        );
    }
}
