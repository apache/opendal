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

use super::capability;
use super::metadata::{self, MetadataKind};
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
    generate_metadata(workspace_dir)
}

/// Order core fields for the public C++ class by `FROZEN_PUBLIC_ORDER`.
///
/// Fails if a core field is not listed, so a new field cannot take an
/// unreviewed position, and if a listed member no longer exists in core,
/// because dropping it would silently break source compatibility.
fn public_order<T>(fields: &[T], name_of: impl Fn(&T) -> &str) -> Result<Vec<usize>> {
    if let Some(field) = fields
        .iter()
        .find(|f| !FROZEN_PUBLIC_ORDER.contains(&name_of(f)))
    {
        bail!(
            "new core capability `{}` must be appended to FROZEN_PUBLIC_ORDER in dev/src/generate/cpp.rs",
            name_of(field)
        );
    }
    FROZEN_PUBLIC_ORDER
        .iter()
        .map(|frozen| {
            fields
                .iter()
                .position(|f| name_of(f) == *frozen)
                .with_context(|| {
                    format!("frozen C++ capability member `{frozen}` is missing from core")
                })
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
    let public_fields: Vec<&CapabilityField> = public_order(&fields, |f| f.name.as_str())?
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

/// Order core accessors for the public C++ class by
/// `METADATA_FROZEN_PUBLIC_ORDER`.
///
/// Fails if a core accessor is not listed, so a new accessor cannot take an
/// unreviewed position, and if a listed member no longer exists in core,
/// because dropping it would silently break source compatibility.
fn metadata_public_order<T>(fields: &[T], name_of: impl Fn(&T) -> &str) -> Result<Vec<usize>> {
    if let Some(field) = fields
        .iter()
        .find(|f| !METADATA_FROZEN_PUBLIC_ORDER.contains(&name_of(f)))
    {
        bail!(
            "new core metadata accessor `{}` must be appended to METADATA_FROZEN_PUBLIC_ORDER in dev/src/generate/cpp.rs",
            name_of(field)
        );
    }
    METADATA_FROZEN_PUBLIC_ORDER
        .iter()
        .map(|frozen| {
            fields
                .iter()
                .position(|f| name_of(f) == *frozen)
                .with_context(|| {
                    format!("frozen C++ metadata member `{frozen}` is missing from core")
                })
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
    let public_fields: Vec<&MetadataField> = metadata_public_order(&fields, |f| f.name.as_str())?
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
        let content = env
            .get_template(template)?
            .render(context! { fields => fields, public_fields => public_fields })?;
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
        let err = public_order(&fields, |f| f).unwrap_err().to_string();
        assert!(err.contains("`new_mid` must be appended to FROZEN_PUBLIC_ORDER"));
    }

    #[test]
    fn test_public_order_covers_every_core_field_once() {
        let fields = core_fields();
        let mut order = public_order(&fields, |f| f.name.as_str()).unwrap();
        order.sort_unstable();
        assert_eq!(order, (0..fields.len()).collect::<Vec<_>>());
    }

    #[test]
    fn test_public_order_rejects_missing_frozen_member() {
        let fields = ["stat", "read"];
        assert!(public_order(&fields, |f| f).is_err());
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
        let err = metadata_public_order(&fields, |f| f)
            .unwrap_err()
            .to_string();
        assert!(err.contains("`new_mid` must be appended to METADATA_FROZEN_PUBLIC_ORDER"));
    }

    #[test]
    fn test_metadata_public_order_rejects_missing_frozen_member() {
        let fields = ["mode", "content_length"];
        let err = metadata_public_order(&fields, |f| f)
            .unwrap_err()
            .to_string();
        assert!(err.contains("`cache_control` is missing from core"));
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
