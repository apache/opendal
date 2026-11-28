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

//! Generator for the C binding and the Go binding, which mirrors the C ABI.
//!
//! The Go binding loads `libopendal_c` through purego and reads
//! `opendal_capability` by memory layout, so both sides are rendered in the
//! same frozen field order to keep the layouts in lockstep.

mod options;

use super::capability;
use super::metadata::{self, MetadataKind};
use anyhow::{Context, Result, bail};
use minijinja::value::Serde;
use minijinja::{Environment, context};
use serde::Serialize;
use std::fs;
use std::path::Path;

/// Render model handed to the C and Go capability templates.
#[derive(Serialize)]
struct CapabilityField {
    /// snake_case field name in core and in the C struct.
    name: String,
    /// lowerCamelCase field name in the Go `opendalCapability` mirror.
    go_field: String,
    /// `go_field` padded so that gofmt column alignment is already satisfied.
    go_field_padded: String,
    /// PascalCase accessor name on the Go `Capability`.
    go_method: String,
    /// Plain `bool` flag when true, `Option<usize>` limit when false.
    is_bool: bool,
    /// Single-line doc text from core.
    doc: String,
}

/// Render model handed to the C and Go metadata templates.
#[derive(Serialize)]
struct MetadataAccessor {
    /// snake_case accessor name in core.
    name: String,
    /// Exported C function that returns this accessor's value.
    c_function: String,
    /// PascalCase method name on the Go `Metadata`.
    go_method: String,
    /// lowerCamelCase field name in the Go `Metadata`.
    go_field: String,
    /// `go_field` padded so that gofmt aligns the struct field types.
    go_field_padded: String,
    /// `go_field:` padded so that gofmt aligns the composite literal values.
    go_key_padded: String,
    /// The shape of the core return type, which picks the C and Go mapping.
    kind: MetadataKind,
    /// First paragraph of the core accessor doc.
    doc: String,
    /// The core doc rephrased to start with the Go method name.
    go_doc: String,
}

pub fn generate(workspace_dir: &Path) -> Result<()> {
    generate_capability(workspace_dir)?;
    generate_metadata(workspace_dir)?;
    options::generate(workspace_dir)
}

/// Core names of the `opendal_capability` fields, in their declaration order.
///
/// C callers may initialize the struct by position, and the Go mirror reads it
/// by memory layout. The first 57 fields keep the order released in C binding
/// 0.47.8 (OpenDAL v0.59.5). This order is append-only: generation fails until
/// every core field is listed, so append each new core field to the end of
/// this list.
const FROZEN_CAPABILITY_ORDER: &[&str] = &[
    "stat",
    "stat_with_if_match",
    "stat_with_if_none_match",
    "stat_with_if_modified_since",
    "stat_with_if_unmodified_since",
    "stat_with_override_cache_control",
    "stat_with_override_content_disposition",
    "stat_with_override_content_type",
    "stat_with_version",
    "read",
    "read_with_if_match",
    "read_with_if_none_match",
    "read_with_override_cache_control",
    "read_with_override_content_disposition",
    "read_with_override_content_type",
    "read_with_if_modified_since",
    "read_with_if_unmodified_since",
    "read_with_version",
    "write",
    "write_can_multi",
    "write_can_empty",
    "write_can_append",
    "write_with_content_type",
    "write_with_content_disposition",
    "write_with_content_encoding",
    "write_with_cache_control",
    "write_with_if_match",
    "write_with_if_none_match",
    "write_with_if_not_exists",
    "write_with_user_metadata",
    "write_multi_max_size",
    "write_multi_min_size",
    "write_total_max_size",
    "create_dir",
    "delete",
    "delete_with_version",
    "delete_with_recursive",
    "copy",
    "copy_with_if_not_exists",
    "copy_with_if_match",
    "copy_with_source_version",
    "copy_can_multi",
    "copy_multi_max_size",
    "copy_multi_min_size",
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
    "presign_delete",
    "shared",
    "stat_with_if_version_match",
    "stat_with_if_version_not_match",
    "read_with_if_version_match",
    "read_with_if_version_not_match",
    "read_with_suffix",
    "read_with_checksum",
    "write_can_copy_from",
    "write_with_content_language",
    "write_with_if_version_match",
    "write_with_if_version_not_match",
    "write_with_expires",
    "write_with_checksum",
    "delete_with_if_match",
    "delete_with_if_none_match",
    "delete_with_if_version_match",
    "delete_with_if_version_not_match",
    "delete_max_size",
    "copy_with_if_none_match",
    "copy_with_if_version_match",
    "copy_with_if_version_not_match",
    "copy_with_user_metadata",
    "copy_with_content_type",
    "copy_with_content_language",
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
    "list_with_glob",
];

/// Order the core capability fields by `FROZEN_CAPABILITY_ORDER`.
///
/// Fails if a core field is not listed, so a new field cannot take an
/// unreviewed position, and if a listed field no longer exists in core,
/// because dropping it would shift every later field.
fn frozen_order(
    mut fields: Vec<capability::CapabilityField>,
) -> Result<Vec<capability::CapabilityField>> {
    if let Some(field) = fields
        .iter()
        .find(|f| !FROZEN_CAPABILITY_ORDER.contains(&f.name.as_str()))
    {
        bail!(
            "new core capability `{}` must be appended to FROZEN_CAPABILITY_ORDER in dev/src/generate/c.rs",
            field.name
        );
    }
    FROZEN_CAPABILITY_ORDER
        .iter()
        .map(|name| {
            let index = fields
                .iter()
                .position(|f| f.name == *name)
                .with_context(|| {
                    format!("FROZEN_CAPABILITY_ORDER field `{name}` is missing from core")
                })?;
            Ok(fields.swap_remove(index))
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
    let parsed = frozen_order(capability::parse(&source)?)?;

    let width = parsed
        .iter()
        .map(|f| to_pascal(&f.name).len())
        .max()
        .unwrap_or(0);
    let fields: Vec<CapabilityField> = parsed
        .into_iter()
        .map(|f| {
            let go_method = to_pascal(&f.name);
            let go_field = lower_first(&go_method);
            CapabilityField {
                go_field_padded: format!("{go_field:<width$} "),
                go_field,
                go_method,
                name: f.name,
                is_bool: f.is_bool,
                doc: f.doc,
            }
        })
        .collect();

    let mut env = Environment::new();
    env.add_template("capability_rs", include_str!("c/capability_rs.j2"))?;
    env.add_template("capability_go", include_str!("c/capability_go.j2"))?;

    let outputs = [
        ("capability_rs", "bindings/c/src/capability.rs"),
        ("capability_go", "bindings/go/capability.go"),
    ];
    let mut rendered = Vec::with_capacity(outputs.len());
    for (template, relative) in outputs {
        let content = env
            .get_template(template)?
            .render(context! { fields => Serde(&fields) })?;
        rendered.push((relative, content));
    }
    Ok(rendered)
}

fn generate_metadata(workspace_dir: &Path) -> Result<()> {
    let source = workspace_dir.join("core/core/src/types/metadata.rs");
    let parsed = metadata::parse(&source)?;

    let width = parsed
        .iter()
        .map(|f| go_field_name(&f.name).len())
        .max()
        .unwrap_or(0);
    let accessors: Vec<MetadataAccessor> = parsed
        .into_iter()
        .map(|f| {
            let go_field = go_field_name(&f.name);
            // Keep the released C function names.
            let c_function = match f.kind {
                MetadataKind::OptionTimestamp => format!("opendal_metadata_{}_ms", f.name),
                MetadataKind::OptionUserMetadata => format!("opendal_metadata_get_{}", f.name),
                _ => format!("opendal_metadata_{}", f.name),
            };
            let go_method = go_method_name(&f.name);
            let go_doc = match f.doc.strip_prefix("Return ") {
                Some(rest) => format!("{go_method} returns {rest}"),
                None => format!("{go_method} reports: {}", f.doc),
            };
            MetadataAccessor {
                c_function,
                go_method,
                go_doc,
                go_field_padded: format!("{go_field:<width$} "),
                go_key_padded: format!("{:<w$} ", format!("{go_field}:"), w = width + 1),
                go_field,
                name: f.name,
                kind: f.kind,
                doc: f.doc,
            }
        })
        .collect();

    let mut env = Environment::new();
    env.add_template("metadata_rs", include_str!("c/metadata_rs.j2"))?;
    env.add_template("metadata_go", include_str!("c/metadata_go.j2"))?;

    let outputs = [
        ("metadata_rs", "bindings/c/src/metadata.rs"),
        ("metadata_go", "bindings/go/metadata.go"),
    ];
    for (template, relative) in outputs {
        let rendered = env
            .get_template(template)?
            .render(context! { accessors => Serde(&accessors) })?;
        fs::write(workspace_dir.join(relative), format!("{rendered}\n"))?;
    }
    Ok(())
}

/// Go spells these words as initialisms, for example `ContentMD5` and `ETag`.
const GO_INITIALISMS: &[(&str, &str)] = &[("md5", "MD5"), ("etag", "ETag")];

fn go_word(word: &str) -> String {
    match GO_INITIALISMS.iter().find(|(w, _)| *w == word) {
        Some((_, initialism)) => initialism.to_string(),
        None => to_pascal(word),
    }
}

fn go_method_name(snake: &str) -> String {
    snake.split('_').map(go_word).collect()
}

fn go_field_name(snake: &str) -> String {
    let mut words = snake.split('_');
    let first = words.next().unwrap_or_default().to_string();
    words.fold(first, |name, word| name + &go_word(word))
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

fn lower_first(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        Some(first) => first.to_ascii_lowercase().to_string() + chars.as_str(),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `opendal_capability` fields released in C binding 0.47.8 (OpenDAL
    /// v0.59.5), copied from `bindings/c/src/operator_info.rs` at `v0.59.5`.
    const RELEASED_C_FIELDS: [&str; 57] = [
        "stat: bool",
        "stat_with_if_match: bool",
        "stat_with_if_none_match: bool",
        "stat_with_if_modified_since: bool",
        "stat_with_if_unmodified_since: bool",
        "stat_with_override_cache_control: bool",
        "stat_with_override_content_disposition: bool",
        "stat_with_override_content_type: bool",
        "stat_with_version: bool",
        "read: bool",
        "read_with_if_match: bool",
        "read_with_if_none_match: bool",
        "read_with_override_cache_control: bool",
        "read_with_override_content_disposition: bool",
        "read_with_override_content_type: bool",
        "read_with_if_modified_since: bool",
        "read_with_if_unmodified_since: bool",
        "read_with_version: bool",
        "write: bool",
        "write_can_multi: bool",
        "write_can_empty: bool",
        "write_can_append: bool",
        "write_with_content_type: bool",
        "write_with_content_disposition: bool",
        "write_with_content_encoding: bool",
        "write_with_cache_control: bool",
        "write_with_if_match: bool",
        "write_with_if_none_match: bool",
        "write_with_if_not_exists: bool",
        "write_with_user_metadata: bool",
        "write_multi_max_size: usize",
        "write_multi_min_size: usize",
        "write_total_max_size: usize",
        "create_dir: bool",
        "delete: bool",
        "delete_with_version: bool",
        "delete_with_recursive: bool",
        "copy: bool",
        "copy_with_if_not_exists: bool",
        "copy_with_if_match: bool",
        "copy_with_source_version: bool",
        "copy_can_multi: bool",
        "copy_multi_max_size: usize",
        "copy_multi_min_size: usize",
        "rename: bool",
        "list: bool",
        "list_with_limit: bool",
        "list_with_start_after: bool",
        "list_with_recursive: bool",
        "list_with_versions: bool",
        "list_with_deleted: bool",
        "presign: bool",
        "presign_read: bool",
        "presign_stat: bool",
        "presign_write: bool",
        "presign_delete: bool",
        "shared: bool",
    ];

    /// The `opendalCapability` mirror of `RELEASED_C_FIELDS`, copied from
    /// `bindings/go/types.go` at `v0.59.5`.
    const RELEASED_GO_FIELDS: [&str; 57] = [
        "stat uint8",
        "statWithIfMatch uint8",
        "statWithIfNoneMatch uint8",
        "statWithIfModifiedSince uint8",
        "statWithIfUnmodifiedSince uint8",
        "statWithOverrideCacheControl uint8",
        "statWithOverrideContentDisposition uint8",
        "statWithOverrideContentType uint8",
        "statWithVersion uint8",
        "read uint8",
        "readWithIfMatch uint8",
        "readWithIfNoneMatch uint8",
        "readWithOverrideCacheControl uint8",
        "readWithOverrideContentDisposition uint8",
        "readWithOverrideContentType uint8",
        "readWithIfModifiedSince uint8",
        "readWithIfUnmodifiedSince uint8",
        "readWithVersion uint8",
        "write uint8",
        "writeCanMulti uint8",
        "writeCanEmpty uint8",
        "writeCanAppend uint8",
        "writeWithContentType uint8",
        "writeWithContentDisposition uint8",
        "writeWithContentEncoding uint8",
        "writeWithCacheControl uint8",
        "writeWithIfMatch uint8",
        "writeWithIfNoneMatch uint8",
        "writeWithIfNotExists uint8",
        "writeWithUserMetadata uint8",
        "writeMultiMaxSize uint",
        "writeMultiMinSize uint",
        "writeTotalMaxSize uint",
        "createDir uint8",
        "delete uint8",
        "deleteWithVersion uint8",
        "deleteWithRecursive uint8",
        "copy uint8",
        "copyWithIfNotExists uint8",
        "copyWithIfMatch uint8",
        "copyWithSourceVersion uint8",
        "copyCanMulti uint8",
        "copyMultiMaxSize uint",
        "copyMultiMinSize uint",
        "rename uint8",
        "list uint8",
        "listWithLimit uint8",
        "listWithStartAfter uint8",
        "listWithRecursive uint8",
        "listWithVersions uint8",
        "listWithDeleted uint8",
        "presign uint8",
        "presignRead uint8",
        "presignStat uint8",
        "presignWrite uint8",
        "presignDelete uint8",
        "shared uint8",
    ];

    fn workspace_dir() -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
    }

    fn core_fields() -> Vec<capability::CapabilityField> {
        capability::parse(&workspace_dir().join("core/core/src/types/capability.rs"))
            .expect("parse core capability")
    }

    fn rendered(path: &str) -> String {
        render_capability(&workspace_dir())
            .unwrap()
            .into_iter()
            .find(|(relative, _)| *relative == path)
            .map(|(_, content)| content)
            .expect("rendered output")
    }

    /// Field declarations of the struct opened by `open`, without Rust's
    /// `pub` and trailing comma and with whitespace collapsed.
    fn struct_fields(source: &str, open: &str) -> Vec<String> {
        let body = source.split_once(open).expect("struct start").1;
        let body = body.split_once("\n}").expect("struct end").0;
        body.lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with("//"))
            .map(|line| {
                line.trim_start_matches("pub ")
                    .trim_end_matches(',')
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect()
    }

    #[test]
    fn test_rendered_capability_keeps_released_prefix() {
        let c = struct_fields(
            &rendered("bindings/c/src/capability.rs"),
            "pub struct opendal_capability {",
        );
        let go = struct_fields(
            &rendered("bindings/go/capability.go"),
            "type opendalCapability struct {",
        );
        assert_eq!(c[..RELEASED_C_FIELDS.len()], RELEASED_C_FIELDS);
        assert_eq!(go[..RELEASED_GO_FIELDS.len()], RELEASED_GO_FIELDS);
        assert_eq!(c.len(), core_fields().len());
        assert_eq!(go.len(), c.len());
    }

    #[test]
    fn test_frozen_order_rejects_unlisted_core_field() {
        let mut fields = core_fields();
        fields.insert(
            fields.len() / 2,
            capability::CapabilityField {
                name: "new_mid".to_string(),
                is_bool: true,
                doc: String::new(),
            },
        );
        let err = frozen_order(fields).unwrap_err().to_string();
        assert!(
            err.contains("`new_mid` must be appended to FROZEN_CAPABILITY_ORDER"),
            "{err}"
        );
    }

    #[test]
    fn test_frozen_order_rejects_missing_frozen_field() {
        let mut fields = core_fields();
        fields.retain(|f| f.name != "read");
        let err = frozen_order(fields).unwrap_err().to_string();
        assert!(err.contains("`read` is missing from core"), "{err}");
    }

    #[test]
    fn test_go_names() {
        assert_eq!(to_pascal("write_multi_max_size"), "WriteMultiMaxSize");
        assert_eq!(lower_first("WriteMultiMaxSize"), "writeMultiMaxSize");
        assert_eq!(to_pascal("stat"), "Stat");

        assert_eq!(go_method_name("content_md5"), "ContentMD5");
        assert_eq!(go_method_name("etag"), "ETag");
        assert_eq!(go_field_name("content_md5"), "contentMD5");
        assert_eq!(go_field_name("etag"), "etag");
        assert_eq!(go_field_name("last_modified"), "lastModified");
    }
}
