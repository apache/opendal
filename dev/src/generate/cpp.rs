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

use super::capability;
use anyhow::{Context, Result};
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

/// Core names of the members `opendal::Capability` exposed before it was
/// generated, in their historical declaration order.
///
/// The public class is an aggregate, so callers may brace-initialize it by
/// position. These members stay first, in this order, so such code keeps its
/// meaning when recompiled. Fields added later are appended in core order.
const HISTORICAL_PUBLIC_ORDER: &[&str] = &[
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
];

pub fn generate(workspace_dir: &Path) -> Result<()> {
    generate_capability(workspace_dir)
}

/// Order core fields for the public C++ class: historical members first, in
/// their historical order, then every other core field in core order.
///
/// Fails if a historical member no longer exists in core, because dropping it
/// would silently break source compatibility.
fn public_order<T>(fields: &[T], name_of: impl Fn(&T) -> &str) -> Result<Vec<usize>> {
    let mut order = Vec::with_capacity(fields.len());
    for historical in HISTORICAL_PUBLIC_ORDER {
        let index = fields
            .iter()
            .position(|f| name_of(f) == *historical)
            .with_context(|| {
                format!("historical C++ capability member `{historical}` is missing from core")
            })?;
        order.push(index);
    }
    for (index, field) in fields.iter().enumerate() {
        if !HISTORICAL_PUBLIC_ORDER.contains(&name_of(field)) {
            order.push(index);
        }
    }
    Ok(order)
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
        let content = env
            .get_template(template)?
            .render(context! { fields => fields, public_fields => public_fields })?;
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
    fn test_rendered_header_declares_public_order() {
        let header = rendered("bindings/cpp/include/capability.hpp");
        let fields = core_fields();
        let order = public_order(&fields, |f| f.name.as_str()).unwrap();
        let mut last = 0;
        for &i in &order {
            let decl = format!(" {}{{", cpp_name(&fields[i].name));
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
    fn test_public_order_appends_rest_in_core_order() {
        let fields = core_fields();
        let order = public_order(&fields, |f| f.name.as_str()).unwrap();
        let appended = &order[HISTORICAL_PUBLIC_ORDER.len()..];
        assert!(appended.windows(2).all(|w| w[0] < w[1]));
        for &i in appended {
            assert!(!HISTORICAL_PUBLIC_ORDER.contains(&fields[i].name.as_str()));
        }
    }

    #[test]
    fn test_public_order_covers_every_core_field_once() {
        let fields = core_fields();
        let mut order = public_order(&fields, |f| f.name.as_str()).unwrap();
        order.sort_unstable();
        assert_eq!(order, (0..fields.len()).collect::<Vec<_>>());
    }

    #[test]
    fn test_public_order_rejects_missing_historical_member() {
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
}
