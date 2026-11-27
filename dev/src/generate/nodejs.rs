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

//! Generator for the Node.js binding.

use super::capability;
use super::metadata::{self, MetadataKind};
use anyhow::Result;
use minijinja::{Environment, context};
use serde::Serialize;
use std::fs;
use std::path::Path;

pub fn generate(workspace_dir: &Path) -> Result<()> {
    generate_capability(workspace_dir)?;
    generate_metadata(workspace_dir)
}

/// Render `bindings/nodejs/src/capability.rs` from
/// `core/core/src/types/capability.rs`, so that the JS `Capability` exposes
/// every core capability field with its core doc comment.
///
/// `generated.d.ts` is derived from this file by `napi build`.
fn generate_capability(workspace_dir: &Path) -> Result<()> {
    let source = workspace_dir.join("core/core/src/types/capability.rs");
    let fields = capability::parse(&source)?;

    let mut env = Environment::new();
    env.add_template("capability_rs", include_str!("nodejs/capability_rs.j2"))?;
    let rendered = env
        .get_template("capability_rs")?
        .render(context! { fields => fields })?;

    let output = workspace_dir.join("bindings/nodejs/src/capability.rs");
    fs::write(output, format!("{rendered}\n"))?;
    Ok(())
}

/// Render model for one accessor of the JS `Metadata` class. Every per-kind
/// decision is precomputed in `metadata_field` so the template stays purely
/// structural.
#[derive(Serialize)]
struct MetadataField {
    /// snake_case accessor name, as in core. napi exposes it in camelCase.
    name: String,
    /// The core return shape; the template adds `isFile()` and
    /// `isDirectory()` after `mode`.
    kind: MetadataKind,
    /// First paragraph of the core accessor doc, unless it does not hold for
    /// the JS form.
    doc: String,
    /// Extra doc paragraph for shapes whose JS form needs explaining, empty
    /// when none.
    note: String,
    /// `getter` for a JS property, empty for a JS method.
    napi: String,
    /// Rust return type of the napi accessor.
    rs_type: String,
    /// Expression that converts the core value, reading from `self.0`.
    rs_expr: String,
}

/// Render `bindings/nodejs/src/metadata.rs` from the accessors of
/// `core/core/src/types/metadata.rs`, so that the JS `Metadata` exposes every
/// core accessor with its core doc comment.
///
/// `generated.d.ts` is derived from this file by `napi build`.
fn generate_metadata(workspace_dir: &Path) -> Result<()> {
    let source = workspace_dir.join("core/core/src/types/metadata.rs");
    let fields: Vec<MetadataField> = metadata::parse(&source)?
        .into_iter()
        .map(metadata_field)
        .collect();

    let mut env = Environment::new();
    env.add_template("metadata_rs", include_str!("nodejs/metadata_rs.j2"))?;
    let rendered = env
        .get_template("metadata_rs")?
        .render(context! { fields => fields })?;

    let output = workspace_dir.join("bindings/nodejs/src/metadata.rs");
    fs::write(output, format!("{rendered}\n"))?;
    Ok(())
}

/// Precompute how one core accessor surfaces in JS.
///
/// This table is the only place that maps a core return shape to its JS
/// form. It keeps the types the binding exposed before codegen, so existing
/// code sees no change: booleans are methods like `fs.Stats#isFile()`,
/// everything else is a nullable getter, timestamps are RFC 3339 strings, and
/// user metadata is a plain object. The match is exhaustive, so a new shape
/// in `metadata.rs` fails to compile here until it is mapped.
fn metadata_field(f: metadata::MetadataField) -> MetadataField {
    let name = f.name;
    let mut doc = f.doc;
    let getter = "getter".to_string();
    let method = String::new();

    let (napi, rs_type, rs_expr, note) = match f.kind {
        MetadataKind::Mode => (
            getter,
            "Option<crate::EntryMode>",
            format!("Some(self.0.{name}().into())"),
            "",
        ),
        MetadataKind::Bool => (method, "bool", format!("self.0.{name}()"), ""),
        MetadataKind::U64 => (getter, "Option<u64>", format!("Some(self.0.{name}())"), ""),
        MetadataKind::OptionBool => (
            method,
            "Option<bool>",
            format!("self.0.{name}()"),
            "Returns `null` when the service does not report it.",
        ),
        MetadataKind::OptionStr => (
            getter,
            "Option<String>",
            format!("self.0.{name}().map(|v| v.to_string())"),
            "",
        ),
        MetadataKind::OptionTimestamp => (
            getter,
            "Option<String>",
            format!("self.0.{name}().map(|v| v.to_string())"),
            "The time is formatted as RFC 3339, like `1996-12-19T16:39:57Z`.",
        ),
        MetadataKind::OptionUserMetadata => {
            // Core returns a borrowed view, while JS receives a copy, so the
            // core doc would be misleading here.
            doc = format!(
                "Return a copy of this entry's {} as a plain object, or `null` when absent.",
                name.replace('_', " ")
            );
            (
                getter,
                "Option<std::collections::HashMap<String, String>>",
                format!(
                    "self.0.{name}().map(|m| \
                     m.into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect())"
                ),
                "Each read returns a new object, so mutating it does not update this `Metadata`.",
            )
        }
    };

    MetadataField {
        name,
        kind: f.kind,
        doc,
        note: note.to_string(),
        napi,
        rs_type: rs_type.to_string(),
        rs_expr,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(name: &str, kind: MetadataKind) -> MetadataField {
        metadata_field(metadata::MetadataField {
            name: name.to_string(),
            kind,
            doc: "Doc.".to_string(),
        })
    }

    #[test]
    fn test_metadata_predicates_are_methods() {
        assert_eq!(field("is_deleted", MetadataKind::Bool).napi, "");
        assert_eq!(field("is_current", MetadataKind::OptionBool).napi, "");
        assert_eq!(field("etag", MetadataKind::OptionStr).napi, "getter");
        assert_eq!(field("mode", MetadataKind::Mode).napi, "getter");
    }

    #[test]
    fn test_metadata_keeps_existing_js_types() {
        let cases = [
            ("mode", MetadataKind::Mode, "Option<crate::EntryMode>"),
            ("content_length", MetadataKind::U64, "Option<u64>"),
            (
                "last_modified",
                MetadataKind::OptionTimestamp,
                "Option<String>",
            ),
            (
                "user_metadata",
                MetadataKind::OptionUserMetadata,
                "Option<std::collections::HashMap<String, String>>",
            ),
        ];
        for (name, kind, rs_type) in cases {
            assert_eq!(field(name, kind).rs_type, rs_type, "{name}");
        }
    }

    #[test]
    fn test_render_core_metadata() {
        let source = crate::workspace_dir().join("core/core/src/types/metadata.rs");
        let fields: Vec<MetadataField> = metadata::parse(&source)
            .unwrap()
            .into_iter()
            .map(metadata_field)
            .collect();

        let mut env = Environment::new();
        env.add_template("metadata_rs", include_str!("nodejs/metadata_rs.j2"))
            .unwrap();
        let rendered = env
            .get_template("metadata_rs")
            .unwrap()
            .render(context! { fields => fields })
            .unwrap();

        for expected in [
            "#[napi]\n    pub fn is_file(&self) -> bool",
            "#[napi]\n    pub fn is_directory(&self) -> bool",
            "#[napi]\n    pub fn is_current(&self) -> Option<bool>",
            "#[napi(getter)]\n    pub fn content_language(&self) -> Option<String>",
        ] {
            assert!(rendered.contains(expected), "missing `{expected}`");
        }
        assert!(!rendered.contains("pub fn is_dir(&self)"));
        // JS receives a copy of user metadata, not core's borrowed view.
        assert!(!rendered.contains("borrowed"));
    }
}
