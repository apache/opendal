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

//! Generator for the OCaml binding.
//!
//! Renders the Rust FFI accessors (`src/operator/capability.rs` and
//! `src/operator/metadata.rs`) and the public `Capability` and `Metadata`
//! modules (`lib/capability.ml{,i}` and `lib/metadata.ml{,i}`). The `external`
//! declarations in `src/operator.mli` are derived from the Rust files by
//! `ocaml-build` during `dune build`.

use super::capability;
use super::metadata::{self, MetadataKind};
use anyhow::{Result, bail};
use minijinja::{Environment, context};
use serde::Serialize;
use std::fs;
use std::path::Path;

pub fn generate(workspace_dir: &Path) -> Result<()> {
    generate_capability(workspace_dir)?;
    generate_metadata(workspace_dir)
}

fn generate_capability(workspace_dir: &Path) -> Result<()> {
    let source = workspace_dir.join("core/core/src/types/capability.rs");
    let fields = capability::parse(&source)?;

    let mut env = Environment::new();
    env.add_filter("ocaml_doc", ocaml_doc);
    env.add_template("capability_rs", include_str!("ocaml/capability_rs.j2"))?;
    env.add_template("capability_ml", include_str!("ocaml/capability_ml.j2"))?;
    env.add_template("capability_mli", include_str!("ocaml/capability_mli.j2"))?;

    let outputs = [
        ("capability_rs", "bindings/ocaml/src/operator/capability.rs"),
        ("capability_ml", "bindings/ocaml/lib/capability.ml"),
        ("capability_mli", "bindings/ocaml/lib/capability.mli"),
    ];
    for (template, relative) in outputs {
        let rendered = env
            .get_template(template)?
            .render(context! { fields => fields })?;
        fs::write(workspace_dir.join(relative), format!("{rendered}\n"))?;
    }
    Ok(())
}

fn generate_metadata(workspace_dir: &Path) -> Result<()> {
    let source = workspace_dir.join("core/core/src/types/metadata.rs");
    let accessors = metadata_accessors(metadata::parse(&source)?)?;

    let mut env = Environment::new();
    env.add_filter("ocaml_doc", ocaml_doc);
    env.add_template("metadata_rs", include_str!("ocaml/metadata_rs.j2"))?;
    env.add_template("metadata_ml", include_str!("ocaml/metadata_ml.j2"))?;
    env.add_template("metadata_mli", include_str!("ocaml/metadata_mli.j2"))?;

    let outputs = [
        ("metadata_rs", "bindings/ocaml/src/operator/metadata.rs"),
        ("metadata_ml", "bindings/ocaml/lib/metadata.ml"),
        ("metadata_mli", "bindings/ocaml/lib/metadata.mli"),
    ];
    for (template, relative) in outputs {
        let rendered = env
            .get_template(template)?
            .render(context! { accessors => accessors })?;
        fs::write(workspace_dir.join(relative), format!("{rendered}\n"))?;
    }
    Ok(())
}

/// One `Metadata` accessor as rendered for OCaml.
#[derive(Debug, Serialize)]
struct MetadataAccessor {
    /// Accessor name in snake_case, shared by core, the FFI function and OCaml.
    name: String,
    /// OCaml type of the result.
    ocaml_type: &'static str,
    /// Rust return type of the FFI function.
    rs_type: &'static str,
    /// Rust expression that converts the core value into `rs_type`.
    rs_expr: String,
    /// First paragraph of the core doc, unescaped.
    doc: String,
    /// Extra odoc sentence describing the OCaml representation, if any.
    ocaml_note: &'static str,
}

/// Accessors that core derives from `mode`.
///
/// The shared parser skips them because they carry no data of their own, but
/// the OCaml binding has always exposed them, so they are rendered right after
/// `mode`.
const DERIVED_FROM_MODE: [(&str, &str); 2] = [
    ("is_file", "Return true if this metadata is for a file."),
    ("is_dir", "Return true if this metadata is for a directory."),
];

fn metadata_accessors(fields: Vec<metadata::MetadataField>) -> Result<Vec<MetadataAccessor>> {
    if !fields
        .iter()
        .any(|f| f.name == "mode" && f.kind == MetadataKind::Mode)
    {
        bail!(
            "metadata accessor `mode` is gone; \
             update DERIVED_FROM_MODE in dev/src/generate/ocaml.rs"
        );
    }

    let mut accessors = Vec::with_capacity(fields.len() + DERIVED_FROM_MODE.len());
    for f in fields {
        let is_mode = f.name == "mode";
        accessors.push(metadata_accessor(f.name, f.kind, f.doc));
        if is_mode {
            for (name, doc) in DERIVED_FROM_MODE {
                accessors.push(metadata_accessor(
                    name.to_string(),
                    MetadataKind::Bool,
                    doc.to_string(),
                ));
            }
        }
    }
    Ok(accessors)
}

/// How each core return type crosses into OCaml.
///
/// `mode` becomes the `entry_mode` variant declared in `metadata_rs.j2`,
/// `u64` and timestamps become `int64`, and user metadata becomes an
/// association list.
fn metadata_accessor(name: String, kind: MetadataKind, doc: String) -> MetadataAccessor {
    let call = format!("metadata.0.{name}()");
    let (ocaml_type, rs_type, rs_expr, ocaml_note) = match kind {
        MetadataKind::Mode => ("entry_mode", "EntryMode", format!("{call}.into()"), ""),
        MetadataKind::Bool => ("bool", "bool", call, ""),
        MetadataKind::U64 => ("int64", "u64", call, ""),
        MetadataKind::OptionBool => ("bool option", "Option<bool>", call, ""),
        MetadataKind::OptionStr => (
            "string option",
            "Option<String>",
            format!("{call}.map(String::from)"),
            "",
        ),
        MetadataKind::OptionTimestamp => (
            "int64 option",
            "Option<i64>",
            format!("{call}.map(|t| t.into_inner().as_second())"),
            "The value is in seconds since the Unix epoch.",
        ),
        MetadataKind::OptionUserMetadata => (
            "(string * string) list option",
            "Option<std::collections::LinkedList<(String, String)>>",
            format!(
                "{call}.map(|m| m.into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect())"
            ),
            "The pairs are copied into an association list.",
        ),
    };
    MetadataAccessor {
        name,
        ocaml_type,
        rs_type,
        rs_expr,
        doc,
        ocaml_note,
    }
}

/// Escape text for an odoc comment: `{`, `}`, `[`, `]` and `@` are markup.
fn ocaml_doc(doc: &str) -> String {
    let mut out = String::with_capacity(doc.len());
    for c in doc.chars() {
        if matches!(c, '{' | '}' | '[' | ']' | '@') {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ocaml_doc_escapes_markup() {
        assert_eq!(ocaml_doc("see [x] @a {b}"), "see \\[x\\] \\@a \\{b\\}");
    }

    #[test]
    fn test_metadata_accessors_keep_derived_after_mode() {
        let path = crate::workspace_dir().join("core/core/src/types/metadata.rs");
        let accessors = metadata_accessors(metadata::parse(&path).unwrap()).unwrap();

        let names: Vec<&str> = accessors.iter().map(|a| a.name.as_str()).collect();
        assert_eq!(names[..3], ["mode", "is_file", "is_dir"]);

        let last_modified = accessors
            .iter()
            .find(|a| a.name == "last_modified")
            .unwrap();
        assert_eq!(last_modified.ocaml_type, "int64 option");
    }
}
