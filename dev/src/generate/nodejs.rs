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

use super::capability::{self, CapabilityField};
use super::core_options::{self, OptionField, OptionKind, OptionsStruct};
use super::metadata::{self, MetadataKind};
use anyhow::{Result, bail};
use minijinja::value::Serde;
use minijinja::{Environment, context};
use serde::Serialize;
use std::fs;
use std::path::Path;

pub fn generate(workspace_dir: &Path) -> Result<()> {
    generate_capability(workspace_dir)?;
    generate_metadata(workspace_dir)?;
    generate_list_options(workspace_dir)
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
        .render(context! { fields => Serde(&fields) })?;

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
        .render(context! { fields => Serde(&fields) })?;

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

/// The core `ListOptions` fields that JS exposes, in declaration order, with
/// the core kind that their JS type was chosen for.
///
/// A core field missing here is not exposed in JS, and the conversion sets it
/// to its default. Generation fails when core changes the kind of an exposed
/// field, so the JS type never changes silently.
const LIST_OPTIONS_FIELDS: &[(&str, OptionKind)] = &[
    ("limit", OptionKind::OptionUsize),
    ("start_after", OptionKind::OptionString),
    ("recursive", OptionKind::Bool),
    ("versions", OptionKind::Bool),
    ("deleted", OptionKind::Bool),
];

/// Render model for one field of the napi `ListOptions` object.
#[derive(Serialize)]
struct ListOptionsField {
    /// snake_case name, as in core. napi exposes it in camelCase.
    name: String,
    /// Rust type of the napi field. napi-rs derives the TS type from it.
    rs_type: &'static str,
    /// The core doc comment, one entry per line.
    doc: Vec<String>,
}

/// One field of the core `ListOptions` literal that the conversion builds.
#[derive(Serialize)]
struct ListOptionsConversion {
    name: String,
    /// Expression that reads the JS value from `value`.
    expr: String,
}

/// Render `bindings/nodejs/src/options/list.rs` from the core `ListOptions`
/// in `core/core/src/types/options.rs`.
///
/// `generated.d.ts` is derived from this file by `napi build`.
fn generate_list_options(workspace_dir: &Path) -> Result<()> {
    let source = workspace_dir.join("core/core/src/types/options.rs");
    let capabilities = capability::parse(&workspace_dir.join("core/core/src/types/capability.rs"))?;
    let rendered = render_list_options(&core_options::parse(&source)?, &capabilities)?;

    let output = workspace_dir.join("bindings/nodejs/src/options/list.rs");
    fs::create_dir_all(output.parent().expect("output has a parent"))?;
    fs::write(output, format!("{rendered}\n"))?;
    Ok(())
}

fn render_list_options(
    parsed: &[OptionsStruct],
    capabilities: &[CapabilityField],
) -> Result<String> {
    let Some(core) = parsed.iter().find(|options| options.name == "ListOptions") else {
        bail!("core has no options struct `ListOptions`");
    };

    let mut fields = Vec::with_capacity(LIST_OPTIONS_FIELDS.len());
    for &(name, kind) in LIST_OPTIONS_FIELDS {
        let Some(field) = core.fields.iter().find(|field| field.name == name) else {
            bail!("core ListOptions has no field `{name}`");
        };
        if field.kind != kind {
            bail!(
                "core ListOptions::{name} is {:?}, but its JS type was chosen for {kind:?}",
                field.kind
            );
        }
        fields.push(ListOptionsField {
            name: field.name.clone(),
            rs_type: list_option_type(field)?.0,
            doc: js_doc(&field.doc, capabilities),
        });
    }

    // List every core field instead of ending with `..Default::default()`, so
    // a new core field fails to compile in the binding until this file is
    // regenerated, and the regenerated conversion shows it as unexposed.
    let mut conversions = Vec::with_capacity(core.fields.len());
    for field in &core.fields {
        let exposed = LIST_OPTIONS_FIELDS
            .iter()
            .any(|&(name, _)| name == field.name);
        let expr = if exposed {
            list_option_type(field)?.1
        } else {
            "Default::default()".to_string()
        };
        conversions.push(ListOptionsConversion {
            name: field.name.clone(),
            expr,
        });
    }

    let mut env = Environment::new();
    env.add_template("list_rs", include_str!("nodejs/list_options_rs.j2"))?;
    Ok(env.get_template("list_rs")?.render(context! {
        doc => js_doc(&core.doc, capabilities),
        fields => Serde(&fields),
        conversions => Serde(&conversions),
        has_unmirrored_fields => core.has_unmirrored_fields,
    })?)
}

/// The napi type of an exposed core `ListOptions` field and the expression
/// that converts it back.
///
/// These keep the types the binding used before codegen: `limit` is a
/// `number` read as `u32`, so an explicit `0` reaches core as `Some(0)`, and
/// an omitted boolean is `false`.
fn list_option_type(field: &OptionField) -> Result<(&'static str, String)> {
    let name = &field.name;
    Ok(match field.kind {
        OptionKind::OptionUsize => ("Option<u32>", format!("value.{name}.map(|v| v as usize)")),
        OptionKind::OptionString => ("Option<String>", format!("value.{name}")),
        OptionKind::Bool => ("Option<bool>", format!("value.{name}.unwrap_or_default()")),
        kind => bail!("core ListOptions::{name} is {kind:?}, which has no JS type yet"),
    })
}

/// The core doc comment as it reads in JS. napi-rs copies it into
/// `generated.d.ts`.
///
/// Rustdoc links become plain code spans, because JS documentation cannot
/// resolve Rust paths: ``[`Capability::list_with_limit`](crate::Capability::list_with_limit)``
/// and ``[`crate::layers::SimulateLayer`]`` keep only their code span.
/// Code spans that name a JS property then use its JS name (see
/// [`js_code_span`]); other code spans stay as written.
fn js_doc(lines: &[String], capabilities: &[CapabilityField]) -> Vec<String> {
    lines
        .iter()
        .map(|line| {
            let mut unlinked = String::with_capacity(line.len());
            let mut rest = line.as_str();
            while let Some(start) = rest.find("[`") {
                let Some(len) = rest[start + 1..].find("`]") else {
                    break;
                };
                unlinked.push_str(&rest[..start]);
                unlinked.push_str(&rest[start + 1..start + len + 2]);
                rest = &rest[start + len + 3..];
                if let Some(target) = rest.strip_prefix('(')
                    && let Some(end) = target.find(')')
                {
                    rest = &target[end + 1..];
                }
            }
            unlinked.push_str(rest);

            let mut out = String::with_capacity(unlinked.len());
            let mut rest = unlinked.as_str();
            while let Some(start) = rest.find('`') {
                let Some(len) = rest[start + 1..].find('`') else {
                    break;
                };
                let code = &rest[start + 1..start + 1 + len];
                out.push_str(&rest[..=start]);
                out.push_str(&js_code_span(code, capabilities).unwrap_or_else(|| code.to_string()));
                out.push('`');
                rest = &rest[start + len + 2..];
            }
            out.push_str(rest);
            out
        })
        .collect()
}

/// The JS name of a code span that names a property of the Node.js surface,
/// or `None` to keep the code span as written.
///
/// napi-rs exposes snake_case Rust names in lower camel case, so an exposed
/// `ListOptions` field like `start_after` reads `startAfter`, and a core
/// capability like `Capability::list_with_start_after` reads
/// `Capability.listWithStartAfter`, the getter on the JS `Capability` that
/// `op.capability()` returns.
fn js_code_span(code: &str, capabilities: &[CapabilityField]) -> Option<String> {
    if LIST_OPTIONS_FIELDS.iter().any(|&(name, _)| name == code) {
        return Some(heck::AsLowerCamelCase(code).to_string());
    }
    let name = code
        .strip_prefix("crate::")
        .unwrap_or(code)
        .strip_prefix("Capability::")?;
    capabilities
        .iter()
        .any(|f| f.name == name)
        .then(|| format!("Capability.{}", heck::AsLowerCamelCase(name)))
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
            .render(context! { fields => Serde(&fields) })
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

    fn core_list_options() -> OptionsStruct {
        let source = crate::workspace_dir().join("core/core/src/types/options.rs");
        core_options::parse(&source)
            .unwrap()
            .into_iter()
            .find(|options| options.name == "ListOptions")
            .unwrap()
    }

    fn core_capabilities() -> Vec<CapabilityField> {
        capability::parse(&crate::workspace_dir().join("core/core/src/types/capability.rs"))
            .unwrap()
    }

    /// The napi struct, which napi-rs turns into `generated.d.ts`.
    fn napi_struct(rendered: &str) -> &str {
        let start = rendered.find("#[napi(object)]").unwrap();
        let end = rendered[start..].find("\n}\n").unwrap();
        &rendered[start..start + end]
    }

    #[test]
    fn test_list_options_new_core_field_is_not_exposed() {
        let core = core_list_options();
        let mut mutated = core.clone();
        mutated.fields.insert(
            0,
            core_options::OptionField {
                name: "fake_option".to_string(),
                kind: OptionKind::OptionString,
                doc: vec!["A fake option.".to_string()],
            },
        );

        let before = render_list_options(&[core], &core_capabilities()).unwrap();
        let after = render_list_options(&[mutated], &core_capabilities()).unwrap();
        assert_eq!(napi_struct(&before), napi_struct(&after));
        assert!(!before.contains("fake_option"));
        assert!(after.contains("fake_option: Default::default(),"));
    }

    #[test]
    fn test_list_options_reject_unknown_shapes() {
        let mut missing = core_list_options();
        missing.fields.retain(|field| field.name != "limit");
        let err = render_list_options(&[missing], &core_capabilities())
            .unwrap_err()
            .to_string();
        assert!(err.contains("has no field `limit`"), "{err}");

        let mut changed = core_list_options();
        let limit = changed
            .fields
            .iter_mut()
            .find(|f| f.name == "limit")
            .unwrap();
        limit.kind = OptionKind::OptionU64;
        let err = render_list_options(&[changed], &core_capabilities())
            .unwrap_err()
            .to_string();
        assert!(err.contains("ListOptions::limit is OptionU64"), "{err}");

        // A kind change to another supported kind must fail too, instead of
        // silently turning `startAfter` into a boolean.
        let mut changed = core_list_options();
        let start_after = changed
            .fields
            .iter_mut()
            .find(|f| f.name == "start_after")
            .unwrap();
        start_after.kind = OptionKind::Bool;
        let err = render_list_options(&[changed], &core_capabilities())
            .unwrap_err()
            .to_string();
        assert!(err.contains("ListOptions::start_after is Bool"), "{err}");
    }

    #[test]
    fn test_list_options_docs_come_from_core() {
        let mut core = core_list_options();
        core.doc = vec!["Struct doc from core.".to_string()];
        let limit = core.fields.iter_mut().find(|f| f.name == "limit").unwrap();
        limit.doc = vec![
            "First line.".to_string(),
            String::new(),
            "Third line.".to_string(),
        ];

        let rendered = render_list_options(&[core], &core_capabilities()).unwrap();
        assert!(rendered.contains("/// Struct doc from core.\n#[napi(object)]"));
        assert!(rendered.contains(
            "    /// First line.\n    ///\n    /// Third line.\n    pub limit: Option<u32>,"
        ));
    }

    #[test]
    fn test_js_doc_strips_rustdoc_links() {
        let lines = [
            "Rely on [`crate::layers::SimulateLayer`](crate::layers::SimulateLayer) and [`Self::recursive`].",
            "A [plain](https://opendal.apache.org) link and `code` stay.",
        ]
        .map(String::from);
        assert_eq!(
            js_doc(&lines, &core_capabilities()),
            [
                "Rely on `crate::layers::SimulateLayer` and `Self::recursive`.",
                "A [plain](https://opendal.apache.org) link and `code` stay.",
            ]
        );
    }

    #[test]
    fn test_js_doc_uses_js_names() {
        let lines = [
            "- Pagination: `limit`, `start_after`.",
            "Requires [`Capability::list_with_limit`](crate::Capability::list_with_limit).",
            "Requires [`crate::Capability::list_with_versions`] and `Capability::list_with_deleted`.",
            "Unknown `Capability::no_such_flag`, `start_after_key`, `glob`, and prose start_after stay.",
        ]
        .map(String::from);
        assert_eq!(
            js_doc(&lines, &core_capabilities()),
            [
                "- Pagination: `limit`, `startAfter`.",
                "Requires `Capability.listWithLimit`.",
                "Requires `Capability.listWithVersions` and `Capability.listWithDeleted`.",
                "Unknown `Capability::no_such_flag`, `start_after_key`, `glob`, and prose start_after stay.",
            ]
        );
    }

    #[test]
    fn test_list_options_docs_use_js_names() {
        let rendered = render_list_options(&[core_list_options()], &core_capabilities()).unwrap();
        // Core writes `start_after` as a plain code span in the struct doc and
        // the capability as a rustdoc link in the field doc.
        assert!(rendered.contains("/// - Pagination: `limit`, `startAfter`.\n"));
        assert!(rendered.contains(
            "    /// Setting this requires `Capability.listWithStartAfter`.\n    pub start_after: Option<String>,"
        ));
        for line in rendered
            .lines()
            .filter(|l| l.trim_start().starts_with("///"))
        {
            assert!(!line.contains("`start_after`"), "{line}");
            assert!(!line.contains("Capability::"), "{line}");
        }
    }
}
