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

//! Operation options for the C binding and the Go options that fill them.
//!
//! Released option structs are C ABI: `opendal.h` fixes their member order
//! and types, and C and Zig callers may read or set members directly.
//! [`RELEASED_OPTIONS`] records that surface. Docs come from the core doc
//! comments, plus how C and Go encode an unset option. Core fields it does not
//! list keep their core default in the conversion and stay out of C and Go.
//!
//! Only `ListOptions` is generated so far. The other option structs are still
//! written by hand in `bindings/c/src/types.rs` and the Go operation files.

use std::path::Path;

use anyhow::{Result, bail};
use minijinja::value::Serde;
use minijinja::{Environment, context};
use serde::Serialize;

use crate::generate::capability::{self, CapabilityField};
use crate::generate::core_options::{self, OptionField, OptionKind, OptionsStruct};
use crate::generate::rustdoc::rewrite_code_spans;

/// How a released C struct stores one core field, and how Go fills it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Repr {
    /// `bool`. Go always calls the setter.
    Bool,
    /// `Option<usize>` as `usize`, where `0` means unset. Go skips `0`.
    UsizeZeroUnset,
    /// `Option<String>` as a `char *` that the setter copies and `_free`
    /// releases. Go passes any string the caller set, including an empty one.
    OwnedString,
}

impl Repr {
    /// The core field shape this representation carries.
    fn kind(self) -> OptionKind {
        match self {
            Repr::Bool => OptionKind::Bool,
            Repr::UsizeZeroUnset => OptionKind::OptionUsize,
            Repr::OwnedString => OptionKind::OptionString,
        }
    }
}

/// One released C option struct and the Go options that fill it.
struct OptionsSpec {
    /// Core struct name, e.g. `ListOptions`.
    core: &'static str,
    /// Released core fields in C member order.
    fields: &'static [(&'static str, Repr)],
}

/// The released C option structs, in `opendal.h` order.
const RELEASED_OPTIONS: &[OptionsSpec] = &[OptionsSpec {
    core: "ListOptions",
    fields: &[
        ("recursive", Repr::Bool),
        ("limit", Repr::UsizeZeroUnset),
        ("start_after", Repr::OwnedString),
        ("versions", Repr::Bool),
        ("deleted", Repr::Bool),
    ],
}];

/// Render model of one C option struct and the Go options that fill it.
#[derive(Serialize)]
struct OptionsModel {
    /// Core struct name, e.g. `ListOptions`.
    core: &'static str,
    /// C struct name, e.g. `opendal_list_options`.
    c_name: String,
    c_doc: Vec<String>,
    new_doc: Vec<String>,
    free_doc: Vec<String>,
    /// Go operation prefix, e.g. `List` in `ListWithLimit`.
    go_prefix: &'static str,
    go_fn_doc: Vec<String>,
    /// Go private options struct, e.g. `listOptions`.
    go_private: String,
    fields: Vec<FieldModel>,
    /// Conversion struct literal fields in core order, defaults included.
    conversion: Vec<String>,
}

#[derive(Serialize)]
struct FieldModel {
    /// C member and core field name.
    name: &'static str,
    repr: Repr,
    c_type: &'static str,
    c_default: &'static str,
    c_doc: Vec<String>,
    setter_doc: Vec<String>,
    /// Go option function, e.g. `ListWithLimit`.
    go_option: String,
    /// Field of the Go private options struct, e.g. `startAfter`.
    go_field: String,
    /// `go_field` padded so that gofmt column alignment is already satisfied.
    go_field_padded: String,
    /// Type of `go_field`.
    go_type: &'static str,
    /// FFI variable of the C setter, e.g. `ffiListOptionsSetLimit`.
    go_ffi: String,
    go_doc: Vec<String>,
}

pub(super) fn generate(workspace_dir: &Path) -> Result<()> {
    let source = workspace_dir.join("core/core/src/types/options.rs");
    let capabilities = capability::parse(&workspace_dir.join("core/core/src/types/capability.rs"))?;
    let models = build(&core_options::parse(&source)?, &capabilities)?;

    let mut env = Environment::new();
    env.add_template("options_rs", include_str!("options_rs.j2"))?;
    env.add_template("options_go", include_str!("options_go.j2"))?;
    let outputs = [
        ("options_rs", "bindings/c/src/options.rs"),
        ("options_go", "bindings/go/options.go"),
    ];
    for (template, relative) in outputs {
        let rendered = env
            .get_template(template)?
            .render(context! { options => Serde(&models) })?;
        std::fs::write(workspace_dir.join(relative), format!("{rendered}\n"))?;
    }
    Ok(())
}

fn build(parsed: &[OptionsStruct], capabilities: &[CapabilityField]) -> Result<Vec<OptionsModel>> {
    RELEASED_OPTIONS
        .iter()
        .map(|spec| {
            let Some(core) = parsed.iter().find(|options| options.name == spec.core) else {
                bail!("core options are missing `{}`, which C releases", spec.core);
            };
            options_model(core, spec, capabilities)
        })
        .collect()
}

fn options_model(
    core: &OptionsStruct,
    spec: &OptionsSpec,
    capabilities: &[CapabilityField],
) -> Result<OptionsModel> {
    let go_prefix = spec.core.strip_suffix("Options").unwrap_or(spec.core);
    let base = heck::AsSnakeCase(go_prefix).to_string();
    let c_name = format!("opendal_{base}_options");

    let mut fields = Vec::new();
    for &(name, repr) in spec.fields {
        let core_field = core_field(core, name, repr)?;
        let c_doc = binding_doc(&core_field.doc, spec, capabilities, Lang::C);
        let go_doc = binding_doc(&core_field.doc, spec, capabilities, Lang::Go);
        let (c_type, c_default, go_type) = match repr {
            Repr::Bool => ("bool", "false", "bool"),
            Repr::UsizeZeroUnset => ("usize", "0", "uint"),
            Repr::OwnedString => ("*mut c_char", "std::ptr::null_mut()", "*string"),
        };
        // Core options have no C or Go encoding of "unset", so the binding
        // states its own.
        let (c_unset, c_param, go_unset) = match repr {
            Repr::Bool => (None, "The value to set.", None),
            Repr::UsizeZeroUnset => (
                Some("`0` means unset."),
                "The value to set; `0` unsets it.",
                Some("A value of 0 leaves it unset."),
            ),
            Repr::OwnedString => (
                Some("NULL means unset."),
                "The string to copy; NULL unsets it.",
                None,
            ),
        };
        let pascal = super::to_pascal(name);
        let go_option = format!("{go_prefix}With{pascal}");
        fields.push(FieldModel {
            name,
            repr,
            c_type,
            c_default,
            c_doc: with_note(&c_doc, c_unset),
            setter_doc: [format!("\\brief Set `{name}`."), String::new()]
                .into_iter()
                .chain(c_doc.iter().cloned())
                .chain([
                    String::new(),
                    format!("@param opts The {c_name} to modify."),
                    format!("@param {name} {c_param}"),
                ])
                .collect(),
            go_doc: with_note(
                &[
                    format!("{go_option} sets the {name} option."),
                    String::new(),
                ]
                .into_iter()
                .chain(go_doc)
                .collect::<Vec<_>>(),
                go_unset,
            ),
            go_option,
            go_field: super::lower_first(&pascal),
            go_field_padded: String::new(),
            go_type,
            go_ffi: format!("ffi{go_prefix}OptionsSet{pascal}"),
        });
    }
    // Pad the Go struct fields as gofmt aligns them.
    let width = fields.iter().map(|f| f.go_field.len()).max().unwrap_or(0);
    for field in &mut fields {
        field.go_field_padded = format!("{:<width$} ", field.go_field);
    }

    let mut c_doc = binding_doc(&core.doc, spec, capabilities, Lang::C);
    if let Some(first) = c_doc.first_mut() {
        *first = format!("\\brief {first}");
    }
    c_doc.extend([
        String::new(),
        format!("Use `{c_name}_new()` to construct and `{c_name}_free()` to free."),
        String::new(),
        format!("@see opendal_operator_{base}_with"),
        format!("@see {c_name}_new"),
        format!("@see {c_name}_free"),
    ]);
    c_doc.extend(
        fields
            .iter()
            .map(|f| format!("@see {c_name}_set_{}", f.name)),
    );

    let mut go_fn_doc = binding_doc(&core.doc, spec, capabilities, Lang::Go);
    if let Some(first) = go_fn_doc.first_mut() {
        let purpose = first.strip_prefix("Options for ").unwrap_or(first);
        *first = format!("With{go_prefix}Fn is a functional option for {purpose}");
    }

    Ok(OptionsModel {
        conversion: conversion(core, &fields)?,
        core: spec.core,
        new_doc: vec![
            format!("\\brief Construct a heap-allocated {c_name} with default values."),
            String::new(),
            format!("@return A new {c_name} with all options set to their defaults."),
            String::new(),
            format!("@see {c_name}_free"),
        ],
        free_doc: vec![
            format!("\\brief Free the heap memory used by {c_name}."),
            String::new(),
            format!("@param opts The {c_name} to free."),
        ],
        c_name,
        c_doc,
        go_prefix,
        go_fn_doc,
        go_private: format!("{}Options", super::lower_first(go_prefix)),
        fields,
    })
}

/// The core field of a released C member. Fail when it is gone from core or
/// changed its shape, since the member and its setter are ABI.
fn core_field<'a>(core: &'a OptionsStruct, name: &str, repr: Repr) -> Result<&'a OptionField> {
    let Some(field) = core.fields.iter().find(|f| f.name == name) else {
        bail!("core {} is missing released C field `{name}`", core.name);
    };
    if field.kind != repr.kind() {
        bail!(
            "core {} field `{name}` has kind {:?}; its released C member needs {:?}",
            core.name,
            field.kind,
            repr.kind()
        );
    }
    Ok(field)
}

/// The binding that a doc comment is rendered for.
#[derive(Clone, Copy)]
enum Lang {
    C,
    Go,
}

/// The core doc comment as `lang` shows it, one entry per line.
///
/// Rustdoc links become plain code spans, as in the Node.js generator,
/// because C and Go documentation cannot resolve Rust paths:
/// ``[`Capability::list_with_limit`](crate::Capability::list_with_limit)``
/// and ``[`crate::layers::SimulateLayer`]`` keep only their code span.
/// Code spans that name a released option or a capability then use their
/// binding name (see [`code_span`]); other code spans stay as written.
fn binding_doc(
    lines: &[String],
    spec: &OptionsSpec,
    capabilities: &[CapabilityField],
    lang: Lang,
) -> Vec<String> {
    lines
        .iter()
        .map(|line| rewrite_code_spans(line, |code| code_span(code, spec, capabilities, lang)))
        .collect()
}

/// The binding name of a code span that names a released field of `spec` or
/// a core capability, or `None` to keep the code span as written.
///
/// - C: a released field is the struct member of the same name, such as
///   `start_after`, and `Capability::list_with_start_after` is the
///   `opendal_capability.list_with_start_after` member.
/// - Go: a released field is its option function, such as
///   `ListWithStartAfter`, and `Capability::list_with_start_after` is the
///   `Capability.ListWithStartAfter` method that `generate_capability` emits.
fn code_span(
    code: &str,
    spec: &OptionsSpec,
    capabilities: &[CapabilityField],
    lang: Lang,
) -> Option<String> {
    if spec.fields.iter().any(|&(name, _)| name == code) {
        return Some(match lang {
            Lang::C => code.to_owned(),
            Lang::Go => {
                let go_prefix = spec.core.strip_suffix("Options").unwrap_or(spec.core);
                format!("{go_prefix}With{}", super::to_pascal(code))
            }
        });
    }
    let name = code
        .strip_prefix("crate::")
        .unwrap_or(code)
        .strip_prefix("Capability::")?;
    capabilities
        .iter()
        .any(|f| f.name == name)
        .then(|| match lang {
            Lang::C => format!("opendal_capability.{name}"),
            Lang::Go => format!("Capability.{}", super::to_pascal(name)),
        })
}

/// `doc` followed by `note` as its own paragraph.
fn with_note(doc: &[String], note: Option<&str>) -> Vec<String> {
    let mut lines = doc.to_vec();
    if let Some(note) = note {
        lines.extend([String::new(), note.to_owned()]);
    }
    lines
}

/// Width limits of rustfmt's default configuration.
const MAX_WIDTH: usize = 100;
const CHAIN_WIDTH: usize = 60;
/// Indent of the fields of the conversion struct literal.
const FIELD_INDENT: usize = 12;

/// The conversion struct literal fields in core order. Fields without a C
/// member keep their core default.
fn conversion(core: &OptionsStruct, fields: &[FieldModel]) -> Result<Vec<String>> {
    core.fields
        .iter()
        .map(|core_field| {
            let name = &core_field.name;
            let line = match fields.iter().find(|f| f.name == name) {
                Some(field) => match field.repr {
                    Repr::Bool => format!("{name}: self.{name},"),
                    Repr::UsizeZeroUnset => {
                        let chain = format!("(self.{name} != 0).then_some(self.{name})");
                        if chain.len() > CHAIN_WIDTH {
                            bail!("conversion of `{name}` would be rewrapped by rustfmt");
                        }
                        format!("{name}: {chain},")
                    }
                    Repr::OwnedString => {
                        format!("{name}: unsafe {{ optional_string(self.{name}, \"{name}\") }},")
                    }
                },
                None => format!("{name}: {},", default_value(core_field.kind)),
            };
            if FIELD_INDENT + line.len() > MAX_WIDTH {
                bail!("conversion of `{name}` would be rewrapped by rustfmt");
            }
            Ok(line)
        })
        .collect()
}

fn default_value(kind: OptionKind) -> &'static str {
    match kind {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn core_options() -> Vec<OptionsStruct> {
        let source = crate::workspace_dir().join("core/core/src/types/options.rs");
        core_options::parse(&source).expect("core options must parse")
    }

    fn core_capabilities() -> Vec<CapabilityField> {
        let source = crate::workspace_dir().join("core/core/src/types/capability.rs");
        capability::parse(&source).expect("core capability must parse")
    }

    /// The rendered C options and Go options.
    fn render(parsed: &[OptionsStruct]) -> (String, String) {
        let models = context! { options => Serde(&build(parsed, &core_capabilities()).unwrap()) };
        let env = Environment::new();
        let c = env
            .render_str(include_str!("options_rs.j2"), models.clone())
            .unwrap();
        let go = env
            .render_str(include_str!("options_go.j2"), models)
            .unwrap();
        (c, go)
    }

    /// The doc comment lines directly above the first line that starts with
    /// `item`, without their comment prefix.
    fn doc_above<'a>(rendered: &'a str, prefix: &str, item: &str) -> Vec<&'a str> {
        let lines: Vec<&str> = rendered.lines().collect();
        let at = lines
            .iter()
            .position(|line| line.starts_with(item))
            .unwrap_or_else(|| panic!("`{item}` must be rendered"));
        let mut doc: Vec<&str> = lines[..at]
            .iter()
            .rev()
            .map_while(|line| line.strip_prefix(prefix))
            .map(str::trim)
            .collect();
        doc.reverse();
        doc
    }

    /// The code spans in the doc comment lines of `rendered`.
    fn doc_code_spans<'a>(rendered: &'a str, prefix: &str) -> Vec<&'a str> {
        rendered
            .lines()
            .filter_map(|line| line.trim_start().strip_prefix(prefix))
            .flat_map(|doc| doc.split('`').skip(1).step_by(2))
            .collect()
    }

    #[test]
    fn test_docs_come_from_core() {
        let parsed = core_options();
        let (c, go) = render(&parsed);
        let spec = &RELEASED_OPTIONS[0];
        let list = parsed.iter().find(|o| o.name == "ListOptions").unwrap();
        for &(name, _) in spec.fields {
            let core_field = list.fields.iter().find(|f| f.name == name).unwrap();

            let doc = binding_doc(&core_field.doc, spec, &core_capabilities(), Lang::C);
            let doc: Vec<&str> = doc.iter().map(String::as_str).collect();
            let member = doc_above(&c, "    ///", &format!("    pub {name}: "));
            assert!(
                member.starts_with(&doc),
                "C member docs of `{name}`: {member:?}"
            );

            let doc = binding_doc(&core_field.doc, spec, &core_capabilities(), Lang::Go);
            let doc: Vec<&str> = doc.iter().map(String::as_str).collect();
            let option = format!("func ListWith{}(", super::super::to_pascal(name));
            let option_doc = doc_above(&go, "//", &option);
            assert!(
                option_doc.windows(doc.len()).any(|lines| lines == doc),
                "Go docs of `{name}`: {option_doc:?}"
            );
        }
    }

    #[test]
    fn test_binding_doc_strips_rustdoc_links() {
        let lines = [
            "Rely on [`crate::layers::SimulateLayer`](crate::layers::SimulateLayer) and [`Self::recursive`].",
            "A [plain](https://opendal.apache.org) link and `code` stay.",
        ]
        .map(String::from);
        let expected = [
            "Rely on `crate::layers::SimulateLayer` and `Self::recursive`.",
            "A [plain](https://opendal.apache.org) link and `code` stay.",
        ];
        for lang in [Lang::C, Lang::Go] {
            let doc = binding_doc(&lines, &RELEASED_OPTIONS[0], &core_capabilities(), lang);
            assert_eq!(doc, expected);
        }
    }

    #[test]
    fn test_binding_doc_uses_binding_names() {
        let lines = [
            "- Pagination: `limit`, `start_after`.",
            "Requires [`Capability::list_with_limit`](crate::Capability::list_with_limit).",
            "Requires [`crate::Capability::list_with_versions`] and `Capability::list_with_deleted`.",
            "Unknown `Capability::no_such_flag`, `start_after_key`, `glob`, and prose start_after stay.",
        ]
        .map(String::from);
        let spec = &RELEASED_OPTIONS[0];
        assert_eq!(
            binding_doc(&lines, spec, &core_capabilities(), Lang::C),
            [
                "- Pagination: `limit`, `start_after`.",
                "Requires `opendal_capability.list_with_limit`.",
                "Requires `opendal_capability.list_with_versions` and `opendal_capability.list_with_deleted`.",
                "Unknown `Capability::no_such_flag`, `start_after_key`, `glob`, and prose start_after stay.",
            ]
        );
        assert_eq!(
            binding_doc(&lines, spec, &core_capabilities(), Lang::Go),
            [
                "- Pagination: `ListWithLimit`, `ListWithStartAfter`.",
                "Requires `Capability.ListWithLimit`.",
                "Requires `Capability.ListWithVersions` and `Capability.ListWithDeleted`.",
                "Unknown `Capability::no_such_flag`, `start_after_key`, `glob`, and prose start_after stay.",
            ]
        );
    }

    #[test]
    fn test_list_docs_name_the_generated_surface() {
        let (c, go) = render(&core_options());

        // Core writes `start_after` as a plain code span in the struct doc and
        // the capability as a rustdoc link in the field doc.
        assert!(c.contains("/// - Pagination: `limit`, `start_after`.\n"));
        assert!(c.contains(
            "    /// Setting this requires `opendal_capability.list_with_start_after`.\n"
        ));
        assert!(go.contains("// - Pagination: `ListWithLimit`, `ListWithStartAfter`.\n"));
        assert!(go.contains("// Setting this requires `Capability.ListWithStartAfter`.\n"));

        // Every option or capability that the docs name exists in the
        // generated C header and Go capability.
        let workspace = crate::workspace_dir();
        let header =
            std::fs::read_to_string(workspace.join("bindings/c/include/opendal.h")).unwrap();
        let struct_body = |name: &str| -> String {
            let start = header
                .find(&format!("typedef struct {name} {{"))
                .unwrap_or_else(|| panic!("opendal.h must define {name}"));
            let end = start + header[start..].find(&format!("}} {name};")).unwrap();
            header[start..end].to_owned()
        };
        let c_members = |name: &str| -> Vec<String> {
            struct_body(name)
                .lines()
                .filter_map(|line| line.trim().strip_suffix(';'))
                .filter_map(|decl| decl.rsplit([' ', '*']).next())
                .map(str::to_owned)
                .collect()
        };
        let released: Vec<&str> = RELEASED_OPTIONS[0]
            .fields
            .iter()
            .map(|&(name, _)| name)
            .collect();
        let list_members = c_members("opendal_list_options");
        let capability_members = c_members("opendal_capability");
        for code in doc_code_spans(&c, "///") {
            assert!(!code.contains("Capability::"), "{code}");
            if let Some(member) = code.strip_prefix("opendal_capability.") {
                assert!(capability_members.iter().any(|m| m == member), "{code}");
            } else if released.contains(&code) {
                assert!(list_members.iter().any(|m| m == code), "{code}");
            }
        }

        let capability_go =
            std::fs::read_to_string(workspace.join("bindings/go/capability.go")).unwrap();
        for code in doc_code_spans(&go, "//") {
            assert!(!code.contains("Capability::"), "{code}");
            assert!(!released.contains(&code), "{code} must use its Go name");
            if let Some(method) = code.strip_prefix("Capability.") {
                assert!(
                    capability_go.contains(&format!("func (c *Capability) {method}() ")),
                    "{code}"
                );
            } else if code.starts_with("ListWith") {
                assert!(go.contains(&format!("\nfunc {code}(")), "{code}");
            }
        }
    }

    #[test]
    fn test_conversion_defaults_unreleased_list_fields() {
        let (rendered, _) = render(&core_options());
        let conversion = rendered
            .split("pub(crate) unsafe fn to_core(&self) -> options::ListOptions {")
            .nth(1)
            .and_then(|rest| rest.split("\n}\n").next())
            .expect("the list conversion must be rendered");
        // The conversion reads `start_after` through a raw pointer, so safe
        // code must not reach it through a `From` impl.
        assert!(!rendered.contains("impl From<"), "{rendered}");
        let defaulted: Vec<&str> = conversion
            .lines()
            .filter_map(|line| line.trim().strip_suffix(','))
            .filter_map(|field| field.split_once(": "))
            .filter(|(_, value)| ["None", "false", "0", "Default::default()"].contains(value))
            .map(|(name, _)| name)
            .collect();
        // `glob` is the only core list option that C and Go do not expose (#9323).
        assert_eq!(defaulted, ["glob"]);
    }

    #[test]
    fn test_released_fields_must_stay_in_core() {
        let mut parsed = core_options();
        let list = parsed.iter_mut().find(|o| o.name == "ListOptions").unwrap();
        list.fields.retain(|f| f.name != "start_after");
        let err = build(&parsed, &core_capabilities())
            .err()
            .unwrap()
            .to_string();
        assert!(
            err.contains("missing released C field `start_after`"),
            "{err}"
        );

        // Any kind change fails, including one to another kind that C can
        // encode, so `start_after` never silently becomes a `bool` member.
        for (name, kind, message) in [
            ("limit", OptionKind::Usize, "`limit` has kind Usize"),
            (
                "start_after",
                OptionKind::Bool,
                "`start_after` has kind Bool",
            ),
        ] {
            let mut parsed = core_options();
            let list = parsed.iter_mut().find(|o| o.name == "ListOptions").unwrap();
            list.fields
                .iter_mut()
                .find(|f| f.name == name)
                .unwrap()
                .kind = kind;
            let err = build(&parsed, &core_capabilities())
                .err()
                .unwrap()
                .to_string();
            assert!(err.contains(message), "{err}");
        }
    }
}
