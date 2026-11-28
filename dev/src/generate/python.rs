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

use crate::generate::capability::{self, CapabilityField};
use crate::generate::core_options::{self, OptionKind, OptionsStruct};
use crate::generate::metadata::{self, MetadataKind};
use crate::generate::options;
use crate::generate::parser::{Config, ConfigType, Services, service_feature, sorted_services};
use anyhow::{Context, Result, bail};
use minijinja::value::Serde;
use minijinja::{Environment, context};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use toml_edit::{DocumentMut, Item};

/// Naming of the service sets the PyPI wheels are built with per platform, for
/// example `services-platform-linux`.
const PLATFORM_FEATURE_PREFIX: &str = "services-platform-";

// Declarative layers with Python config schemas in python_config.j2.
const LAYER_CONFIGS: &[&str] = &[
    "concurrent_limit",
    "logging",
    "mime_guess",
    "retry",
    "timeout",
];

/// Subset of the `cargo metadata --format-version 1` output consumed here.
/// The schema is controlled by cargo, so only the fields this generator reads
/// are declared; serde ignores everything else.
#[derive(Deserialize)]
struct CargoMetadata {
    packages: Vec<MetadataPackage>,
    resolve: MetadataResolve,
}

#[derive(Deserialize)]
struct MetadataPackage {
    name: String,
    id: String,
}

#[derive(Deserialize)]
struct MetadataResolve {
    nodes: Vec<MetadataResolveNode>,
}

#[derive(Deserialize)]
struct MetadataResolveNode {
    id: String,
    features: Vec<String>,
}

fn cargo_metadata(workspace_dir: &Path, features: &[String]) -> Result<CargoMetadata> {
    let mut command = Command::new("cargo");
    command
        .arg("metadata")
        .arg("--format-version")
        .arg("1")
        .arg("--manifest-path")
        .arg(workspace_dir.join("bindings/python/Cargo.toml"));
    if !features.is_empty() {
        command.arg("--features").arg(features.join(","));
    }

    let output = command
        .output()
        .context("failed to run `cargo metadata` for bindings/python")?;
    if !output.status.success() {
        bail!(
            "`cargo metadata` failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    serde_json::from_slice(&output.stdout).context("failed to parse `cargo metadata` output")
}

/// Parse the `services-platform-*` feature names declared in the `[features]`
/// table of `bindings/python/Cargo.toml`.
fn platform_features(workspace_dir: &Path) -> Result<Vec<String>> {
    let manifest_path = workspace_dir.join("bindings/python/Cargo.toml");
    let manifest = fs::read_to_string(&manifest_path)
        .with_context(|| format!("failed to read {}", manifest_path.display()))?;
    let document = manifest
        .parse::<DocumentMut>()
        .with_context(|| format!("failed to parse {}", manifest_path.display()))?;
    let features = document
        .get("features")
        .and_then(Item::as_table)
        .with_context(|| format!("no [features] table in {}", manifest_path.display()))?;

    let mut platforms: Vec<String> = features
        .iter()
        .map(|(name, _)| name.to_string())
        .filter(|name| name.starts_with(PLATFORM_FEATURE_PREFIX))
        .collect();
    platforms.sort();
    if platforms.is_empty() {
        bail!(
            "no `{PLATFORM_FEATURE_PREFIX}*` feature declared in {}",
            manifest_path.display()
        );
    }

    Ok(platforms)
}

/// Resolve the facade features enabled across the PyPI wheel platforms.
/// `cargo metadata` only resolves features and never compiles, so services that
/// build on a subset of the wheel platforms, such as SFTP and Foyer, are still
/// resolved on every host.
///
/// The enabled services are read from the resolve node of the `opendal` facade
/// crate, whose `services-*` features are exactly one per service, instead of
/// the binding's own feature namespace, which also carries aggregate sets such
/// as `services-all` and `services-platform-*`.
fn enabled_facade_features(workspace_dir: &Path) -> Result<Vec<String>> {
    let platforms = platform_features(workspace_dir)?;
    let metadata = cargo_metadata(workspace_dir, &platforms)?;

    let package = metadata
        .packages
        .iter()
        .find(|package| package.name == "opendal")
        .with_context(|| "package `opendal` not found in `cargo metadata` output")?;
    let node = metadata
        .resolve
        .nodes
        .into_iter()
        .find(|node| node.id == package.id)
        .with_context(|| "resolve node for `opendal` not found in `cargo metadata` output")?;
    Ok(node.features)
}

pub fn generate(workspace_dir: PathBuf, services: Services) -> Result<()> {
    let features = enabled_facade_features(&workspace_dir)?;
    // Service keys are directory names under core/services, which mix `-` and
    // `_` (e.g. `aliyun-drive` vs `mini_moka`), so normalize both sides.
    let enabled_services: HashSet<String> = features
        .iter()
        .filter_map(|name| name.strip_prefix("services-"))
        .map(|service| service.replace('-', "_"))
        .collect();
    if enabled_services.is_empty() {
        bail!("no `services-*` feature enabled by the `{PLATFORM_FEATURE_PREFIX}*` features");
    }
    let srvs = sorted_services(services, |srv| {
        enabled_services.contains(&srv.replace('-', "_"))
    });
    let layers: Vec<_> = LAYER_CONFIGS
        .iter()
        .copied()
        .filter(|name| features.contains(&format!("layers-{}", snake_to_kebab_case(name))))
        .collect();
    let mut env = Environment::new();
    env.add_template("python", include_str!("python.j2"))?;
    env.add_template("python_config", include_str!("python_config.j2"))?;
    env.add_function("snake_to_kebab_case", snake_to_kebab_case);
    env.add_function("service_to_feature", service_feature);
    env.add_function("service_to_pascal", service_to_pascal);
    env.add_function("make_python_type", make_python_type);
    env.add_function("make_pydoc_param_header", make_pydoc_param_header);
    env.add_function("make_pydoc_param", make_pydoc_param);
    env.add_function("make_config_field_type", make_config_field_type);
    env.add_function("make_config_field_doc", make_config_field_doc);
    env.add_function("config_field_is_required", config_field_is_required);

    // Generate the `Scheme` enum (Rust).
    let tmpl = env.get_template("python")?;
    let output = workspace_dir.join("bindings/python/src/services.rs");
    let mut rendered = tmpl.render(context! { srvs => Serde(&srvs) })?;
    if !rendered.ends_with('\n') {
        rendered.push('\n');
    }
    fs::write(output, rendered)?;

    // Generate the service, context, and layer config TypedDicts (Python).
    let tmpl = env.get_template("python_config")?;
    let output = workspace_dir.join("bindings/python/python/opendal/config.py");
    let mut rendered = tmpl.render(context! { srvs => Serde(&srvs), layers => Serde(&layers) })?;
    if !rendered.ends_with('\n') {
        rendered.push('\n');
    }
    fs::write(output, rendered)?;

    // Generate the operator options that come from core (Rust), then the
    // typed operator options TypedDicts (Python).
    let generated = generate_options_rs(&workspace_dir)?;
    generate_options_py(&workspace_dir, &generated)?;

    // Generate the `Capability` pyclass (Rust) from core.
    generate_capability(&workspace_dir)?;

    // Generate the `Metadata` pyclass (Rust) from core.
    generate_metadata(&workspace_dir)?;

    Ok(())
}

/// Render `bindings/python/src/capability.rs` from
/// `core/core/src/types/capability.rs`, so that every core capability field is
/// exposed with its core doc comment and nothing is maintained by hand.
///
/// The `.pyi` stub is derived from this pyclass by `just stub-gen`.
fn generate_capability(workspace_dir: &Path) -> Result<()> {
    let source = workspace_dir.join("core/core/src/types/capability.rs");
    let fields = capability::parse(&source)?;

    let mut env = Environment::new();
    env.add_template("capability_rs", include_str!("python/capability_rs.j2"))?;
    let rendered = env
        .get_template("capability_rs")?
        .render(context! { fields => Serde(&fields) })?;

    let output = workspace_dir.join("bindings/python/src/capability.rs");
    fs::write(output, format!("{rendered}\n"))?;
    Ok(())
}

/// Render model handed to the `Metadata` template. Every per-kind decision is
/// precomputed in `metadata_field` so the template stays purely structural.
#[derive(Debug, Serialize)]
struct MetadataField {
    /// Accessor name in snake_case, exactly as written in core.
    name: String,
    /// First paragraph of the core accessor doc.
    doc: String,
    /// Rust return type of the getter, as converted to Python by PyO3.
    py_type: &'static str,
    /// Expression that produces the getter value from the core metadata.
    py_value: String,
    /// `format!` placeholder used for this accessor in `__repr__`.
    repr_fmt: Option<&'static str>,
}

/// Map one core accessor to its Python getter.
fn metadata_field(accessor: metadata::MetadataAccessor) -> Result<MetadataField> {
    if accessor.name == "checksum" && accessor.output == syn::parse_quote!(-> Option<Checksum>) {
        return Ok(MetadataField {
            name: accessor.name,
            doc: "Return the full-object checksum as (algorithm, raw digest bytes), or None."
                .to_string(),
            py_type: "Option<(&'static str, Vec<u8>)>",
            py_value: "self.0.checksum().and_then(options::checksum_into_python)".to_string(),
            repr_fmt: None,
        });
    }

    let f = accessor.into_field()?;
    let name = &f.name;
    let mut doc = f.doc;
    let (py_type, py_value, repr_fmt) = match f.kind {
        MetadataKind::Mode => (
            "EntryMode",
            format!("EntryMode::new(self.0.{name}())"),
            "{}",
        ),
        MetadataKind::Bool => ("bool", format!("self.0.{name}()"), "{}"),
        MetadataKind::U64 => ("u64", format!("self.0.{name}()"), "{}"),
        MetadataKind::OptionBool => ("Option<bool>", format!("self.0.{name}()"), "{:?}"),
        MetadataKind::OptionStr => ("Option<&str>", format!("self.0.{name}()"), "{:?}"),
        MetadataKind::OptionTimestamp => (
            "Option<jiff::Timestamp>",
            format!("self.0.{name}().map(utils::timestamp_to_jiff)"),
            "{:?}",
        ),
        MetadataKind::OptionUserMetadata => {
            // Core returns a borrowed view, while Python receives an owned copy,
            // so the core doc would be misleading here.
            doc = format!(
                "Return a copy of this entry's {} as a dict.",
                name.replace('_', " ")
            );
            (
                "Option<std::collections::HashMap<String, String>>",
                format!(
                    "self.0.{name}().map(|metadata| {{ metadata.into_iter().map(|(key, value)| (key.to_string(), value.to_string())).collect() }})"
                ),
                "{:?}",
            )
        }
    };

    Ok(MetadataField {
        name: f.name,
        doc,
        py_type,
        py_value,
        repr_fmt: Some(repr_fmt),
    })
}

/// Render `bindings/python/src/metadata.rs` from
/// `core/core/src/types/metadata.rs`, including the Python checksum mapping.
///
/// The derived `is_file` and `is_dir` getters are part of the template, and
/// the `.pyi` stub is derived from this pyclass by `just stub-gen`.
fn generate_metadata(workspace_dir: &Path) -> Result<()> {
    let source = workspace_dir.join("core/core/src/types/metadata.rs");
    let fields: Vec<MetadataField> = metadata::parse_accessors(&source)?
        .into_iter()
        .map(metadata_field)
        .collect::<Result<_>>()?;

    let mut env = Environment::new();
    env.add_template("metadata_rs", include_str!("python/metadata_rs.j2"))?;
    let rendered = env
        .get_template("metadata_rs")?
        .render(context! { fields => Serde(&fields) })?;

    let output = workspace_dir.join("bindings/python/src/metadata.rs");
    fs::write(output, format!("{rendered}\n"))?;
    Ok(())
}

fn snake_to_kebab_case(str: &str) -> String {
    str.replace('_', "-")
}

fn service_to_pascal(service: &str) -> String {
    let mut result = String::with_capacity(service.len());
    let mut capitalize = true;

    for &b in service.as_bytes() {
        if b == b'_' || b == b'-' {
            capitalize = true;
        } else if capitalize {
            result.push((b as char).to_ascii_uppercase());
            capitalize = false;
        } else {
            result.push(b as char);
        }
    }

    result
}

/// Format a block of text with trimming, wrapping, and indentation.
///
/// # Arguments
/// * `text` - The input text to format.
/// * `indent` - Number of spaces to prepend to each line.
/// * `max_line_length` - Maximum total line width including indentation.
///
/// # Returns
/// A formatted string where:
/// - Paragraphs and sentences are split properly.
/// - Each line is trimmed and wrapped within `max_line_length`.
/// - All lines end with a single `\n` (except the very last one).
/// - Whitespace is normalized and excess whitespace is removed.
pub fn format_text(text: &str, indent: usize, max_line_length: usize) -> String {
    // Precompute indentation string
    let indent_str = " ".repeat(indent);
    let effective_width = max_line_length.saturating_sub(indent);

    // Preallocate output buffer with a rough estimate (input len + ~20%)
    let mut output = String::with_capacity(text.len() + text.len() / 5);

    // Small closure to normalize whitespace (single space, trim edges)
    let normalize = |s: &str| {
        let mut result = String::with_capacity(s.len());
        let mut prev_space = true;
        for ch in s.chars() {
            if ch.is_whitespace() {
                if !prev_space {
                    result.push(' ');
                    prev_space = true;
                }
            } else {
                result.push(ch);
                prev_space = false;
            }
        }
        result.trim().to_owned()
    };

    // Split input into "segments" (roughly sentences or paragraphs)
    let mut start = 0;
    let chars: Vec<char> = text.chars().collect();
    let mut segments = Vec::new();

    for (i, &ch) in chars.iter().enumerate() {
        match ch {
            '.' | '!' | '?' if i + 1 < chars.len() && chars[i + 1].is_whitespace() => {
                // Sentence end
                segments.push(&text[start..=i]);
                start = i + 1;
            }
            '\n' => {
                if i > start {
                    segments.push(&text[start..i]);
                }
                start = i + 1;
            }
            _ => {}
        }
    }
    if start < text.len() {
        segments.push(&text[start..]);
    }

    // Process each normalized segment
    for segment in segments {
        let normalized = normalize(segment);
        if normalized.is_empty() {
            continue;
        }

        let mut line = String::with_capacity(effective_width);
        for word in normalized.split_whitespace() {
            if line.is_empty() {
                line.push_str(word);
            } else if line.len() + 1 + word.len() <= effective_width {
                line.push(' ');
                line.push_str(word);
            } else {
                // Emit current line
                output.push_str(&indent_str);
                output.push_str(line.trim_end());
                output.push('\n');

                // Start new line with this word
                line.clear();
                line.push_str(word);
            }
        }

        // Emit any remaining words in this paragraph
        if !line.is_empty() {
            output.push_str(&indent_str);
            output.push_str(line.trim_end());
            output.push('\n');
        }
    }

    // Remove any trailing newline(s)
    while output.ends_with('\n') {
        output.pop();
    }

    output
}

// Compose first line: param_name : type[, optional]
pub fn make_pydoc_param_header(
    name: &str,
    ty: Serde<ConfigType>,
    optional: bool,
) -> Result<String, minijinja::Error> {
    let py_type = make_python_type(ty)?;
    let optional_str = if optional { ", optional" } else { "" };
    let first_line = format!("{} : {}{}", name, py_type, optional_str);

    Ok(first_line)
}

/// Generate a properly indented NumPy-style docstring line for a parameter
pub fn make_pydoc_param(comments: &str, ty: Serde<ConfigType>) -> Result<String, minijinja::Error> {
    // Collect description parts
    let mut desc_parts = Vec::new();
    let comments = comments.trim();

    if !comments.is_empty() {
        let trimmed = comments.replace('\n', " ");
        if !trimmed.is_empty() {
            desc_parts.push(trimmed);
        }
    }

    match ty.0 {
        ConfigType::Duration => desc_parts.push(
            "a human readable duration string see https://docs.rs/humantime/latest/humantime/fn.parse_duration.html for more details".to_string()
        ),
        ConfigType::Vec => desc_parts.push(
            "A \",\" separated string, for example \"127.0.0.1:1,127.0.0.1:2\"".to_string()
        ),
        _ => {}
    }

    Ok(format_text(&desc_parts.join(". "), 20, 72)
        .trim_end()
        .to_string())
}

/// Field names that denote a local filesystem path, for which the generated
/// config item type is widened to accept `os.PathLike[str]` in addition to
/// `str`. Kept intentionally small and explicit.
fn is_path_like_field(name: &str) -> bool {
    matches!(name, "root" | "atomic_write_dir")
}

/// Render the Python type annotation for a generated `TypedDict` item.
///
/// Returns only the *base* value type (no `Required[...]`/`NotRequired[...]`
/// wrapper — the template applies that based on `config_field_is_required`).
/// Curated path-like `String` fields are widened to `str | os.PathLike[str]`.
fn make_config_field_type(field: Serde<Config>) -> Result<String, minijinja::Error> {
    Ok(match field.value {
        ConfigType::Bool => "bool".to_string(),
        ConfigType::Duration => "str | timedelta".to_string(),
        ConfigType::Usize
        | ConfigType::U64
        | ConfigType::I64
        | ConfigType::U32
        | ConfigType::U16 => "int".to_string(),
        ConfigType::Vec => "list[str]".to_string(),
        ConfigType::HashMap => "dict[str, str]".to_string(),
        ConfigType::String => {
            if is_path_like_field(&field.name) {
                "str | os.PathLike[str]".to_string()
            } else {
                "str".to_string()
            }
        }
    })
}

/// Whether a generated `TypedDict` item is `Required[...]`.
///
/// The base `TypedDict` is declared `total=False`, so every item is optional
/// (`NotRequired`) unless it is a required core field. `bool` flags are always
/// treated as optional, matching the Java binding and historical stub-gen.
fn config_field_is_required(field: Serde<Config>) -> Result<bool, minijinja::Error> {
    Ok(!field.optional && field.value != ConfigType::Bool)
}

/// Render the field docstring body, ready to embed in a `"""..."""` docstring.
///
/// Returns an empty string when there is nothing to document (the template then
/// omits the docstring). Backslashes and `"""` runs are escaped, and a trailing
/// `"` is escaped so it cannot merge with the closing delimiter.
fn make_config_field_doc(field: Serde<Config>) -> Result<String, minijinja::Error> {
    let mut parts = Vec::new();
    let comments = field.comments.trim();
    if !comments.is_empty() {
        let normalized = comments.replace('\n', " ");
        if !normalized.is_empty() {
            parts.push(normalized);
        }
    }

    match field.value {
        ConfigType::Duration => parts.push(
            "A datetime.timedelta or human readable duration string, e.g. \"5s\" (see \
             https://docs.rs/humantime/latest/humantime/fn.parse_duration.html)."
                .to_string(),
        ),
        ConfigType::Vec => {
            parts.push("A list of strings; serialized as a \",\" separated value.".to_string())
        }
        _ => {}
    }

    if let Some(deprecated) = &field.deprecated {
        parts.push(format!(
            "[Deprecated since {}] {}",
            deprecated.since,
            deprecated.note.trim()
        ));
    }

    Ok(escape_docstring(&parts.join(" ")))
}

/// Make a string safe to place inside a Python triple-quoted docstring.
fn escape_docstring(text: &str) -> String {
    let mut out = text.replace('\\', "\\\\").replace(r#"""""#, r#"\"\"\""#);
    // A docstring body ending in `"` would merge with the closing `"""`.
    if out.ends_with('"') {
        out.pop();
        out.push_str("\\\"");
    }
    out
}

fn make_python_type(ty: Serde<ConfigType>) -> Result<String, minijinja::Error> {
    Ok(match ty.0 {
        ConfigType::Bool => "builtins.bool",
        ConfigType::Duration => "typing.Any",
        ConfigType::I64
        | ConfigType::Usize
        | ConfigType::U64
        | ConfigType::U32
        | ConfigType::U16 => "builtins.int",
        ConfigType::Vec => "typing.Any",
        ConfigType::HashMap => "builtins.dict",
        ConfigType::String => "builtins.str",
    }
    .to_string())
}

/// License header shared by generated Python files.
const PYTHON_LICENSE_HEADER: &str = "\
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# \"License\"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
";

/// A Python options class generated from the core options struct of the
/// same name. The class and field docstrings come from the core doc comments.
struct PyOptions {
    /// The class name, e.g. `ListOptions`.
    name: &'static str,
    /// The keyword arguments in their Python order: the name, which is also
    /// the core field and the Rust field it sets, and the kind that core
    /// field must have. A core field that no keyword argument sets is not
    /// exposed and keeps its default.
    options: &'static [(&'static str, OptionKind)],
}

/// The Python options classes generated from `core/core/src/types/options.rs`.
/// The other classes in `bindings/python/src/options.rs` are hand-written.
const GENERATED_OPTIONS: &[PyOptions] = &[PyOptions {
    name: "ListOptions",
    options: &[
        ("limit", OptionKind::OptionUsize),
        ("start_after", OptionKind::OptionString),
        ("recursive", OptionKind::Bool),
        ("versions", OptionKind::Bool),
        ("deleted", OptionKind::Bool),
    ],
}];

/// The class order of `options.py`, which hand-written and generated classes
/// share.
const OPTIONS_PY_ORDER: &[&str] = &[
    "ReadOptions",
    "WriteOptions",
    "ListOptions",
    "StatOptions",
    "CopyOptions",
    "RenameOptions",
    "DeleteOptions",
];

/// Render model of one generated Python options class.
#[derive(Debug, Serialize)]
struct PyClass {
    name: &'static str,
    /// The core doc comment, one entry per line.
    doc: Vec<String>,
    fields: Vec<PyField>,
    /// Every field of the core struct in core order, with the expression
    /// that sets it.
    conversion: Vec<ConvertField>,
}

#[derive(Debug, Serialize)]
struct PyField {
    name: &'static str,
    kind: OptionKind,
    /// The core doc comment, one entry per line.
    doc: Vec<String>,
    rust_type: &'static str,
}

#[derive(Debug, Serialize)]
struct ConvertField {
    name: String,
    expr: String,
}

/// The Rust field type of a keyword argument and its `options.py` type, for
/// the core kinds that generated classes expose so far.
fn option_types(kind: OptionKind) -> Option<(&'static str, options::OptionType)> {
    Some(match kind {
        OptionKind::OptionString => ("Option<String>", options::OptionType::Str),
        OptionKind::OptionUsize => ("Option<usize>", options::OptionType::Int),
        OptionKind::Bool => ("Option<bool>", options::OptionType::Bool),
        _ => return None,
    })
}

/// The value of a core field that Python does not expose. Every core options
/// struct derives `Default`, so this equals the field's default.
fn default_expr(kind: OptionKind) -> &'static str {
    match kind {
        OptionKind::Bool => "false",
        OptionKind::Usize => "0",
        OptionKind::BytesRange => "Default::default()",
        _ => "None",
    }
}

/// The docstring of a generated class or field: the core doc comment, one
/// entry per line.
///
/// Rustdoc links become plain code spans, because Python documentation
/// cannot resolve Rust paths: ``[`Capability::list_with_limit`](crate::Capability::list_with_limit)``
/// and ``[`crate::layers::SimulateLayer`]`` keep only their code span.
/// Code spans that name a core capability then use its Python name (see
/// [`py_code_span`]); other code spans stay as written.
fn py_doc(
    core_doc: &[String],
    item: &str,
    capabilities: &[CapabilityField],
) -> Result<Vec<String>> {
    if core_doc.iter().all(|line| line.is_empty()) {
        bail!("core {item} has no doc comment");
    }
    let mut lines = Vec::with_capacity(core_doc.len());
    for line in core_doc {
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
            out.push_str(&py_code_span(code, capabilities).unwrap_or_else(|| code.to_string()));
            out.push('`');
            rest = &rest[start + len + 2..];
        }
        out.push_str(rest);
        if out.contains('"') || out.contains('\\') {
            bail!(
                "core {item} doc line `{out}` has a character docstrings must escape; \
                 teach dev/src/generate/python.rs about it"
            );
        }
        lines.push(out);
    }
    Ok(lines)
}

/// The Python name of a code span that names a core capability, or `None` to
/// keep the code span as written.
///
/// The `Capability` pyclass exposes every core capability as an attribute of
/// the same name, so `Capability::list_with_start_after` reads
/// `Capability.list_with_start_after`, the attribute of the `Capability` that
/// `Operator.capability()` returns. Keyword arguments keep their core names,
/// so a code span that names an options field, like `start_after`, needs no
/// rewrite.
fn py_code_span(code: &str, capabilities: &[CapabilityField]) -> Option<String> {
    let name = code
        .strip_prefix("crate::")
        .unwrap_or(code)
        .strip_prefix("Capability::")?;
    capabilities
        .iter()
        .any(|f| f.name == name)
        .then(|| format!("Capability.{name}"))
}

/// Build the render model of one generated class, checking the table against
/// the parsed core struct.
fn py_class(
    spec: &PyOptions,
    parsed: &[OptionsStruct],
    capabilities: &[CapabilityField],
) -> Result<PyClass> {
    let class = spec.name;
    let Some(core) = parsed.iter().find(|options| options.name == class) else {
        bail!("core options struct `{class}` not found");
    };
    if core.has_unmirrored_fields {
        bail!("core {class} has fields the core options parser skips; Python cannot set them");
    }
    let mut seen = HashSet::new();
    for field in &core.fields {
        if !seen.insert(field.name.as_str()) {
            bail!("core {class} contains duplicate field `{}`", field.name);
        }
    }

    let mut fields = Vec::with_capacity(spec.options.len());
    for &(name, kind) in spec.options {
        if fields.iter().any(|field: &PyField| field.name == name) {
            bail!("Python {class} lists `{name}` twice");
        }
        let Some(field) = core.fields.iter().find(|field| field.name == name) else {
            bail!("core {class} has no field `{name}` for Python {class}.{name}");
        };
        if field.kind != kind {
            bail!(
                "core {class}::{name} has kind {:?}; Python {class}.{name} requires {kind:?}",
                field.kind
            );
        }
        let Some((rust_type, _)) = option_types(kind) else {
            bail!(
                "Python {class}.{name} is a core {kind:?}; \
                 teach dev/src/generate/python.rs about it"
            );
        };
        fields.push(PyField {
            name,
            kind,
            doc: py_doc(&field.doc, &format!("{class}::{name}"), capabilities)?,
            rust_type,
        });
    }

    let conversion = core
        .fields
        .iter()
        .map(|field| {
            let name = &field.name;
            let expr = match fields.iter().find(|f| f.name == name) {
                None => default_expr(field.kind).to_string(),
                Some(f) if f.kind == OptionKind::Bool => format!("opts.{name}.unwrap_or(false)"),
                Some(_) => format!("opts.{name}"),
            };
            ConvertField {
                name: name.clone(),
                expr,
            }
        })
        .collect();

    Ok(PyClass {
        name: class,
        doc: py_doc(&core.doc, class, capabilities)?,
        fields,
        conversion,
    })
}

fn generated_classes(
    parsed: &[OptionsStruct],
    capabilities: &[CapabilityField],
) -> Result<Vec<PyClass>> {
    GENERATED_OPTIONS
        .iter()
        .map(|spec| py_class(spec, parsed, capabilities))
        .collect()
}

fn render_options_rs(classes: &[PyClass]) -> Result<String> {
    let mut env = Environment::new();
    env.add_template("options_rs", include_str!("python/options_rs.j2"))?;
    let rendered = env
        .get_template("options_rs")?
        .render(context! { classes => Serde(classes) })?;
    Ok(format!("{rendered}\n"))
}

/// Render `bindings/python/src/options/generated.rs`, the option classes
/// generated from `core/core/src/types/options.rs`.
fn generate_options_rs(workspace_dir: &Path) -> Result<Vec<PyClass>> {
    let source = workspace_dir.join("core/core/src/types/options.rs");
    let capabilities = capability::parse(&workspace_dir.join("core/core/src/types/capability.rs"))?;
    let classes = generated_classes(&core_options::parse(&source)?, &capabilities)?;
    let output = workspace_dir.join("bindings/python/src/options/generated.rs");
    fs::create_dir_all(output.parent().expect("output has a parent"))?;
    fs::write(output, render_options_rs(&classes)?)?;
    Ok(classes)
}

/// Python type of an operator option field, parsed from
/// `bindings/python/src/options.rs`.
fn make_option_type(ty: options::OptionType) -> &'static str {
    match ty {
        options::OptionType::Str => "str",
        options::OptionType::Int => "int",
        options::OptionType::Bool => "bool",
        options::OptionType::DateTime => "datetime",
        options::OptionType::Dict => "dict[str, str]",
        options::OptionType::ChecksumAlgo => "ChecksumAlgorithm | None",
    }
}

/// Generate `bindings/python/python/opendal/options.py`, the typed keyword
/// arguments accepted by the operator methods.
///
/// The option fields come from the hand-written `#[pyclass]` structs in
/// `bindings/python/src/options.rs` and from the generated classes, so the
/// generated file stays in sync with the Rust option types.
fn generate_options_py(workspace_dir: &Path, generated: &[PyClass]) -> Result<()> {
    let output = workspace_dir.join("bindings/python/python/opendal/options.py");
    fs::write(output, render_options_py(workspace_dir, generated)?)?;
    Ok(())
}

fn render_options_py(workspace_dir: &Path, generated: &[PyClass]) -> Result<String> {
    let source = workspace_dir.join("bindings/python/src/options.rs");
    let mut options = options::parse(&source)?;
    if options.is_empty() {
        anyhow::bail!("no option structs parsed from {}", source.display());
    }
    for class in generated {
        options.push(options::Options {
            name: class.name.to_string(),
            doc: class.doc.join("\n"),
            fields: class
                .fields
                .iter()
                .map(|field| options::OptionField {
                    name: field.name.to_string(),
                    ty: option_types(field.kind)
                        .expect("generated fields have a Python type")
                        .1,
                    doc: field.doc.join("\n"),
                })
                .collect(),
        });
    }
    let mut ordered = Vec::with_capacity(options.len());
    for name in OPTIONS_PY_ORDER {
        let Some(at) = options.iter().position(|opts| opts.name == *name) else {
            bail!("options class {name} is neither hand-written nor generated");
        };
        ordered.push(options.remove(at));
    }
    if let Some(extra) = options.first() {
        bail!("add {} to OPTIONS_PY_ORDER", extra.name);
    }
    let options = ordered;

    let mut out = String::new();
    out.push_str(PYTHON_LICENSE_HEADER);
    out.push_str("\n# DO NOT EDIT IT MANUALLY. This file is generated by opendal/dev/generate/python.rs.\n\n");
    out.push_str(OPTIONS_MODULE_DOC);
    out.push_str(
        "\nfrom __future__ import annotations\n\nfrom datetime import datetime\nfrom typing import Literal, TypeAlias, TypedDict\n",
    );
    out.push_str(
        "\nChecksumAlgorithm: TypeAlias = Literal[\n    \"crc32c\", \"crc64nvme\", \"crc64ecma\", \"md5\", \"sha1\", \"sha256\"\n]\n\"\"\"Full-object checksum algorithms. Each service defines the algorithms it supports.\"\"\"\n",
    );

    let mut exports = vec!["ChecksumAlgorithm".to_string()];
    for opts in &options {
        // `ReadOptions` maps to the `ReadKwargs` TypedDict.
        let kwargs = format!(
            "{}Kwargs",
            opts.name.strip_suffix("Options").unwrap_or(&opts.name)
        );
        exports.push(kwargs.clone());
        let doc = if opts.doc.is_empty() {
            "Keyword arguments for the corresponding `Operator` / `AsyncOperator` method."
                .to_string()
        } else {
            opts.doc.clone()
        };
        out.push_str(&format!("\n\nclass {kwargs}(TypedDict, total=False):\n"));
        push_docstring(&mut out, &doc);
        out.push('\n');
        for field in &opts.fields {
            out.push_str(&format!(
                "    {}: {}\n",
                field.name,
                make_option_type(field.ty)
            ));
            push_docstring(&mut out, &field.doc);
        }
    }

    // `open` accepts the union of the read and write options, so its kwargs
    // TypedDict inherits both.
    exports.push("OpenKwargs".to_string());
    out.push_str(
        "\n\nclass OpenKwargs(ReadKwargs, WriteKwargs, total=False):\n    \"\"\"Keyword arguments for `Operator.open` and `AsyncOperator.open`.\n\n    Accepted keys depend on `mode`: read modes (`\"r\"`, `\"rb\"`, `\"br\"`) accept\n    reader options only, while write, append, and create modes (`\"w\"`, `\"wb\"`,\n    `\"ab\"`, `\"xb\"`, etc.) accept writer options only.\n\n    The `content_type`, `cache_control`, and `content_disposition` options\n    apply only when writing, appending, or creating and raise `TypeError` in\n    read mode. Use the operator's `read()` method to supply response header\n    overrides.\n    \"\"\"\n",
    );

    out.push_str("\n\n__all__ = [\n");
    for name in &exports {
        out.push_str(&format!("    \"{name}\",\n"));
    }
    out.push_str("]\n");
    Ok(out)
}

/// Module docstring for the generated `options.py`.
const OPTIONS_MODULE_DOC: &str = "\
\"\"\"Typed options for operator methods.

Each `*Kwargs` is a `TypedDict` describing the keyword arguments accepted by
the corresponding `Operator` / `AsyncOperator` method, e.g. `ReadKwargs` for
`read`. Every key is optional; an unset key is equivalent to the method's
default.

Build an options dict, then unpack it into the call:

```python
import opendal

opts: opendal.options.ReadKwargs = {
    \"offset\": 1024,
    \"size\": 2048,
}
op.read(\"path/to/file\", **opts)
```

A static type checker validates the keys and their value types against the
method's keyword arguments.
\"\"\"
";

/// Render a class or field docstring as a `"""..."""` block in a class body,
/// one line per source line.
fn push_docstring(out: &mut String, doc: &str) {
    let lines: Vec<&str> = doc.lines().collect();
    if lines.is_empty() {
        return;
    }
    out.push_str("    \"\"\"");
    out.push_str(lines[0]);
    if lines.len() == 1 {
        out.push_str("\"\"\"\n");
    } else {
        for line in &lines[1..] {
            out.push('\n');
            if !line.is_empty() {
                out.push_str("    ");
                out.push_str(line);
            }
        }
        out.push_str("\n    \"\"\"\n");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generate::core_options::OptionField;
    use proc_macro2::TokenTree;
    use std::collections::BTreeMap;

    fn core_options() -> Vec<OptionsStruct> {
        let path = crate::workspace_dir().join("core/core/src/types/options.rs");
        core_options::parse(&path).expect("core options must parse")
    }

    fn core_capabilities() -> Vec<CapabilityField> {
        capability::parse(&crate::workspace_dir().join("core/core/src/types/capability.rs"))
            .expect("core capabilities must parse")
    }

    fn list_class(parsed: &[OptionsStruct]) -> PyClass {
        let mut classes = generated_classes(parsed, &core_capabilities()).unwrap();
        assert_eq!(classes.len(), 1);
        classes.remove(0)
    }

    /// The keyword-only parameters of every `#[pyo3(signature)]` method in
    /// `bindings/python/src/operator.rs`, keyed by type and method.
    fn operator_kwargs() -> BTreeMap<(String, String), Vec<String>> {
        let path = crate::workspace_dir().join("bindings/python/src/operator.rs");
        let file = syn::parse_file(&fs::read_to_string(path).unwrap()).unwrap();
        let mut methods = BTreeMap::new();
        for item in file.items {
            let syn::Item::Impl(impl_) = item else {
                continue;
            };
            let syn::Type::Path(ty) = &*impl_.self_ty else {
                continue;
            };
            let ty = ty.path.segments.last().unwrap().ident.to_string();
            for item in impl_.items {
                let syn::ImplItem::Fn(method) = item else {
                    continue;
                };
                for attr in &method.attrs {
                    let syn::Meta::List(list) = &attr.meta else {
                        continue;
                    };
                    let tokens: Vec<TokenTree> = list.tokens.clone().into_iter().collect();
                    let (Some(TokenTree::Ident(key)), Some(TokenTree::Group(params))) =
                        (tokens.first(), tokens.get(2))
                    else {
                        continue;
                    };
                    if !attr.path().is_ident("pyo3") || key != "signature" {
                        continue;
                    }
                    // Split `(path, *, limit=None, ...)` at its top-level
                    // commas and keep the names after the bare `*`.
                    let mut split: Vec<Vec<TokenTree>> = vec![Vec::new()];
                    for token in params.stream() {
                        match token {
                            TokenTree::Punct(p) if p.as_char() == ',' => split.push(Vec::new()),
                            token => split.last_mut().unwrap().push(token),
                        }
                    }
                    let mut keyword_only = false;
                    let mut kwargs = Vec::new();
                    for param in split {
                        match param.as_slice() {
                            [TokenTree::Punct(p)] if p.as_char() == '*' => keyword_only = true,
                            [TokenTree::Ident(name), ..] if keyword_only => {
                                kwargs.push(name.to_string())
                            }
                            _ => {}
                        }
                    }
                    methods.insert((ty.clone(), method.sig.ident.to_string()), kwargs);
                }
            }
        }
        methods
    }

    /// The `ListOptions` API on main (4ae8e6122), copied from
    /// `bindings/python/src/options.rs`, with the docstrings that core's doc
    /// comments give it.
    #[test]
    fn test_list_options_match_main() {
        let class = list_class(&core_options());
        let fields: Vec<String> = class
            .fields
            .iter()
            .map(|f| format!("{}: {}", f.name, f.rust_type))
            .collect();
        assert_eq!(
            fields,
            [
                "limit: Option<usize>",
                "start_after: Option<String>",
                "recursive: Option<bool>",
                "versions: Option<bool>",
                "deleted: Option<bool>",
            ]
        );

        // Copied from `core/core/src/types/options.rs`, with rustdoc links
        // as plain code spans.
        assert_eq!(
            class.doc,
            [
                "Options for list operations.",
                "",
                "# Groups",
                "- Traversal: `recursive`.",
                "- Pagination: `limit`, `start_after`.",
                "- Versioning: `versions`, `deleted`.",
                "",
                "The operation fails with `crate::ErrorKind::Unsupported` when an option",
                "requires a capability that the operator lacks.",
            ]
        );
        let docs: Vec<String> = class
            .fields
            .iter()
            .map(|f| format!("{}: {}", f.name, f.doc.join("\n")))
            .collect();
        assert_eq!(
            docs,
            [
                "limit: Maximum results per request (backend hint) to control memory and throttling.",
                "start_after: Start listing after this key, excluding the key itself.\n\
                 \n\
                 Setting this requires `Capability.list_with_start_after`.",
                "recursive: Whether to list recursively under the prefix; default `false`.\n\
                 \n\
                 Requires `Capability.list_with_recursive`.\n\
                 Services without native recursive listing rely on `crate::layers::SimulateLayer`\n\
                 (installed automatically by the default `auto-simulate` feature).",
                "versions: Include object versions; default `false`.\n\
                 \n\
                 Requires `Capability.list_with_versions`.",
                "deleted: Include delete markers; default `false`.\n\
                 \n\
                 Requires `Capability.list_with_deleted`.",
            ]
        );

        // An explicit `limit`, including `0`, passes through, and an omitted
        // flag is `false`. Every other core field, such as `glob`, keeps the
        // default that main's `..Default::default()` gave it.
        let (set, defaulted): (Vec<_>, Vec<_>) = class
            .conversion
            .iter()
            .map(|f| format!("{}: {}", f.name, f.expr))
            .partition(|f| f.contains("opts."));
        assert_eq!(
            set,
            [
                "limit: opts.limit",
                "start_after: opts.start_after",
                "recursive: opts.recursive.unwrap_or(false)",
                "versions: opts.versions.unwrap_or(false)",
                "deleted: opts.deleted.unwrap_or(false)",
            ]
        );
        assert!(defaulted.contains(&"glob: None".to_string()));
        for field in &defaulted {
            let value = field.split_once(": ").unwrap().1;
            assert!(
                ["None", "false", "0", "Default::default()"].contains(&value),
                "{field}"
            );
        }

        // `FromPyObject` reads each key and ignores any other key.
        let rs = render_options_rs(&[class]).unwrap();
        let extract = rs
            .split_once("impl<'a, 'py> FromPyObject<'a, 'py> for ListOptions {")
            .unwrap()
            .1
            .split("\n}\n")
            .next()
            .unwrap();
        let reads: Vec<&str> = extract
            .lines()
            .map(str::trim)
            .filter(|line| line.contains("extract_optional"))
            .collect();
        assert_eq!(
            reads,
            [
                "limit: extract_optional(&dict, \"limit\")?,",
                "start_after: extract_optional(&dict, \"start_after\")?,",
                "recursive: extract_optional(&dict, \"recursive\")?,",
                "versions: extract_optional(&dict, \"versions\")?,",
                "deleted: extract_optional(&dict, \"deleted\")?,",
            ]
        );
        assert!(!extract.contains("reject_unknown_kwargs"));
        assert!(rs.contains(
            "/// requires a capability that the operator lacks.\n\
             #[pyclass(module = \"opendal\")]\n\
             #[derive(Default, Debug)]\n\
             pub struct ListOptions {\n    \
             /// Maximum results per request (backend hint) to control memory and throttling.\n    \
             pub limit: Option<usize>,\n    \
             /// Start listing after this key, excluding the key itself.\n    \
             ///\n    \
             /// Setting this requires `Capability.list_with_start_after`.\n    \
             pub start_after: Option<String>,\n"
        ));
    }

    /// The `options.py` TypedDicts on main (4ae8e6122): the class order and
    /// the `ListKwargs` keys and types, with the docstrings that core's doc
    /// comments give them.
    #[test]
    fn test_options_py_matches_main() {
        let generated = generated_classes(&core_options(), &core_capabilities()).unwrap();
        let py = render_options_py(&crate::workspace_dir(), &generated).unwrap();
        let classes: Vec<&str> = py
            .lines()
            .filter_map(|line| line.strip_prefix("class "))
            .map(|class| class.split('(').next().unwrap())
            .collect();
        assert_eq!(
            classes,
            [
                "ReadKwargs",
                "WriteKwargs",
                "ListKwargs",
                "StatKwargs",
                "CopyKwargs",
                "RenameKwargs",
                "DeleteKwargs",
                "OpenKwargs",
            ]
        );

        let list = py
            .split_once("\n\nclass ListKwargs(TypedDict, total=False):\n")
            .unwrap()
            .1
            .split_once("\n\n\nclass StatKwargs(")
            .unwrap()
            .0;
        // A key is an `identifier: type` line; docstring lines are not.
        let keys: Vec<&str> = list
            .lines()
            .filter_map(|line| line.strip_prefix("    "))
            .filter(|line| {
                line.split_once(": ").is_some_and(|(key, _)| {
                    !key.is_empty() && key.chars().all(|c| c.is_ascii_lowercase() || c == '_')
                })
            })
            .collect();
        assert_eq!(
            keys,
            [
                "limit: int",
                "start_after: str",
                "recursive: bool",
                "versions: bool",
                "deleted: bool",
            ]
        );
        assert_eq!(
            list,
            r#"    """Options for list operations.

    # Groups
    - Traversal: `recursive`.
    - Pagination: `limit`, `start_after`.
    - Versioning: `versions`, `deleted`.

    The operation fails with `crate::ErrorKind::Unsupported` when an option
    requires a capability that the operator lacks.
    """

    limit: int
    """Maximum results per request (backend hint) to control memory and throttling."""
    start_after: str
    """Start listing after this key, excluding the key itself.

    Setting this requires `Capability.list_with_start_after`.
    """
    recursive: bool
    """Whether to list recursively under the prefix; default `false`.

    Requires `Capability.list_with_recursive`.
    Services without native recursive listing rely on `crate::layers::SimulateLayer`
    (installed automatically by the default `auto-simulate` feature).
    """
    versions: bool
    """Include object versions; default `false`.

    Requires `Capability.list_with_versions`.
    """
    deleted: bool
    """Include delete markers; default `false`.

    Requires `Capability.list_with_deleted`.
    """"#
        );
    }

    /// The `list` and `scan` keyword arguments on main (4ae8e6122), copied
    /// from `python/opendal/operator.pyi`.
    #[test]
    fn test_list_signatures_match_main() {
        let methods = operator_kwargs();
        for (ty, method, kwargs) in [
            (
                "Operator",
                "list",
                "limit start_after recursive versions deleted",
            ),
            ("Operator", "scan", "limit start_after versions deleted"),
            (
                "AsyncOperator",
                "list",
                "limit start_after recursive versions deleted",
            ),
            (
                "AsyncOperator",
                "scan",
                "limit start_after versions deleted",
            ),
        ] {
            let key = (ty.to_string(), method.to_string());
            assert_eq!(methods[&key].join(" "), kwargs, "{ty}.{method}");
        }
    }

    #[test]
    fn test_list_signatures_match_options_table() {
        let class = list_class(&core_options());
        let methods = operator_kwargs();
        // `scan` is `list` with `recursive=True`.
        for (ty, method, omitted) in [
            ("Operator", "list", None),
            ("Operator", "scan", Some("recursive")),
            ("AsyncOperator", "list", None),
            ("AsyncOperator", "scan", Some("recursive")),
        ] {
            let mut expected: Vec<&str> = class
                .fields
                .iter()
                .map(|f| f.name)
                .filter(|name| Some(*name) != omitted)
                .collect();
            let mut actual: Vec<&str> = methods[&(ty.to_string(), method.to_string())]
                .iter()
                .map(String::as_str)
                .collect();
            expected.sort();
            actual.sort();
            assert_eq!(
                actual, expected,
                "{ty}.{method} must take the ListOptions keyword arguments"
            );
        }
    }

    #[test]
    fn test_new_core_field_is_not_exposed() {
        let parsed = core_options();
        let base_rs =
            render_options_rs(&generated_classes(&parsed, &core_capabilities()).unwrap()).unwrap();
        let base_py = render_options_py(
            &crate::workspace_dir(),
            &generated_classes(&parsed, &core_capabilities()).unwrap(),
        )
        .unwrap();

        let mut mutated = parsed.clone();
        let list = mutated
            .iter_mut()
            .find(|o| o.name == "ListOptions")
            .unwrap();
        for (name, kind) in [
            ("fake_option", OptionKind::OptionString),
            ("fake_flag", OptionKind::Bool),
        ] {
            list.fields.push(OptionField {
                name: name.to_string(),
                kind,
                doc: Vec::new(),
            });
        }
        let classes = generated_classes(&mutated, &core_capabilities()).unwrap();
        let rs = render_options_rs(&classes).unwrap();
        let py = render_options_py(&crate::workspace_dir(), &classes).unwrap();

        assert_eq!(py, base_py);
        let added: Vec<&str> = rs.lines().filter(|l| l.contains("fake_")).collect();
        assert_eq!(
            added,
            [
                "            fake_option: None,",
                "            fake_flag: false,"
            ]
        );
        let kept: Vec<&str> = rs.lines().filter(|l| !l.contains("fake_")).collect();
        assert_eq!(kept, base_rs.lines().collect::<Vec<_>>());
    }

    #[test]
    fn test_list_options_reject_core_drift() {
        fn position(options: &OptionsStruct, name: &str) -> usize {
            options.fields.iter().position(|f| f.name == name).unwrap()
        }
        type Mutation = fn(&mut OptionsStruct);
        let cases: [(Mutation, &str); 7] = [
            (
                |list| {
                    list.fields.remove(position(list, "limit"));
                },
                "core ListOptions has no field `limit` for Python ListOptions.limit",
            ),
            (
                |list| {
                    let i = position(list, "limit");
                    list.fields[i].kind = OptionKind::OptionU64;
                },
                "core ListOptions::limit has kind OptionU64; \
                 Python ListOptions.limit requires OptionUsize",
            ),
            // A change to another kind that Python supports must fail too,
            // instead of silently turning `start_after` into a bool.
            (
                |list| {
                    let i = position(list, "start_after");
                    list.fields[i].kind = OptionKind::Bool;
                },
                "core ListOptions::start_after has kind Bool; \
                 Python ListOptions.start_after requires OptionString",
            ),
            (
                |list| {
                    let glob = list.fields[position(list, "glob")].clone();
                    list.fields.push(glob);
                },
                "core ListOptions contains duplicate field `glob`",
            ),
            (
                |list| {
                    let i = position(list, "limit");
                    list.fields[i].doc.clear();
                },
                "core ListOptions::limit has no doc comment",
            ),
            (
                |list| {
                    let i = position(list, "limit");
                    list.fields[i].doc = vec!["See [`Capability`] and \\d.".to_string()];
                },
                "core ListOptions::limit doc line `See `Capability` and \\d.` has a character \
                 docstrings must escape; teach dev/src/generate/python.rs about it",
            ),
            (
                |list| list.has_unmirrored_fields = true,
                "core ListOptions has fields the core options parser skips; Python cannot set them",
            ),
        ];

        let parsed = core_options();
        for (mutate, error) in cases {
            let mut mutated = parsed.clone();
            mutate(
                mutated
                    .iter_mut()
                    .find(|o| o.name == "ListOptions")
                    .unwrap(),
            );
            assert_eq!(
                generated_classes(&mutated, &core_capabilities())
                    .unwrap_err()
                    .to_string(),
                error
            );
        }
    }

    #[test]
    fn test_py_doc_strips_rustdoc_links() {
        let lines = [
            "Rely on [`crate::layers::SimulateLayer`](crate::layers::SimulateLayer) and [`Self::recursive`].",
            "",
            "A [plain](https://opendal.apache.org) link and `code` stay.",
        ]
        .map(String::from);
        assert_eq!(
            py_doc(&lines, "Test", &core_capabilities()).unwrap(),
            [
                "Rely on `crate::layers::SimulateLayer` and `Self::recursive`.",
                "",
                "A [plain](https://opendal.apache.org) link and `code` stay.",
            ]
        );
    }

    #[test]
    fn test_py_doc_uses_python_names() {
        let lines = [
            "- Pagination: `limit`, `start_after`.",
            "Requires [`Capability::list_with_limit`](crate::Capability::list_with_limit).",
            "Requires [`crate::Capability::list_with_versions`] and `Capability::list_with_deleted`.",
            "Unknown `Capability::no_such_flag`, `crate::ErrorKind::Unsupported`, and prose Capability::list stay.",
        ]
        .map(String::from);
        assert_eq!(
            py_doc(&lines, "Test", &core_capabilities()).unwrap(),
            [
                "- Pagination: `limit`, `start_after`.",
                "Requires `Capability.list_with_limit`.",
                "Requires `Capability.list_with_versions` and `Capability.list_with_deleted`.",
                "Unknown `Capability::no_such_flag`, `crate::ErrorKind::Unsupported`, and prose Capability::list stay.",
            ]
        );
    }

    /// Every capability the generated docstrings name is an attribute of the
    /// `Capability` class in `python/opendal/capability.pyi`.
    #[test]
    fn test_list_options_docs_name_python_capabilities() {
        let pyi = fs::read_to_string(
            crate::workspace_dir().join("bindings/python/python/opendal/capability.pyi"),
        )
        .unwrap();
        let attributes: HashSet<&str> = pyi
            .lines()
            .filter_map(|line| line.strip_prefix("    def ")?.split_once("(self, /)"))
            .map(|(name, _)| name)
            .collect();
        assert!(attributes.contains("list_with_start_after"));

        let class = list_class(&core_options());
        let docs: Vec<&String> = class
            .doc
            .iter()
            .chain(class.fields.iter().flat_map(|f| &f.doc))
            .collect();
        let mut named = Vec::new();
        for line in &docs {
            assert!(!line.contains("Capability::"), "{line}");
            for span in line.split('`').skip(1).step_by(2) {
                if let Some(name) = span.strip_prefix("Capability.") {
                    assert!(attributes.contains(name), "{span} is not in capability.pyi");
                    named.push(name);
                }
            }
        }
        assert_eq!(
            named,
            [
                "list_with_start_after",
                "list_with_recursive",
                "list_with_versions",
                "list_with_deleted",
            ]
        );
    }
}
