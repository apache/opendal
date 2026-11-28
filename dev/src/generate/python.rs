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
use std::collections::{BTreeMap, HashSet};
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

/// The Python names of the core items that generated docstrings reference.
struct PyNames {
    /// The core capabilities; the Python `Capability` has an attribute of
    /// the same name for each.
    capabilities: Vec<CapabilityField>,
    /// Each core `ErrorKind` variant and the `opendal.exceptions` class that
    /// `bindings/python/src/errors.rs` raises for it.
    exceptions: BTreeMap<String, String>,
}

impl PyNames {
    fn parse(workspace_dir: &Path) -> Result<Self> {
        let capabilities =
            capability::parse(&workspace_dir.join("core/core/src/types/capability.rs"))?;
        let errors = workspace_dir.join("bindings/python/src/errors.rs");
        // `format_pyerr_impl` maps each kind with a line such as
        // `ocore::ErrorKind::Unsupported => Unsupported::new_err(e),`.
        let exceptions: BTreeMap<String, String> = fs::read_to_string(&errors)?
            .lines()
            .filter_map(|line| {
                let (kind, class) = line
                    .trim()
                    .strip_prefix("ocore::ErrorKind::")?
                    .split_once(" => ")?;
                Some((
                    kind.to_string(),
                    class.strip_suffix("::new_err(e),")?.to_string(),
                ))
            })
            .collect();
        if !exceptions.contains_key("Unsupported") {
            bail!(
                "no ErrorKind to exception mapping parsed from {}",
                errors.display()
            );
        }
        Ok(Self {
            capabilities,
            exceptions,
        })
    }
}

/// The docstring of a generated class or field: the core doc comment, one
/// entry per line, adjusted for Python callers.
///
/// - Rustdoc links become plain code spans, because Python documentation
///   cannot resolve Rust paths: ``[`Capability::list_with_limit`](crate::Capability::list_with_limit)``
///   and ``[`crate::layers::SimulateLayer`]`` keep only their code span.
/// - Code spans that name a core capability or error kind use the Python
///   check or exception instead (see [`py_code_span`]); other code spans stay
///   as written.
/// - A sentence that names a Rust layer (a `crate::layers::` code span) is
///   dropped, because Python callers cannot install Rust layers or choose
///   Cargo features. The capability that the same doc names, such as
///   `op.capability().list_with_recursive`, is the check Python callers have.
///   The rest of that paragraph is joined into one line.
///
/// The text is not escaped here; the renderers escape it for their output.
fn py_doc(core_doc: &[String], item: &str, names: &PyNames) -> Result<Vec<String>> {
    if core_doc.iter().all(|line| line.is_empty()) {
        bail!("core {item} has no doc comment");
    }
    let lines: Vec<String> = core_doc
        .iter()
        .map(|line| py_doc_line(line, names))
        .collect();

    let mut out = Vec::with_capacity(lines.len());
    for paragraph in lines.split(|line| line.is_empty()) {
        let paragraph: Vec<String> = if paragraph
            .iter()
            .any(|line| line.contains("`crate::layers::"))
        {
            let text = paragraph.join(" ");
            let kept: Vec<&str> = sentences(&text)
                .into_iter()
                .filter(|sentence| !sentence.contains("`crate::layers::"))
                .collect();
            if kept.is_empty() {
                Vec::new()
            } else {
                vec![kept.join(" ")]
            }
        } else {
            paragraph.to_vec()
        };
        if paragraph.is_empty() {
            continue;
        }
        if !out.is_empty() {
            out.push(String::new());
        }
        out.extend(paragraph);
    }
    if out.is_empty() {
        bail!("core {item} has no doc comment that applies to Python");
    }
    Ok(out)
}

/// Split prose into sentences at a `. ` that an uppercase letter or a code
/// span follows, so `e.g. a` and `(see below).` stay inside their sentence.
fn sentences(text: &str) -> Vec<&str> {
    let mut sentences = Vec::new();
    let mut start = 0;
    for (at, _) in text.match_indices(". ") {
        let next = text[at + 2..].chars().next();
        if next.is_some_and(|c| c.is_ascii_uppercase() || c == '`') {
            sentences.push(&text[start..=at]);
            start = at + 2;
        }
    }
    sentences.push(&text[start..]);
    sentences
}

/// One doc line with rustdoc links unlinked and code spans renamed for
/// Python (see [`py_doc`]).
fn py_doc_line(line: &str, names: &PyNames) -> String {
    let mut unlinked = String::with_capacity(line.len());
    let mut rest = line;
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
        out.push_str(&py_code_span(code, names).unwrap_or_else(|| code.to_string()));
        out.push('`');
        rest = &rest[start + len + 2..];
    }
    out.push_str(rest);
    out
}

/// The Python form of a code span that names a core capability or error
/// kind, or `None` to keep the code span as written.
///
/// - `Capability::list_with_start_after` becomes
///   `op.capability().list_with_start_after`: the `Capability` that
///   `Operator.capability()` returns has an attribute of the same name for
///   every core capability, so callers can check it before setting an option.
/// - `ErrorKind::Unsupported` becomes `opendal.exceptions.Unsupported`, the
///   exception that `bindings/python/src/errors.rs` raises for that kind.
///
/// Keyword arguments keep their core names, so a code span that names an
/// options field, like `start_after`, needs no rewrite.
fn py_code_span(code: &str, names: &PyNames) -> Option<String> {
    let path = code.strip_prefix("crate::").unwrap_or(code);
    if let Some(name) = path.strip_prefix("Capability::") {
        return names
            .capabilities
            .iter()
            .any(|f| f.name == name)
            .then(|| format!("op.capability().{name}"));
    }
    let kind = path.strip_prefix("ErrorKind::")?;
    let class = names.exceptions.get(kind)?;
    Some(format!("opendal.exceptions.{class}"))
}

/// Build the render model of one generated class, checking the table against
/// the parsed core struct.
fn py_class(spec: &PyOptions, parsed: &[OptionsStruct], names: &PyNames) -> Result<PyClass> {
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
            doc: py_doc(&field.doc, &format!("{class}::{name}"), names)?,
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
        doc: py_doc(&core.doc, class, names)?,
        fields,
        conversion,
    })
}

fn generated_classes(parsed: &[OptionsStruct], names: &PyNames) -> Result<Vec<PyClass>> {
    GENERATED_OPTIONS
        .iter()
        .map(|spec| py_class(spec, parsed, names))
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
    let names = PyNames::parse(workspace_dir)?;
    let classes = generated_classes(&core_options::parse(&source)?, &names)?;
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
/// one line per source line. Python reads the block back as `doc`, whatever
/// quotes or backslashes it holds, and `just fmt` leaves it unchanged.
fn push_docstring(out: &mut String, doc: &str) {
    let doc = docstring_source(doc, !doc.contains('\n'));
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

/// The text between the `"""` quotes of a docstring that Python reads back as
/// `doc`. `closes_inline` says the closing quotes follow the text on the same
/// line.
///
/// A backslash becomes `\u005c`. A `"` becomes `\u0022` when it would join
/// the quotes around it: a leading `"`, every third `"` in a row, and the
/// quotes right before inline closing quotes. `just fmt` keeps these escapes:
/// ruff's D301 accepts `\u` escapes, but its fix for any other escape adds an
/// `r` prefix that changes the value, and `ruff format` puts a space after
/// opening quotes that a `"` follows, which D210 then rejects.
/// `scripts/postprocess_stubs.py` applies the same escapes to the stubs.
fn docstring_source(doc: &str, closes_inline: bool) -> String {
    const BACKSLASH: &str = r"\u005c";
    const QUOTE: &str = r"\u0022";
    let trailing = if closes_inline {
        doc.len() - doc.trim_end_matches('"').len()
    } else {
        0
    };
    let (body, quotes) = doc.split_at(doc.len() - trailing);
    let mut out = String::with_capacity(doc.len());
    let mut run = 0;
    for (at, c) in body.char_indices() {
        match c {
            '\\' => out.push_str(BACKSLASH),
            '"' if at == 0 || run == 2 => out.push_str(QUOTE),
            c => out.push(c),
        }
        run = match c {
            '"' if at > 0 && run < 2 => run + 1,
            _ => 0,
        };
    }
    for _ in quotes.chars() {
        out.push_str(QUOTE);
    }
    out
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

    fn py_names() -> PyNames {
        PyNames::parse(&crate::workspace_dir()).expect("Python names must parse")
    }

    fn list_class(parsed: &[OptionsStruct]) -> PyClass {
        let mut classes = generated_classes(parsed, &py_names()).unwrap();
        assert_eq!(classes.len(), 1);
        classes.remove(0)
    }

    /// The docstrings of every class and class field in `source`, keyed by
    /// `Class` and `Class.field`, as Python's `ast` parses them and
    /// `inspect.cleandoc` dedents them. Panics when `source` is not valid
    /// Python.
    fn python_docstrings(source: &str) -> BTreeMap<String, String> {
        use std::io::Write;
        use std::process::Stdio;

        const SCRIPT: &str = r#"
import ast, inspect, json, sys
docs = {}
for cls in ast.parse(sys.stdin.read()).body:
    if not isinstance(cls, ast.ClassDef):
        continue
    field = None
    for i, node in enumerate(cls.body):
        if isinstance(node, ast.AnnAssign):
            field = node.target.id
        elif isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant):
            key = cls.name if i == 0 else f"{cls.name}.{field}"
            docs[key] = inspect.cleandoc(node.value.value)
print(json.dumps(docs))
"#;
        let mut child = Command::new("python3")
            .args(["-c", SCRIPT])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("python3 must be installed to check generated Python");
        child
            .stdin
            .take()
            .unwrap()
            .write_all(source.as_bytes())
            .unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "generated Python does not parse:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout).unwrap()
    }

    /// The code spans in `doc` that start with `prefix`, without it.
    fn spans<'a>(doc: &'a [String], prefix: &str) -> Vec<&'a str> {
        doc.iter()
            .flat_map(|line| line.split('`').skip(1).step_by(2))
            .filter_map(|span| span.strip_prefix(prefix))
            .collect()
    }

    /// The `ListOptions` surface on main (4ae8e6122), copied from
    /// `bindings/python/src/options.rs`, and the Python checks its docs
    /// name, copied from the capability each field requires in
    /// `core/core/src/types/options.rs`. The doc wording itself is core's,
    /// so a core reword passes.
    fn assert_list_surface(class: &PyClass) {
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

        assert_eq!(
            spans(&class.doc, "opendal.exceptions."),
            ["Unsupported"],
            "{:?}",
            class.doc
        );
        let checks: Vec<String> = class
            .fields
            .iter()
            .map(|f| {
                format!(
                    "{}: {}",
                    f.name,
                    spans(&f.doc, "op.capability().").join(" ")
                )
            })
            .collect();
        assert_eq!(
            checks,
            [
                "limit: ",
                "start_after: list_with_start_after",
                "recursive: list_with_recursive",
                "versions: list_with_versions",
                "deleted: list_with_deleted",
            ]
        );

        // No Rust-only names reach Python callers.
        for line in class
            .doc
            .iter()
            .chain(class.fields.iter().flat_map(|f| &f.doc))
        {
            for rust in ["crate::", "Capability::", "ErrorKind::", "auto-simulate"] {
                assert!(!line.contains(rust), "{line}");
            }
        }
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
    /// `bindings/python/src/options.rs`, with docstrings taken from core.
    #[test]
    fn test_list_options_match_main() {
        let parsed = core_options();
        let class = list_class(&parsed);
        assert_list_surface(&class);

        // Each docstring is its own core doc comment.
        let core = parsed.iter().find(|o| o.name == "ListOptions").unwrap();
        let names = py_names();
        assert_eq!(class.doc, py_doc(&core.doc, "ListOptions", &names).unwrap());
        for field in &class.fields {
            let core = core.fields.iter().find(|f| f.name == field.name).unwrap();
            assert_eq!(field.doc, py_doc(&core.doc, field.name, &names).unwrap());
        }

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

        // The pyclass and its fields, in order, each after its docs.
        let body = rs
            .split_once("#[pyclass(module = \"opendal\")]\n#[derive(Default, Debug)]\npub struct ListOptions {\n")
            .unwrap()
            .1
            .split_once("\n}\n")
            .unwrap()
            .0;
        let members: Vec<&str> = body
            .lines()
            .map(str::trim)
            .filter(|line| !line.starts_with("///"))
            .collect();
        assert_eq!(
            members,
            [
                "pub limit: Option<usize>,",
                "pub start_after: Option<String>,",
                "pub recursive: Option<bool>,",
                "pub versions: Option<bool>,",
                "pub deleted: Option<bool>,",
            ]
        );
        assert!(body.starts_with("    /// "), "{body}");
    }

    /// The `options.py` TypedDicts on main (4ae8e6122): the class order and
    /// the `ListKwargs` keys and types, with docstrings taken from core.
    #[test]
    fn test_options_py_matches_main() {
        let generated = generated_classes(&core_options(), &py_names()).unwrap();
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

        // The docstrings parse back, as Python reads them, to the docs.
        let docs = python_docstrings(&py);
        let class = &generated[0];
        assert_eq!(docs["ListKwargs"], class.doc.join("\n"));
        for field in &class.fields {
            assert_eq!(
                docs[&format!("ListKwargs.{}", field.name)],
                field.doc.join("\n")
            );
        }
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
        let base_rs = render_options_rs(&generated_classes(&parsed, &py_names()).unwrap()).unwrap();
        let base_py = render_options_py(
            &crate::workspace_dir(),
            &generated_classes(&parsed, &py_names()).unwrap(),
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
        let classes = generated_classes(&mutated, &py_names()).unwrap();
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
                    list.fields[i].doc =
                        vec!["Install [`crate::layers::SimulateLayer`] to page.".to_string()];
                },
                "core ListOptions::limit has no doc comment that applies to Python",
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
                generated_classes(&mutated, &py_names())
                    .unwrap_err()
                    .to_string(),
                error
            );
        }
    }

    #[test]
    fn test_py_doc_strips_rustdoc_links() {
        let lines = [
            "Use [`crate::Operator::list_with`](crate::Operator::list_with) and [`Self::recursive`].",
            "",
            "A [plain](https://opendal.apache.org) link and `code` stay.",
        ]
        .map(String::from);
        assert_eq!(
            py_doc(&lines, "Test", &py_names()).unwrap(),
            [
                "Use `crate::Operator::list_with` and `Self::recursive`.",
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
            "Fails with [`crate::ErrorKind::Unsupported`] or `ErrorKind::ConditionNotMatch`.",
            "Unknown `Capability::no_such_flag`, `ErrorKind::NoSuchKind`, and prose Capability::list stay.",
        ]
        .map(String::from);
        assert_eq!(
            py_doc(&lines, "Test", &py_names()).unwrap(),
            [
                "- Pagination: `limit`, `start_after`.",
                "Requires `op.capability().list_with_limit`.",
                "Requires `op.capability().list_with_versions` and `op.capability().list_with_deleted`.",
                "Fails with `opendal.exceptions.Unsupported` or `opendal.exceptions.ConditionNotMatch`.",
                "Unknown `Capability::no_such_flag`, `ErrorKind::NoSuchKind`, and prose Capability::list stay.",
            ]
        );
    }

    /// Python callers cannot install Rust layers, so a sentence that names
    /// one is dropped; the rest of its paragraph stays.
    #[test]
    fn test_py_doc_drops_rust_layer_sentences() {
        let lines = [
            "Whether to list recursively; default `false`.",
            "",
            "Requires [`Capability::list_with_recursive`](crate::Capability::list_with_recursive).",
            "Services without native recursive listing rely on [`crate::layers::SimulateLayer`]",
            "(installed automatically by the default `auto-simulate` feature). Use it e.g. for scans.",
            "",
            "Or install [`crate::layers::SimulateLayer`] yourself.",
            "",
            "- A list item.",
            "- Another item.",
        ]
        .map(String::from);
        assert_eq!(
            py_doc(&lines, "Test", &py_names()).unwrap(),
            [
                "Whether to list recursively; default `false`.",
                "",
                "Requires `op.capability().list_with_recursive`. Use it e.g. for scans.",
                "",
                "- A list item.",
                "- Another item.",
            ]
        );
    }

    /// Core docs may hold any text: quotes, backslashes, and rewording reach
    /// valid Python with their value intact, and the surface checks, which
    /// pin no core wording, still pass.
    #[test]
    fn test_list_options_survive_any_core_doc() {
        let mut parsed = core_options();
        // The capability each field names, whatever markup core uses for it.
        let capabilities: BTreeMap<&str, String> = list_class(&parsed)
            .fields
            .iter()
            .map(|f| (f.name, spans(&f.doc, "op.capability().").join(" ")))
            .collect();
        let list = parsed.iter_mut().find(|o| o.name == "ListOptions").unwrap();
        list.doc = [
            r#""Options" for list operations, with \d, \\ and """ inside."#,
            "",
            "Fails with [`crate::ErrorKind::Unsupported`] when an option",
            r#"needs a missing capability, a "quoted" ending""#,
        ]
        .map(String::from)
        .to_vec();
        for field in &mut list.fields {
            let capability = match capabilities.get(field.name.as_str()) {
                Some(name) if !name.is_empty() => {
                    format!(" Needs [`Capability::{name}`](crate::Capability::{name}).")
                }
                _ => String::new(),
            };
            field.doc = vec![format!(r#"Reworded "{}" doc.{capability}"#, field.name)];
        }
        fn field<'a>(list: &'a mut OptionsStruct, name: &str) -> &'a mut Vec<String> {
            &mut list.fields.iter_mut().find(|f| f.name == name).unwrap().doc
        }
        *field(list, "limit") = vec![r#"Maximum results "per request""#.to_string()];
        field(list, "start_after")[0].push_str(r" Keys match \d+.");
        *field(list, "deleted") = vec![
            r#"Ends with """"#.to_string(),
            String::new(),
            r#"Needs [`Capability::list_with_deleted`](crate::Capability::list_with_deleted) and \ "x""#
                .to_string(),
        ];

        let generated = generated_classes(&parsed, &py_names()).unwrap();
        let class = &generated[0];
        assert_list_surface(class);
        assert_eq!(
            class.fields[1].doc,
            [
                r#"Reworded "start_after" doc. Needs `op.capability().list_with_start_after`. Keys match \d+."#
            ]
        );

        let py = render_options_py(&crate::workspace_dir(), &generated).unwrap();
        // `bindings/python/tests/test_stub_docstrings.py` checks that
        // `just fmt` keeps the first of these lines.
        for line in [
            r#"    """\u0022Options" for list operations, with \u005cd, \u005c\u005c and ""\u0022 inside."#,
            r#"    needs a missing capability, a "quoted" ending""#,
            r#"    """Maximum results "per request\u0022""""#,
            r#"    """Reworded "start_after" doc. Needs `op.capability().list_with_start_after`. Keys match \u005cd+.""""#,
        ] {
            assert!(py.lines().any(|l| l == line), "{line}\n{py}");
        }
        // Ruff's D301 fix rewrites any other escape, and `ruff format`
        // separates a `"` from the opening quotes.
        for (at, _) in py.match_indices('\\') {
            assert!(py[at + 1..].starts_with('u'), "{}", &py[at..]);
        }
        assert!(!py.contains("\"\"\"\""), "{py}");

        let docs = python_docstrings(&py);
        assert_eq!(
            docs["ListKwargs"],
            "\"Options\" for list operations, with \\d, \\\\ and \"\"\" inside.\n\
             \n\
             Fails with `opendal.exceptions.Unsupported` when an option\n\
             needs a missing capability, a \"quoted\" ending\""
        );
        assert_eq!(docs["ListKwargs.limit"], r#"Maximum results "per request""#);
        for field in &class.fields {
            assert_eq!(
                docs[&format!("ListKwargs.{}", field.name)],
                field.doc.join("\n")
            );
        }

        // The pyclass doc, which pyo3-introspection writes into
        // `_opendal.pyi`, is core's text unescaped.
        let rs = render_options_rs(&generated).unwrap();
        for line in [
            r#"/// "Options" for list operations, with \d, \\ and """ inside."#,
            "    /// Maximum results \"per request\"\n    pub limit: Option<usize>,",
        ] {
            assert!(rs.contains(line), "{line}\n{rs}");
        }
    }

    /// Every capability and exception the generated docstrings name exists
    /// in the Python API: `python/opendal/capability.pyi` and
    /// `python/opendal/exceptions.pyi`.
    #[test]
    fn test_list_options_docs_name_python_api() {
        let stub = |name: &str| {
            fs::read_to_string(
                crate::workspace_dir()
                    .join("bindings/python/python/opendal")
                    .join(name),
            )
            .unwrap()
        };
        let capability = stub("capability.pyi");
        let attributes: HashSet<&str> = capability
            .lines()
            .filter_map(|line| line.strip_prefix("    def ")?.split_once("(self, /)"))
            .map(|(name, _)| name)
            .collect();
        assert!(attributes.contains("list_with_start_after"));
        let exceptions = stub("exceptions.pyi");
        let classes: HashSet<&str> = exceptions
            .lines()
            .filter_map(|line| line.strip_prefix("class ")?.split_once('('))
            .map(|(name, _)| name)
            .collect();
        assert!(classes.contains("NotFound"));

        let class = list_class(&core_options());
        let docs: Vec<String> = class
            .doc
            .iter()
            .chain(class.fields.iter().flat_map(|f| &f.doc))
            .cloned()
            .collect();
        let checks = spans(&docs, "op.capability().");
        for name in &checks {
            assert!(attributes.contains(name), "{name} is not in capability.pyi");
        }
        assert_eq!(
            checks,
            [
                "list_with_start_after",
                "list_with_recursive",
                "list_with_versions",
                "list_with_deleted",
            ]
        );
        let raised = spans(&docs, "opendal.exceptions.");
        for name in &raised {
            assert!(classes.contains(name), "{name} is not in exceptions.pyi");
        }
        assert_eq!(raised, ["Unsupported"]);

        // Every exception that the generator may name exists.
        for class in py_names().exceptions.values() {
            assert!(
                classes.contains(class.as_str()),
                "{class} is not in exceptions.pyi"
            );
        }
    }
}
