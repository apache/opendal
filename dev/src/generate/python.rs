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

use crate::generate::options;
use crate::generate::parser::{Config, ConfigType, Services, service_feature, sorted_services};
use anyhow::{Context, Result, bail};
use minijinja::value::ViaDeserialize;
use minijinja::{Environment, context};
use serde::Deserialize;
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
    let mut rendered = tmpl.render(context! { srvs => srvs })?;
    if !rendered.ends_with('\n') {
        rendered.push('\n');
    }
    fs::write(output, rendered)?;

    // Generate the service, context, and layer config TypedDicts (Python).
    let tmpl = env.get_template("python_config")?;
    let output = workspace_dir.join("bindings/python/python/opendal/config.py");
    let mut rendered = tmpl.render(context! { srvs => srvs, layers => layers })?;
    if !rendered.ends_with('\n') {
        rendered.push('\n');
    }
    fs::write(output, rendered)?;

    // Generate the typed operator options TypedDicts (Python).
    generate_options_py(&workspace_dir)?;

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
    ty: ViaDeserialize<ConfigType>,
    optional: bool,
) -> Result<String, minijinja::Error> {
    let py_type = make_python_type(ty)?;
    let optional_str = if optional { ", optional" } else { "" };
    let first_line = format!("{} : {}{}", name, py_type, optional_str);

    Ok(first_line)
}

/// Generate a properly indented NumPy-style docstring line for a parameter
pub fn make_pydoc_param(
    comments: &str,
    ty: ViaDeserialize<ConfigType>,
) -> Result<String, minijinja::Error> {
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
fn make_config_field_type(field: ViaDeserialize<Config>) -> Result<String, minijinja::Error> {
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
fn config_field_is_required(field: ViaDeserialize<Config>) -> Result<bool, minijinja::Error> {
    Ok(!field.optional && field.value != ConfigType::Bool)
}

/// Render the field docstring body, ready to embed in a `"""..."""` docstring.
///
/// Returns an empty string when there is nothing to document (the template then
/// omits the docstring). Backslashes and `"""` runs are escaped, and a trailing
/// `"` is escaped so it cannot merge with the closing delimiter.
fn make_config_field_doc(field: ViaDeserialize<Config>) -> Result<String, minijinja::Error> {
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

fn make_python_type(ty: ViaDeserialize<ConfigType>) -> Result<String, minijinja::Error> {
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

/// Python type of an operator option field, parsed from
/// `bindings/python/src/options.rs`.
fn make_option_type(ty: options::OptionType) -> &'static str {
    match ty {
        options::OptionType::Str => "str",
        options::OptionType::Int => "int",
        options::OptionType::Bool => "bool",
        options::OptionType::DateTime => "datetime",
        options::OptionType::Dict => "dict[str, str]",
    }
}

/// Generate `bindings/python/python/opendal/options.py`, the typed keyword
/// arguments accepted by the operator methods.
///
/// The option fields are parsed from the `#[pyclass]` structs in
/// `bindings/python/src/options.rs`, so the generated file stays in sync with
/// the Rust option types.
fn generate_options_py(workspace_dir: &Path) -> Result<()> {
    let source = workspace_dir.join("bindings/python/src/options.rs");
    let options = options::parse(&source)?;
    if options.is_empty() {
        anyhow::bail!("no option structs parsed from {}", source.display());
    }

    let mut out = String::new();
    out.push_str(PYTHON_LICENSE_HEADER);
    out.push_str("\n# DO NOT EDIT IT MANUALLY. This file is generated by opendal/dev/generate/python.rs.\n\n");
    out.push_str(OPTIONS_MODULE_DOC);
    out.push_str(
        "\nfrom __future__ import annotations\n\nfrom datetime import datetime\nfrom typing import TypedDict\n",
    );

    let mut kwargs_names = Vec::new();
    for opts in &options {
        // `ReadOptions` maps to the `ReadKwargs` TypedDict.
        let kwargs = format!(
            "{}Kwargs",
            opts.name.strip_suffix("Options").unwrap_or(&opts.name)
        );
        kwargs_names.push(kwargs.clone());
        let doc = if opts.doc.is_empty() {
            "Keyword arguments for the corresponding `Operator` / `AsyncOperator` method."
                .to_string()
        } else {
            opts.doc.clone()
        };
        out.push_str(&format!(
            "\n\nclass {kwargs}(TypedDict, total=False):\n    \"\"\"{doc}\"\"\"\n\n"
        ));
        for field in &opts.fields {
            out.push_str(&format!(
                "    {}: {}\n",
                field.name,
                make_option_type(field.ty)
            ));
            push_field_doc(&mut out, &field.doc);
        }
    }

    // `open` accepts the union of the read and write options, so its kwargs
    // TypedDict inherits both.
    kwargs_names.push("OpenKwargs".to_string());
    out.push_str(
        "\n\nclass OpenKwargs(ReadKwargs, WriteKwargs, total=False):\n    \"\"\"Keyword arguments for `Operator.open` and `AsyncOperator.open`.\n\n    Accepted keys depend on `mode`: read modes (`\"r\"`, `\"rb\"`, `\"br\"`) accept\n    reader options only, while write, append, and create modes (`\"w\"`, `\"wb\"`,\n    `\"ab\"`, `\"xb\"`, etc.) accept writer options only.\n\n    The `content_type`, `cache_control`, and `content_disposition` options\n    apply only when writing, appending, or creating and raise `TypeError` in\n    read mode. Use the operator's `read()` method to supply response header\n    overrides.\n    \"\"\"\n",
    );

    out.push_str("\n\n__all__ = [\n");
    for name in &kwargs_names {
        out.push_str(&format!("    \"{name}\",\n"));
    }
    out.push_str("]\n");

    let output = workspace_dir.join("bindings/python/python/opendal/options.py");
    fs::write(output, out)?;
    Ok(())
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

/// Render a field docstring as a `"""..."""` block, one line per source line.
fn push_field_doc(out: &mut String, doc: &str) {
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
