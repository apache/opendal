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

use crate::generate::parser::{Config, ConfigType, Service, Services, sorted_services};
use anyhow::Result;
use minijinja::value::ViaDeserialize;
use minijinja::{Environment, context};
use std::fs;
use std::path::PathBuf;

/// Path-like fields, widened to accept `os.PathLike` in addition to `str`.
fn is_path_like_field(name: &str) -> bool {
    matches!(name, "root" | "atomic_write_dir")
}

/// Whether a field is keyword-optional in Python (optional core fields and bools).
fn field_is_optional(field: &Config) -> bool {
    field.optional || field.value == ConfigType::Bool
}

/// Order fields so required (no-default) params precede optional ones, as pyo3
/// signatures require. Order within each group stays alphabetical.
fn ordered_for_signature(services: Services) -> Services {
    services
        .into_iter()
        .map(|(k, srv)| {
            let mut config = srv.config;
            config.sort_by(|a, b| {
                field_is_optional(a)
                    .cmp(&field_is_optional(b))
                    .then_with(|| a.name.cmp(&b.name))
            });
            (k, Service { config })
        })
        .collect()
}

fn enabled_service(srv: &str) -> bool {
    // Services keyed by their directory name (kebab-case). Excludes services not
    // built by `bindings/python/Cargo.toml`'s `services-all` feature.
    !matches!(
        srv,
        "etcd"
            | "foundationdb"
            | "hdfs"
            | "hdfs-native"
            | "rocksdb"
            | "tikv"
            | "github"
            | "cloudflare-kv"
            | "monoiofs"
            | "dbfs"
            | "surrealdb"
            | "d1"
            | "opfs"
            | "compfs"
            | "lakefs"
            | "pcloud"
            | "vercel-blob"
            | "sftp"
            | "foyer"
    )
}

pub fn generate(workspace_dir: PathBuf, services: Services) -> Result<()> {
    let srvs = ordered_for_signature(sorted_services(services, enabled_service));
    let mut env = Environment::new();
    env.add_template("python", include_str!("python.j2"))?;
    env.add_function("snake_to_kebab_case", snake_to_kebab_case);
    env.add_function("service_to_feature", service_to_feature);
    env.add_function("service_to_pascal", service_to_pascal);
    env.add_function("make_python_type", make_python_type);
    env.add_function("make_pydoc_param_header", make_pydoc_param_header);
    env.add_function("make_pydoc_param", make_pydoc_param);
    env.add_function("config_param_type", config_param_type);
    env.add_function("config_new_param", config_new_param);
    env.add_function("config_to_opts", config_to_opts);
    env.add_function("config_assign_map", config_assign_map);
    env.add_function("config_getter", config_getter);
    env.add_function("config_to_map", config_to_map);
    env.add_function("config_picklable", config_picklable);
    env.add_function("config_class_doc", config_class_doc);
    env.add_function("config_field_doc", config_field_doc);
    let tmpl = env.get_template("python")?;

    let output = workspace_dir.join("bindings/python/src/services.rs");
    let mut rendered = tmpl.render(context! { srvs => srvs })?;
    if !rendered.ends_with('\n') {
        rendered.push('\n');
    }
    fs::write(output, rendered)?;

    Ok(())
}

fn snake_to_kebab_case(str: &str) -> String {
    str.replace('_', "-")
}

fn service_to_feature(service: &str) -> String {
    format!("services-{}", snake_to_kebab_case(service))
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

/// Classifies a config field to drive codegen.
#[derive(Clone, Copy, PartialEq)]
enum FieldKind {
    Str,
    StrRequired,
    Path,
    Bool,
    Num,
    Vec,
    Map,
    Duration,
    /// A core type the parser can't map (e.g. an enum); passed through as a string.
    Other,
}

fn field_kind(field: &Config) -> FieldKind {
    match field.value {
        ConfigType::Bool => FieldKind::Bool,
        ConfigType::Usize | ConfigType::U64 | ConfigType::I64 | ConfigType::U32 | ConfigType::U16 => {
            FieldKind::Num
        }
        ConfigType::Vec => FieldKind::Vec,
        ConfigType::HashMap => FieldKind::Map,
        ConfigType::Duration => FieldKind::Duration,
        ConfigType::String => {
            if is_opaque_string_fallback(&field.name) {
                FieldKind::Other
            } else if is_path_like_field(&field.name) {
                FieldKind::Path
            } else if field.optional {
                FieldKind::Str
            } else {
                FieldKind::StrRequired
            }
        }
    }
}

/// Config fields whose core type is a string-parsed enum, not a `String`.
fn is_opaque_string_fallback(field: &str) -> bool {
    matches!(field, "repo_type" | "download_mode")
}

fn int_type(ty: ConfigType) -> &'static str {
    match ty {
        ConfigType::Usize => "usize",
        ConfigType::U64 => "u64",
        ConfigType::I64 => "i64",
        ConfigType::U32 => "u32",
        ConfigType::U16 => "u16",
        _ => "usize",
    }
}

/// The Rust `#[new]` parameter type, chosen so introspection renders the exact
/// Python type.
fn config_param_type(field: ViaDeserialize<Config>) -> Result<String, minijinja::Error> {
    Ok(match field_kind(&field) {
        FieldKind::Str | FieldKind::Duration | FieldKind::Other => "Option<String>".to_string(),
        FieldKind::StrRequired => "String".to_string(),
        FieldKind::Path => "Option<crate::PyPath>".to_string(),
        FieldKind::Bool => "Option<bool>".to_string(),
        FieldKind::Num => format!("Option<{}>", int_type(field.value)),
        FieldKind::Vec => "Option<Vec<String>>".to_string(),
        FieldKind::Map => "Option<std::collections::HashMap<String, String>>".to_string(),
    })
}

/// The `#[new]` signature parameter, defaulting optional fields to `None`.
fn config_new_param(field: ViaDeserialize<Config>) -> Result<String, minijinja::Error> {
    if field_is_optional(&field) {
        Ok(format!("{} = None", field.name))
    } else {
        Ok(field.name.to_string())
    }
}

/// Insert a `#[new]` parameter into the option map handed to `from_iter`.
fn config_to_opts(field: ViaDeserialize<Config>) -> Result<String, minijinja::Error> {
    let name = &field.name;
    let key = &field.name;
    Ok(match field_kind(&field) {
        FieldKind::StrRequired => format!("opts.insert(\"{key}\".to_string(), {name});"),
        FieldKind::Str | FieldKind::Duration | FieldKind::Other => {
            format!("if let Some(v) = {name} {{ opts.insert(\"{key}\".to_string(), v); }}")
        }
        FieldKind::Path => format!(
            "if let Some(v) = {name} {{ opts.insert(\"{key}\".to_string(), v.into_string()); }}"
        ),
        FieldKind::Bool => format!(
            "if let Some(v) = {name} {{ opts.insert(\"{key}\".to_string(), if v {{ \"true\" }} else {{ \"false\" }}.to_string()); }}"
        ),
        FieldKind::Num => format!(
            "if let Some(v) = {name} {{ opts.insert(\"{key}\".to_string(), v.to_string()); }}"
        ),
        FieldKind::Vec => format!(
            "if let Some(v) = {name} {{ opts.insert(\"{key}\".to_string(), v.join(\",\")); }}"
        ),
        // Map fields are set on the config directly after `from_iter`.
        FieldKind::Map => String::new(),
    })
}

/// Set a map-valued field on the core config after `from_iter` (map values
/// cannot pass through the flat option map). No-op for other kinds.
fn config_assign_map(field: ViaDeserialize<Config>) -> Result<String, minijinja::Error> {
    let name = &field.name;
    Ok(match field_kind(&field) {
        FieldKind::Map => format!("if let Some(v) = {name} {{ cfg.{name} = Some(v); }}"),
        _ => String::new(),
    })
}

/// A getter (with doc comment) for a config field. Opaque fields have none.
fn config_getter(field: ViaDeserialize<Config>) -> Result<String, minijinja::Error> {
    let name = &field.name;
    let kind = field_kind(&field);

    if kind == FieldKind::Other {
        return Ok(String::new());
    }

    let body = match kind {
        FieldKind::StrRequired => {
            format!("#[getter]\n    fn {name}(&self) -> String {{ self.0.{name}.clone() }}")
        }
        FieldKind::Str => {
            format!("#[getter]\n    fn {name}(&self) -> Option<String> {{ self.0.{name}.clone() }}")
        }
        // `PyPath`/`Option<bool>` keep the getter type equal to the constructor.
        FieldKind::Path => format!(
            "#[getter]\n    fn {name}(&self) -> Option<crate::PyPath> {{ self.0.{name}.clone().map(crate::PyPath::from) }}"
        ),
        FieldKind::Bool => {
            format!("#[getter]\n    fn {name}(&self) -> Option<bool> {{ Some(self.0.{name}) }}")
        }
        FieldKind::Num => {
            let ty = int_type(field.value);
            if field.optional {
                format!("#[getter]\n    fn {name}(&self) -> Option<{ty}> {{ self.0.{name} }}")
            } else {
                format!("#[getter]\n    fn {name}(&self) -> {ty} {{ self.0.{name} }}")
            }
        }
        FieldKind::Vec => {
            format!(
                "#[getter]\n    fn {name}(&self) -> Option<Vec<String>> {{ self.0.{name}.clone() }}"
            )
        }
        FieldKind::Map => format!(
            "#[getter]\n    fn {name}(&self) -> Option<std::collections::HashMap<String, String>> {{ self.0.{name}.clone() }}"
        ),
        FieldKind::Duration => format!(
            "#[getter]\n    fn {name}(&self) -> Option<String> {{ self.0.{name}.map(|d| format!(\"{{}}s\", d.as_secs())) }}"
        ),
        FieldKind::Other => unreachable!(),
    };

    let doc = config_field_doc(field)?;
    if doc.is_empty() {
        Ok(body)
    } else {
        Ok(format!("{doc}\n    {body}"))
    }
}

/// Insert a field into the flat option map used for `repr`/pickle.
fn config_to_map(field: ViaDeserialize<Config>) -> Result<String, minijinja::Error> {
    let name = &field.name;
    let key = &field.name;
    Ok(match field_kind(&field) {
        FieldKind::StrRequired => {
            format!("map.insert(\"{key}\".to_string(), self.0.{name}.clone());")
        }
        FieldKind::Str | FieldKind::Path => format!(
            "if let Some(v) = &self.0.{name} {{ map.insert(\"{key}\".to_string(), v.clone()); }}"
        ),
        FieldKind::Bool => format!(
            "map.insert(\"{key}\".to_string(), if self.0.{name} {{ \"true\" }} else {{ \"false\" }}.to_string());"
        ),
        FieldKind::Num => {
            if field.optional {
                format!(
                    "if let Some(v) = &self.0.{name} {{ map.insert(\"{key}\".to_string(), v.to_string()); }}"
                )
            } else {
                format!("map.insert(\"{key}\".to_string(), self.0.{name}.to_string());")
            }
        }
        FieldKind::Vec => format!(
            "if let Some(v) = &self.0.{name} {{ map.insert(\"{key}\".to_string(), v.join(\",\")); }}"
        ),
        FieldKind::Duration => format!(
            "if let Some(d) = &self.0.{name} {{ map.insert(\"{key}\".to_string(), format!(\"{{}}s\", d.as_secs())); }}"
        ),
        // Map/opaque fields are omitted from the flat option map.
        FieldKind::Map | FieldKind::Other => String::new(),
    })
}

/// Flip `ok` to false when a map-valued field is set (breaks flat-map pickling).
fn config_picklable(field: ViaDeserialize<Config>) -> Result<String, minijinja::Error> {
    let name = &field.name;
    Ok(match field_kind(&field) {
        FieldKind::Map => format!("if self.0.{name}.is_some() {{ ok = false; }}"),
        _ => String::new(),
    })
}

/// Render text as Rust `///` doc-comment lines (lifted into `.pyi` docstrings).
fn to_doc_lines(text: &str, indent: usize) -> String {
    let pad = " ".repeat(indent);
    let normalized: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        return String::new();
    }
    format_text(&normalized, 0, 76)
        .lines()
        .map(|line| {
            let line = line.trim_end();
            if line.is_empty() {
                format!("{pad}///")
            } else {
                format!("{pad}/// {line}")
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The class-level doc comment for a service config.
fn config_class_doc(service: &str) -> Result<String, minijinja::Error> {
    Ok(to_doc_lines(
        &format!(
            "Configuration for the `{}` service.",
            snake_to_kebab_case(service)
        ),
        0,
    ))
}

/// The field doc comment, including any deprecation note.
fn config_field_doc(field: ViaDeserialize<Config>) -> Result<String, minijinja::Error> {
    let mut text = field.comments.trim().to_string();

    match field_kind(&field) {
        FieldKind::Duration => {
            if !text.is_empty() {
                text.push(' ');
            }
            text.push_str("Accepts a humantime duration string (e.g. \"5s\").");
        }
        FieldKind::Vec => {
            if !text.is_empty() {
                text.push(' ');
            }
            text.push_str("A list of strings, serialized as a \",\" separated value.");
        }
        _ => {}
    }

    if let Some(deprecated) = &field.deprecated {
        if !text.is_empty() {
            text.push(' ');
        }
        text.push_str(&format!(
            "[Deprecated since {}] {}",
            deprecated.since,
            deprecated.note.trim()
        ));
    }

    Ok(to_doc_lines(&text, 4))
}


