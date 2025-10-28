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

use crate::generate::parser::{ConfigType, Services, sorted_services};
use anyhow::Result;
use minijinja::value::ViaDeserialize;
use minijinja::{Environment, context};
use std::fs;
use std::path::PathBuf;

fn enabled_service(srv: &str) -> bool {
    match srv {
        // not enabled in bindings/python/Cargo.toml
        "etcd" | "foundationdb" | "hdfs" | "rocksdb" | "tikv"  => false,
        _ => true,
    }
}

pub fn generate(workspace_dir: PathBuf, services: Services) -> Result<()> {
    let srvs = sorted_services(services, enabled_service);
    let mut env = Environment::new();
    env.add_template("python", include_str!("python.j2"))?;
    env.add_function("snake_to_kebab_case", snake_to_kebab_case);
    env.add_function("service_to_feature", service_to_feature);
    env.add_function("service_to_pascal", service_to_pascal);
    env.add_function("make_python_type", make_python_type);
    env.add_function("make_pydoc_param_header", make_pydoc_param_header);
    env.add_function("make_pydoc_param", make_pydoc_param);
    let tmpl = env.get_template("python")?;

    let output = workspace_dir.join("bindings/python/src/services.rs");
    fs::write(output, tmpl.render(context! { srvs => srvs })?)?;
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
            '.' | '!' | '?' => {
                // Sentence end
                if i + 1 < chars.len() && chars[i + 1].is_whitespace() {
                    segments.push(&text[start..=i]);
                    start = i + 1;
                }
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
pub fn make_pydoc_param_header(name: &str, ty: ViaDeserialize<ConfigType>, optional: bool) -> Result<String, minijinja::Error> {
    let py_type = make_python_type(ty)?;
    let optional_str = if optional { ", optional" } else { "" };
    let first_line = format!("{} : {}{}", name, py_type, optional_str);

    Ok(first_line)
}

/// Generate a properly indented NumPy-style docstring line for a parameter
pub fn make_pydoc_param(comments: &str, ty: ViaDeserialize<ConfigType>) -> Result<String, minijinja::Error> {
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

    Ok(format_text(&desc_parts.join(". "), 20, 72).trim_end().to_string())
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
        ConfigType::String => "builtins.str",
    }
    .to_string())
}
