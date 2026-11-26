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

//! Generator for the C binding and the Go binding, which mirrors the C ABI.
//!
//! The Go binding loads `libopendal_c` through purego and reads
//! `opendal_capability` by memory layout, so both sides are rendered from the
//! same ordered field list to keep the layouts in lockstep.

use super::capability;
use anyhow::Result;
use minijinja::{Environment, context};
use serde::Serialize;
use std::fs;
use std::path::Path;

/// Render model handed to the C and Go capability templates.
#[derive(Serialize)]
struct CapabilityField {
    /// snake_case field name in core and in the C struct.
    name: String,
    /// lowerCamelCase field name in the Go `opendalCapability` mirror.
    go_field: String,
    /// `go_field` padded so that gofmt column alignment is already satisfied.
    go_field_padded: String,
    /// PascalCase accessor name on the Go `Capability`.
    go_method: String,
    /// Plain `bool` flag when true, `Option<usize>` limit when false.
    is_bool: bool,
    /// Single-line doc text from core.
    doc: String,
}

pub fn generate(workspace_dir: &Path) -> Result<()> {
    generate_capability(workspace_dir)
}

fn generate_capability(workspace_dir: &Path) -> Result<()> {
    let source = workspace_dir.join("core/core/src/types/capability.rs");
    let parsed = capability::parse(&source)?;

    let width = parsed
        .iter()
        .map(|f| to_pascal(&f.name).len())
        .max()
        .unwrap_or(0);
    let fields: Vec<CapabilityField> = parsed
        .into_iter()
        .map(|f| {
            let go_method = to_pascal(&f.name);
            let go_field = lower_first(&go_method);
            CapabilityField {
                go_field_padded: format!("{go_field:<width$} "),
                go_field,
                go_method,
                name: f.name,
                is_bool: f.is_bool,
                doc: f.doc,
            }
        })
        .collect();

    let mut env = Environment::new();
    env.add_template("capability_rs", include_str!("c/capability_rs.j2"))?;
    env.add_template("capability_go", include_str!("c/capability_go.j2"))?;

    let outputs = [
        ("capability_rs", "bindings/c/src/capability.rs"),
        ("capability_go", "bindings/go/capability.go"),
    ];
    for (template, relative) in outputs {
        let rendered = env
            .get_template(template)?
            .render(context! { fields => fields })?;
        fs::write(workspace_dir.join(relative), format!("{rendered}\n"))?;
    }
    Ok(())
}

fn to_pascal(snake: &str) -> String {
    snake
        .split('_')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => first.to_ascii_uppercase().to_string() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

fn lower_first(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        Some(first) => first.to_ascii_lowercase().to_string() + chars.as_str(),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_go_names() {
        assert_eq!(to_pascal("write_multi_max_size"), "WriteMultiMaxSize");
        assert_eq!(lower_first("WriteMultiMaxSize"), "writeMultiMaxSize");
        assert_eq!(to_pascal("stat"), "Stat");
    }
}
