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
use super::metadata::{self, MetadataKind};
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

/// Render model handed to the C and Go metadata templates.
#[derive(Serialize)]
struct MetadataAccessor {
    /// snake_case accessor name in core.
    name: String,
    /// Exported C function that returns this accessor's value.
    c_function: String,
    /// PascalCase method name on the Go `Metadata`.
    go_method: String,
    /// lowerCamelCase field name in the Go `Metadata`.
    go_field: String,
    /// `go_field` padded so that gofmt aligns the struct field types.
    go_field_padded: String,
    /// `go_field:` padded so that gofmt aligns the composite literal values.
    go_key_padded: String,
    /// The shape of the core return type, which picks the C and Go mapping.
    kind: MetadataKind,
    /// First paragraph of the core accessor doc.
    doc: String,
    /// The core doc rephrased to start with the Go method name.
    go_doc: String,
}

pub fn generate(workspace_dir: &Path) -> Result<()> {
    generate_capability(workspace_dir)?;
    generate_metadata(workspace_dir)
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

fn generate_metadata(workspace_dir: &Path) -> Result<()> {
    let source = workspace_dir.join("core/core/src/types/metadata.rs");
    let parsed = metadata::parse(&source)?;

    let width = parsed
        .iter()
        .map(|f| go_field_name(&f.name).len())
        .max()
        .unwrap_or(0);
    let accessors: Vec<MetadataAccessor> = parsed
        .into_iter()
        .map(|f| {
            let go_field = go_field_name(&f.name);
            // Keep the released C function names.
            let c_function = match f.kind {
                MetadataKind::OptionTimestamp => format!("opendal_metadata_{}_ms", f.name),
                MetadataKind::OptionUserMetadata => format!("opendal_metadata_get_{}", f.name),
                _ => format!("opendal_metadata_{}", f.name),
            };
            let go_method = go_method_name(&f.name);
            let go_doc = match f.doc.strip_prefix("Return ") {
                Some(rest) => format!("{go_method} returns {rest}"),
                None => format!("{go_method} reports: {}", f.doc),
            };
            MetadataAccessor {
                c_function,
                go_method,
                go_doc,
                go_field_padded: format!("{go_field:<width$} "),
                go_key_padded: format!("{:<w$} ", format!("{go_field}:"), w = width + 1),
                go_field,
                name: f.name,
                kind: f.kind,
                doc: f.doc,
            }
        })
        .collect();

    let mut env = Environment::new();
    env.add_template("metadata_rs", include_str!("c/metadata_rs.j2"))?;
    env.add_template("metadata_go", include_str!("c/metadata_go.j2"))?;

    let outputs = [
        ("metadata_rs", "bindings/c/src/metadata.rs"),
        ("metadata_go", "bindings/go/metadata.go"),
    ];
    for (template, relative) in outputs {
        let rendered = env
            .get_template(template)?
            .render(context! { accessors => accessors })?;
        fs::write(workspace_dir.join(relative), format!("{rendered}\n"))?;
    }
    Ok(())
}

/// Go spells these words as initialisms, for example `ContentMD5` and `ETag`.
const GO_INITIALISMS: &[(&str, &str)] = &[("md5", "MD5"), ("etag", "ETag")];

fn go_word(word: &str) -> String {
    match GO_INITIALISMS.iter().find(|(w, _)| *w == word) {
        Some((_, initialism)) => initialism.to_string(),
        None => to_pascal(word),
    }
}

fn go_method_name(snake: &str) -> String {
    snake.split('_').map(go_word).collect()
}

fn go_field_name(snake: &str) -> String {
    let mut words = snake.split('_');
    let first = words.next().unwrap_or_default().to_string();
    words.fold(first, |name, word| name + &go_word(word))
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

        assert_eq!(go_method_name("content_md5"), "ContentMD5");
        assert_eq!(go_method_name("etag"), "ETag");
        assert_eq!(go_field_name("content_md5"), "contentMD5");
        assert_eq!(go_field_name("etag"), "etag");
        assert_eq!(go_field_name("last_modified"), "lastModified");
    }
}
