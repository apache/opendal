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

//! Generator for the Ruby binding.

use super::capability;
use super::metadata::{self, MetadataKind};
use anyhow::Result;
use minijinja::value::Serde;
use minijinja::{Environment, context};
use serde::Serialize;
use std::fs;
use std::path::Path;

pub fn generate(workspace_dir: &Path) -> Result<()> {
    generate_capability(workspace_dir)?;
    generate_metadata(workspace_dir)
}

/// Render `bindings/ruby/src/capability.rs` from
/// `core/core/src/types/capability.rs`, so that `OpenDal::Capability` exposes
/// every core capability field with its core doc comment.
fn generate_capability(workspace_dir: &Path) -> Result<()> {
    let source = workspace_dir.join("core/core/src/types/capability.rs");
    let fields = capability::parse(&source)?;

    let mut env = Environment::new();
    env.add_template("capability_rs", include_str!("ruby/capability_rs.j2"))?;
    let rendered = env
        .get_template("capability_rs")?
        .render(context! { fields => Serde(&fields) })?;

    let output = workspace_dir.join("bindings/ruby/src/capability.rs");
    fs::write(output, format!("{rendered}\n"))?;
    Ok(())
}

/// One `OpenDal::Metadata` method, rendered from a core accessor.
#[derive(Debug, Serialize)]
struct MetadataMethod {
    /// Core accessor name, also used as the Rust function name.
    name: String,
    /// Ruby method name.
    ruby_name: String,
    /// The shape of the core return type, which picks the Ruby conversion.
    kind: MetadataKind,
    /// First paragraph of the core accessor doc.
    doc: String,
}

/// Render `bindings/ruby/src/metadata.rs` from
/// `core/core/src/types/metadata.rs`, so that `OpenDal::Metadata` exposes
/// every core accessor with its core doc comment.
fn generate_metadata(workspace_dir: &Path) -> Result<()> {
    let source = workspace_dir.join("core/core/src/types/metadata.rs");
    let methods: Vec<MetadataMethod> = metadata::parse(&source)?
        .into_iter()
        .map(|f| MetadataMethod {
            ruby_name: ruby_method_name(&f.name, f.kind),
            name: f.name,
            kind: f.kind,
            doc: f.doc,
        })
        .collect();

    let mut env = Environment::new();
    env.add_template("metadata_rs", include_str!("ruby/metadata_rs.j2"))?;
    let rendered = env
        .get_template("metadata_rs")?
        .render(context! { methods => Serde(&methods) })?;

    let output = workspace_dir.join("bindings/ruby/src/metadata.rs");
    fs::write(output, format!("{rendered}\n"))?;
    Ok(())
}

/// The Ruby method name for a core accessor.
///
/// Boolean `is_*` accessors become Ruby predicates, so `is_deleted` is exposed
/// as `deleted?`. Every other accessor keeps its core name.
fn ruby_method_name(name: &str, kind: MetadataKind) -> String {
    match (kind, name.strip_prefix("is_")) {
        (MetadataKind::Bool | MetadataKind::OptionBool, Some(rest)) => format!("{rest}?"),
        _ => name.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ruby_method_name() {
        assert_eq!(
            ruby_method_name("is_deleted", MetadataKind::Bool),
            "deleted?"
        );
        assert_eq!(
            ruby_method_name("is_current", MetadataKind::OptionBool),
            "current?"
        );
        assert_eq!(ruby_method_name("etag", MetadataKind::OptionStr), "etag");
        assert_eq!(
            ruby_method_name("is_x", MetadataKind::OptionStr),
            "is_x",
            "only boolean accessors become predicates"
        );
    }

    #[test]
    fn test_ruby_metadata_keeps_existing_methods() {
        let path = crate::workspace_dir().join("core/core/src/types/metadata.rs");
        let names: Vec<String> = metadata::parse(&path)
            .expect("core metadata must parse")
            .into_iter()
            .map(|f| ruby_method_name(&f.name, f.kind))
            .collect();

        // Methods exposed before the binding was generated must not be renamed.
        for existing in [
            "mode",
            "content_disposition",
            "content_length",
            "content_md5",
            "content_type",
            "etag",
            "last_modified",
        ] {
            assert!(
                names.iter().any(|n| n == existing),
                "`{existing}` must stay in OpenDal::Metadata"
            );
        }
    }
}
