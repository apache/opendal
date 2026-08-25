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

//! Generator for the .NET binding capability mirrors.
//!
//! The capability payload crosses the FFI boundary as a `#[repr(C)]` struct,
//! so the Rust mirror, the C# interop struct, and the public C# surface must
//! agree on field order and on the sentinel that encodes an absent limit.
//! All three files are rendered from `core/core/src/types/capability.rs` so
//! they cannot drift from core or from each other.

use anyhow::Result;
use minijinja::{Environment, context};
use serde::Serialize;
use std::fs;
use std::path::PathBuf;

use super::capability;

/// Render model handed to the templates, with every casing precomputed so
/// the templates stay purely structural.
#[derive(Serialize)]
struct Field {
    /// snake_case, used by the Rust mirror.
    name: String,
    /// PascalCase, used by the public C# properties.
    pascal: String,
    /// camelCase, used by the C# interop struct.
    camel: String,
    /// Plain `bool` flag when true, `Option<usize>` limit when false.
    is_bool: bool,
    /// Field doc from core, XML-escaped for use in C# doc comments.
    doc: String,
}

pub fn generate(workspace_dir: PathBuf) -> Result<()> {
    let source = workspace_dir.join("core/core/src/types/capability.rs");
    let fields: Vec<Field> = capability::parse(&source)?
        .into_iter()
        .map(|f| Field {
            pascal: heck::AsUpperCamelCase(&f.name).to_string(),
            camel: heck::AsLowerCamelCase(&f.name).to_string(),
            name: f.name,
            is_bool: f.is_bool,
            doc: xml_escape(&f.doc),
        })
        .collect();

    let mut env = Environment::new();
    env.add_template("capability_rs", include_str!("dotnet_capability_rs.j2"))?;
    env.add_template("interop_cs", include_str!("dotnet_interop_cs.j2"))?;
    env.add_template("public_cs", include_str!("dotnet_public_cs.j2"))?;

    let outputs = [
        ("capability_rs", "bindings/dotnet/src/capability.rs"),
        (
            "interop_cs",
            "bindings/dotnet/OpenDAL/Interop/NativeObject/OpenDALCapability.cs",
        ),
        ("public_cs", "bindings/dotnet/OpenDAL/Capability.cs"),
    ];
    for (template, relative) in outputs {
        let tmpl = env.get_template(template)?;
        let mut rendered = tmpl.render(context! { fields => fields })?;
        if !rendered.ends_with('\n') {
            rendered.push('\n');
        }
        fs::write(workspace_dir.join(relative), rendered)?;
    }

    Ok(())
}

/// Escape the characters that are special inside C# XML doc comments.
fn xml_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}
