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

//! Generator for the OCaml binding.
//!
//! Renders the Rust FFI accessors (`src/operator/capability.rs`) and the
//! public `Capability` module (`lib/capability.ml{,i}`). The `external`
//! declarations in `src/operator.mli` are derived from the Rust file by
//! `ocaml-build` during `dune build`.

use super::capability;
use anyhow::Result;
use minijinja::{Environment, context};
use std::fs;
use std::path::Path;

pub fn generate(workspace_dir: &Path) -> Result<()> {
    generate_capability(workspace_dir)
}

fn generate_capability(workspace_dir: &Path) -> Result<()> {
    let source = workspace_dir.join("core/core/src/types/capability.rs");
    let fields = capability::parse(&source)?;

    let mut env = Environment::new();
    env.add_filter("ocaml_doc", ocaml_doc);
    env.add_template("capability_rs", include_str!("ocaml/capability_rs.j2"))?;
    env.add_template("capability_ml", include_str!("ocaml/capability_ml.j2"))?;
    env.add_template("capability_mli", include_str!("ocaml/capability_mli.j2"))?;

    let outputs = [
        ("capability_rs", "bindings/ocaml/src/operator/capability.rs"),
        ("capability_ml", "bindings/ocaml/lib/capability.ml"),
        ("capability_mli", "bindings/ocaml/lib/capability.mli"),
    ];
    for (template, relative) in outputs {
        let rendered = env
            .get_template(template)?
            .render(context! { fields => fields })?;
        fs::write(workspace_dir.join(relative), format!("{rendered}\n"))?;
    }
    Ok(())
}

/// Escape text for an odoc comment: `{`, `}`, `[`, `]` and `@` are markup.
fn ocaml_doc(doc: &str) -> String {
    let mut out = String::with_capacity(doc.len());
    for c in doc.chars() {
        if matches!(c, '{' | '}' | '[' | ']' | '@') {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ocaml_doc_escapes_markup() {
        assert_eq!(ocaml_doc("see [x] @a {b}"), "see \\[x\\] \\@a \\{b\\}");
    }
}
