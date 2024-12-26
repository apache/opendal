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

mod parser;

mod binding_python;

use anyhow::Result;
use std::path::PathBuf;

pub fn run(language: &str) -> Result<()> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let services_path = manifest_dir.join("../core/src/services").canonicalize()?;
    let project_root = manifest_dir.join("..").canonicalize()?;
    let services = parser::parse(&services_path.to_string_lossy())?;

    match language {
        "python" | "py" => binding_python::generate(project_root, &services),
        _ => Err(anyhow::anyhow!("Unsupported language: {}", language)),
    }
}
