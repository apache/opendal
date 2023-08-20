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

use std::path::PathBuf;

pub fn main() -> std::io::Result<()> {
    let root = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());

    ocaml_build::Sigs::new("src/seek_from.ml")
        .with_source_dir(root.join("src/seek_from"))
        .generate()?;
    ocaml_build::Sigs::new("src/operator.ml")
        .with_source_dir(root.join("src/operator"))
        .generate()
}
