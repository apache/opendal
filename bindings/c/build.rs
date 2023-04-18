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

extern crate cbindgen;

use std::io::ErrorKind;
use std::{path::Path, process::Command};

fn main() {
    let header_file = Path::new("include").join("opendal.h");

    cbindgen::generate(".")
        .expect("Unable to generate bindings")
        .write_to_file(header_file);

    if let Err(e) = Command::new("clang-format")
        .arg("--style=WebKit")
        .arg("--verbose")
        .arg("-i")
        .arg("include/opendal.h")
        .spawn()
    {
        if e.kind() == ErrorKind::NotFound {
            panic!("\x1b[31mclang-format\x1b[0m not found, please install it through package manager first");
        } else {
            panic!("Failed to run build.rs: {}", e)
        }
    }
}
