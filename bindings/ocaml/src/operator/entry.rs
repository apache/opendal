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

use super::*;

#[ocaml::func]
#[ocaml::sig("entry -> string ")]
pub fn entry_path(entry: &mut Entry) -> String {
    entry.0.path().to_string()
}

#[ocaml::func]
#[ocaml::sig("entry -> string ")]
pub fn entry_name(entry: &mut Entry) -> String {
    entry.0.name().to_string()
}

#[ocaml::func]
#[ocaml::sig("entry -> metadata ")]
pub fn entry_metadata(entry: &mut Entry) -> ocaml::Pointer<Metadata> {
    Metadata(entry.0.metadata().clone()).into()
}

#[ocaml::func]
#[ocaml::sig("entry -> (string * metadata) ")]
pub fn entry_into_parts(entry: &mut Entry) -> (String, ocaml::Pointer<Metadata>) {
    (
        entry.0.path().to_string(),
        Metadata(entry.0.metadata().clone()).into(),
    )
}
