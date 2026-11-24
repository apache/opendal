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
#[ocaml::sig("writer -> bytes -> (unit, string) Result.t ")]
pub fn writer_write(writer: &mut Writer, bs: &[u8]) -> Result<(), String> {
    // Core may retain the buffer after this call, but `bs` points into the OCaml
    // heap and can be mutated or moved by the GC. Hand over an owned copy.
    map_res_error(writer.0.write(bs.to_vec()))
}

#[ocaml::func]
#[ocaml::sig("writer -> (metadata, string) Result.t ")]
pub fn writer_close(writer: &mut Writer) -> Result<ocaml::Pointer<Metadata>, String> {
    map_res_error(writer.0.close()).map(|m| Metadata(m).into())
}
