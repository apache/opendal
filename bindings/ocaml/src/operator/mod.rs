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

mod _type;
mod capability;
mod entry;
mod lister;
mod metadata;
mod operator_info;
mod reader;
mod writer;

use std::collections::LinkedList;
use std::sync::Mutex;

use _type::*;

use super::*;

#[ocaml::func]
#[ocaml::sig("string -> (string * string) list -> (operator, string) Result.t ")]
pub fn operator(
    scheme_str: &[u8],
    map: LinkedList<(&[u8], &[u8])>,
) -> Result<ocaml::Pointer<Operator>, String> {
    let scheme = parse_utf8(scheme_str, "scheme")?;
    let mut configs = HashMap::with_capacity(map.len());
    for (key, value) in map {
        let key = parse_utf8(key, "config key")?;
        let value = parse_utf8(value, "config value")?;
        configs.insert(key.to_owned(), value.to_owned());
    }

    let op = map_res_error(new_operator(scheme, configs))?;

    let runtime = RUNTIME.as_ref().map_err(|e| e.clone())?;
    let _enter = runtime.enter();
    let blocking_op = map_res_error(od::blocking::Operator::new(op))?;

    Ok(Operator(blocking_op).into())
}

#[ocaml::func]
#[ocaml::sig("operator -> string -> (entry array, string) Result.t ")]
pub fn blocking_list(
    operator: &Operator,
    path: &[u8],
) -> Result<Vec<ocaml::Pointer<Entry>>, String> {
    let path = parse_utf8(path, "path")?;
    map_res_error(
        operator
            .0
            .list(path)
            .map(|m| m.into_iter().map(|it| Entry(it).into()).collect()),
    )
}

#[ocaml::func]
#[ocaml::sig("operator -> string -> (lister, string) Result.t ")]
pub fn blocking_lister(operator: &Operator, path: &[u8]) -> Result<ocaml::Pointer<Lister>, String> {
    let path = parse_utf8(path, "path")?;
    map_res_error(operator.0.lister(path)).map(|op| Lister(Mutex::new(op)).into())
}

#[ocaml::func]
#[ocaml::sig("operator -> string -> (metadata, string) Result.t ")]
pub fn blocking_stat(operator: &Operator, path: &[u8]) -> Result<ocaml::Pointer<Metadata>, String> {
    let path = parse_utf8(path, "path")?;
    map_res_error(operator.0.stat(path).map(|m| Metadata(m).into()))
}

#[ocaml::func]
#[ocaml::sig("operator -> string -> (bool, string) Result.t ")]
pub fn blocking_is_exist(operator: &Operator, path: &[u8]) -> Result<bool, String> {
    let path = parse_utf8(path, "path")?;
    map_res_error(operator.0.exists(path))
}

#[ocaml::func]
#[ocaml::sig("operator -> string -> (bool, string) Result.t ")]
pub fn blocking_create_dir(operator: &Operator, path: &[u8]) -> Result<bool, String> {
    let path = parse_utf8(path, "path")?;
    map_res_error(operator.0.create_dir(path)).map(|()| true)
}

#[ocaml::func]
#[ocaml::sig("operator -> string -> (char array, string) Result.t ")]
pub fn blocking_read(operator: &Operator, path: &[u8]) -> Result<Vec<u8>, String> {
    let path = parse_utf8(path, "path")?;
    map_res_error(operator.0.read(path).map(|v| v.to_vec()))
}

#[ocaml::func]
#[ocaml::sig("operator -> string -> (reader, string) Result.t ")]
pub fn blocking_reader(operator: &Operator, path: &[u8]) -> Result<ocaml::Pointer<Reader>, String> {
    let path = parse_utf8(path, "path")?;
    map_res_error(operator.0.reader(path)).map(|op| Reader(op).into())
}

#[ocaml::func]
#[ocaml::sig("operator -> string -> bytes -> (unit, string) Result.t ")]
pub fn blocking_write(operator: &Operator, path: &[u8], bs: &[u8]) -> Result<(), String> {
    let path = parse_utf8(path, "path")?;
    // Core may retain the buffer after this call, but `bs` points into the OCaml
    // heap and can be mutated or moved by the GC. Hand over an owned copy.
    map_res_error(operator.0.write(path, bs.to_vec()).map(|_| ()))
}

#[ocaml::func]
#[ocaml::sig("operator -> string -> (writer, string) Result.t ")]
pub fn blocking_writer(operator: &Operator, path: &[u8]) -> Result<ocaml::Pointer<Writer>, String> {
    let path = parse_utf8(path, "path")?;
    map_res_error(operator.0.writer(path)).map(|op| Writer(Mutex::new(op)).into())
}

#[ocaml::func]
#[ocaml::sig("operator -> string -> string -> (unit, string) Result.t ")]
pub fn blocking_copy(operator: &Operator, from: &[u8], to: &[u8]) -> Result<(), String> {
    let from = parse_utf8(from, "from")?;
    let to = parse_utf8(to, "to")?;
    map_res_error(operator.0.copy(from, to).map(|_| ()))
}

#[ocaml::func]
#[ocaml::sig("operator -> string -> string -> (unit, string) Result.t ")]
pub fn blocking_rename(operator: &Operator, from: &[u8], to: &[u8]) -> Result<(), String> {
    let from = parse_utf8(from, "from")?;
    let to = parse_utf8(to, "to")?;
    map_res_error(operator.0.rename(from, to))
}

#[ocaml::func]
#[ocaml::sig("operator -> string -> (unit, string) Result.t ")]
pub fn blocking_delete(operator: &Operator, path: &[u8]) -> Result<(), String> {
    let path = parse_utf8(path, "path")?;
    map_res_error(operator.0.delete(path))
}

#[ocaml::func]
#[ocaml::sig("operator -> string array -> (unit, string) Result.t ")]
pub fn blocking_remove(operator: &Operator, path: Vec<&[u8]>) -> Result<(), String> {
    let paths = path
        .into_iter()
        .map(|p| parse_utf8(p, "path"))
        .collect::<Result<Vec<_>, _>>()?;
    map_res_error(operator.0.delete_iter(paths))
}

#[ocaml::func]
#[ocaml::sig("operator -> string -> (unit, string) Result.t ")]
pub fn blocking_remove_all(operator: &Operator, path: &[u8]) -> Result<(), String> {
    use opendal::options::DeleteOptions;
    let path = parse_utf8(path, "path")?;
    map_res_error(operator.0.delete_options(
        path,
        DeleteOptions {
            recursive: true,
            ..Default::default()
        },
    ))
}

#[ocaml::func]
#[ocaml::sig("operator -> (unit, string) Result.t ")]
pub fn blocking_check(operator: &Operator) -> Result<(), String> {
    map_res_error(operator.0.check())
}

#[ocaml::func]
#[ocaml::sig("operator -> operator_info ")]
pub fn operator_info(operator: &Operator) -> ocaml::Pointer<OperatorInfo> {
    OperatorInfo(operator.0.info()).into()
}

#[ocaml::func]
#[ocaml::sig("operator_info -> capability ")]
pub fn operator_info_capability(info: &OperatorInfo) -> ocaml::Pointer<Capability> {
    Capability(info.0.capability()).into()
}
