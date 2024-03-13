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

use std::io;

use super::*;

#[ocaml::func]
#[ocaml::sig("reader -> bytes -> (int, string) Result.t ")]
pub fn reader_read(reader: &mut Reader, buf: &mut [u8]) -> Result<usize, String> {
    let bs = map_res_error(reader.0.read(buf.len()))?;
    buf[..bs.len()].copy_from_slice(&bs);
    Ok(bs.len())
}

#[ocaml::func]
#[ocaml::sig("reader -> Seek_from.seek_from -> (int64, string) Result.t ")]
pub fn reader_seek(reader: &mut Reader, pos: seek_from::SeekFrom) -> Result<u64, String> {
    map_res_error(reader.0.seek(io::SeekFrom::from(pos)))
}
