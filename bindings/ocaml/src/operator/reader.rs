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

use std::io::Read;

use super::*;

#[ocaml::func]
#[ocaml::sig("reader -> bytes -> int64 -> (int, string) Result.t ")]
pub fn reader_pread(reader: &Reader, buf: &mut [u8], offset: i64) -> Result<usize, String> {
    let offset =
        u64::try_from(offset).map_err(|_| format!("offset must be non-negative, got {offset}"))?;
    if buf.is_empty() {
        return Ok(0);
    }

    // An unbounded range stops at EOF and returns a short read instead of failing
    // the bounded-range completeness check when the buffer exceeds the remaining
    // bytes. It also avoids computing `offset + buf.len()`, which can overflow.
    let mut reader = map_res_error(reader.0.clone().into_std_read(offset..))?;
    reader.read(buf).map_err(|err| err.to_string())
}
