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

#[derive(ocaml::FromValue, ocaml::ToValue, Clone, Copy)]
#[ocaml::sig(
    "
| Start of int64  (** 
    [Start]: Sets the offset to the provided number of bytes.
    *)

| End of int64 (** 
    [End]: Sets the offset to the size of this object plus the specified number of
    bytes.

    It is possible to seek beyond the end of an object, but it's an error to
    seek before byte 0.
    *)

| Current of int64 (** 
    [Current]: Sets the offset to the current position plus the specified number of
    bytes.

    It is possible to seek beyond the end of an object, but it's an error to
    seek before byte 0.  
    *)
"
)]
pub enum SeekFrom {
    Start(u64),
    End(i64),
    Current(i64),
}

impl From<SeekFrom> for io::SeekFrom {
    fn from(value: SeekFrom) -> Self {
        match value {
            SeekFrom::Start(v) => io::SeekFrom::Start(v),
            SeekFrom::End(v) => io::SeekFrom::End(v),
            SeekFrom::Current(v) => io::SeekFrom::Current(v),
        }
    }
}
