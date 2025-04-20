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

use derive_builder::Builder;
use encoding_rs::{Encoding, UTF_8};

/// Options for reading files. These options can be used to specify various parameters
/// when reading a file.
#[derive(Builder, Debug)]
#[builder(setter(into))]
pub struct ReadOptions {
    /// The starting offset for reading. Defaults to 0, which means reading from the beginning of the file.
    #[builder(default = "0")]
    pub offset: u64,

    /// The number of bytes to read. If set to -1, it means reading all content from the offset to the end of the file.
    /// In Rust, we use `Option<u64>` to represent this, `None` means reading to the end.
    #[builder(default)]
    pub length: Option<u64>,

    /// The buffer size used for reading. Defaults to 8192 bytes (8KB).
    #[builder(default = "8192")]
    pub buffer_size: usize,

    /// The charset used for decoding text files. Defaults to UTF-8.
    #[builder(default = "UTF_8")]
    pub charset: &'static Encoding,

    /// Whether to skip new line characters when reading text files. Defaults to false.
    #[builder(default = "false")]
    pub skip_new_line: bool,
}
