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

use magnus::exception;
use magnus::function;
use magnus::Error;
use magnus::Ruby;

// We will use `ocore::` to represents opendal rust core functionalities.
// This convention aligns with the Python binding.
pub use ::opendal as ocore;

mod capability;
mod io;
mod metadata;
mod operator;

pub fn format_magnus_error(err: ocore::Error) -> Error {
    Error::new(exception::runtime_error(), err.to_string())
}

/// Apache OpenDAL™ Ruby binding
#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    let gem_module = ruby.define_module("OpenDAL")?;
    let _ = operator::include(&gem_module);
    let _ = metadata::include(&gem_module);
    let _ = capability::include(&gem_module);
    let _ = io::include(&gem_module);

    Ok(())
}
