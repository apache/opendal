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

use std::collections::HashMap;
use std::str::FromStr;

use magnus::class;
use magnus::exception;
use magnus::method;
use magnus::prelude::*;
use magnus::Error;
use magnus::RModule;
use magnus::RString;

use crate::metadata::Metadata;
use crate::*;

#[magnus::wrap(class = "OpenDAL::Operator", free_immediately, size)]
#[derive(Clone, Debug)]
struct Operator(ocore::BlockingOperator);

fn format_magnus_error(err: ocore::Error) -> Error {
    Error::new(exception::runtime_error(), err.to_string())
}

impl Operator {
    fn new(scheme: String, options: Option<HashMap<String, String>>) -> Result<Self, Error> {
        let scheme = ocore::Scheme::from_str(&scheme)
            .map_err(|err| {
                ocore::Error::new(ocore::ErrorKind::Unexpected, "unsupported scheme")
                    .set_source(err)
            })
            .map_err(format_magnus_error)?;
        let options = options.unwrap_or_default();

        let op = ocore::Operator::via_iter(scheme, options)
            .map_err(format_magnus_error)?
            .blocking();
        Ok(Operator(op))
    }

    /// Reads the whole path into string.
    fn read(&self, path: String) -> Result<bytes::Bytes, Error> {
        let buffer = self.0.read(&path).map_err(format_magnus_error)?;
        Ok(buffer.to_bytes())
    }

    /// Writes string into given path.
    fn write(&self, path: String, bs: RString) -> Result<(), Error> {
        self.0
            .write(&path, bs.to_bytes())
            .map_err(format_magnus_error)
    }

    /// Gets current path's metadata **without cache** directly.
    fn stat(&self, path: String) -> Result<Metadata, Error> {
        self.0
            .stat(&path)
            .map_err(format_magnus_error)
            .map(Metadata::new)
    }
}

pub fn include(gem_module: &RModule) -> Result<(), Error> {
    let class = gem_module.define_class("Operator", class::object())?;
    class.define_singleton_method("new", function!(Operator::new, 2))?;
    class.define_method("read", method!(Operator::read, 1))?;
    class.define_method("write", method!(Operator::write, 2))?;
    class.define_method("stat", method!(Operator::stat, 1))?;

    Ok(())
}
