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

use std::{collections::HashMap, str::FromStr};

use magnus::{class, define_class, error::Result, exception, function, method, prelude::*, Error};
use opendal as od;

fn build_operator(scheme: od::Scheme, map: HashMap<String, String>) -> Result<od::Operator> {
    use od::services::*;

    let op = match scheme {
        od::Scheme::Azblob => od::Operator::from_map::<Azblob>(map)
            .map_err(format_magnus_error)?
            .finish(),
        od::Scheme::Azdfs => od::Operator::from_map::<Azdfs>(map)
            .map_err(format_magnus_error)?
            .finish(),
        od::Scheme::Fs => od::Operator::from_map::<Fs>(map)
            .map_err(format_magnus_error)?
            .finish(),
        od::Scheme::Gcs => od::Operator::from_map::<Gcs>(map)
            .map_err(format_magnus_error)?
            .finish(),
        od::Scheme::Ghac => od::Operator::from_map::<Ghac>(map)
            .map_err(format_magnus_error)?
            .finish(),
        od::Scheme::Http => od::Operator::from_map::<Http>(map)
            .map_err(format_magnus_error)?
            .finish(),
        od::Scheme::Ipmfs => od::Operator::from_map::<Ipmfs>(map)
            .map_err(format_magnus_error)?
            .finish(),
        od::Scheme::Memory => od::Operator::from_map::<Memory>(map)
            .map_err(format_magnus_error)?
            .finish(),
        od::Scheme::Obs => od::Operator::from_map::<Obs>(map)
            .map_err(format_magnus_error)?
            .finish(),
        od::Scheme::Oss => od::Operator::from_map::<Oss>(map)
            .map_err(format_magnus_error)?
            .finish(),
        od::Scheme::S3 => od::Operator::from_map::<S3>(map)
            .map_err(format_magnus_error)?
            .finish(),
        od::Scheme::Webdav => od::Operator::from_map::<Webdav>(map)
            .map_err(format_magnus_error)?
            .finish(),
        od::Scheme::Webhdfs => od::Operator::from_map::<Webhdfs>(map)
            .map_err(format_magnus_error)?
            .finish(),
        _ => {
            return Err(format_magnus_error(od::Error::new(
                od::ErrorKind::Unexpected,
                "not supported scheme",
            )))
        }
    };

    Ok(op)
}

#[magnus::wrap(class = "Operator", free_immediately, size)]
#[derive(Clone, Debug)]
pub struct Operator(od::BlockingOperator);

impl Operator {
    pub fn new(scheme: String, options: Option<HashMap<String, String>>) -> Result<Self> {
        let scheme = od::Scheme::from_str(&scheme)
            .map_err(|err| {
                od::Error::new(od::ErrorKind::Unexpected, "unsupported scheme").set_source(err)
            })
            .map_err(format_magnus_error)?;
        let options = options.unwrap_or_default();
        Ok(Operator(build_operator(scheme, options)?.blocking()))
    }

    /// Read the whole path into string.
    pub fn read(&self, path: String) -> Result<String> {
        Ok(format!(
            "{:?}",
            self.0.read(&path).map_err(format_magnus_error)
        ))
    }

    /// Write string into given path.
    pub fn write(&self, path: String, bs: String) -> Result<()> {
        self.0.write(&path, bs).map_err(format_magnus_error)
    }
}

fn format_magnus_error(err: od::Error) -> Error {
    Error::new(exception::runtime_error(), err.to_string())
}

#[magnus::init]
fn init() -> Result<()> {
    let class = define_class("Operator", class::object())?;
    class.define_singleton_method("new", function!(Operator::new, 2))?;
    class.define_method("read", method!(Operator::read, 1))?;
    class.define_method("write", method!(Operator::write, 2))?;
    Ok(())
}
