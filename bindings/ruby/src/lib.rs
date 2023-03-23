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

use magnus::{
    class, define_class, define_global_function, error::Result, exception, function, method,
    prelude::*, Error,
};
use opendal::services::Memory;

fn hello_opendal() {
    let op = opendal::Operator::new(Memory::default()).unwrap().finish();
    println!("{op:?}")
}

fn build_operator(
    scheme: opendal::Scheme,
    map: HashMap<String, String>,
) -> Result<opendal::Operator> {
    use opendal::services::*;

    let op = match scheme {
        opendal::Scheme::Azblob => opendal::Operator::from_map::<Azblob>(map)
            .map_err(format_magnus_error)?
            .finish(),
        opendal::Scheme::Azdfs => opendal::Operator::from_map::<Azdfs>(map)
            .map_err(format_magnus_error)?
            .finish(),
        opendal::Scheme::Fs => opendal::Operator::from_map::<Fs>(map)
            .map_err(format_magnus_error)?
            .finish(),
        opendal::Scheme::Gcs => opendal::Operator::from_map::<Gcs>(map)
            .map_err(format_magnus_error)?
            .finish(),
        opendal::Scheme::Ghac => opendal::Operator::from_map::<Ghac>(map)
            .map_err(format_magnus_error)?
            .finish(),
        opendal::Scheme::Http => opendal::Operator::from_map::<Http>(map)
            .map_err(format_magnus_error)?
            .finish(),
        opendal::Scheme::Ipmfs => opendal::Operator::from_map::<Ipmfs>(map)
            .map_err(format_magnus_error)?
            .finish(),
        opendal::Scheme::Memory => opendal::Operator::from_map::<Memory>(map)
            .map_err(format_magnus_error)?
            .finish(),
        opendal::Scheme::Obs => opendal::Operator::from_map::<Obs>(map)
            .map_err(format_magnus_error)?
            .finish(),
        opendal::Scheme::Oss => opendal::Operator::from_map::<Oss>(map)
            .map_err(format_magnus_error)?
            .finish(),
        opendal::Scheme::S3 => opendal::Operator::from_map::<S3>(map)
            .map_err(format_magnus_error)?
            .finish(),
        opendal::Scheme::Webdav => opendal::Operator::from_map::<Webdav>(map)
            .map_err(format_magnus_error)?
            .finish(),
        opendal::Scheme::Webhdfs => opendal::Operator::from_map::<Webhdfs>(map)
            .map_err(format_magnus_error)?
            .finish(),
        _ => {
            return Err(format_magnus_error(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "not supported scheme",
            )))
        }
    };

    Ok(op)
}

#[magnus::wrap(class = "Operator", free_immediately, size)]
#[derive(Clone, Debug)]
pub struct Operator(opendal::BlockingOperator);

impl Operator {
    pub fn new(scheme: String, options: Option<HashMap<String, String>>) -> Result<Self> {
        let scheme = opendal::Scheme::from_str(&scheme)
            .map_err(|err| {
                opendal::Error::new(opendal::ErrorKind::Unexpected, "unsupported scheme")
                    .set_source(err)
            })
            .map_err(format_magnus_error)?;
        let options = options.unwrap_or_default();
        Ok(Operator(build_operator(scheme, options)?.blocking()))
    }

    /// Read the whole path into bytes.
    pub fn read(&self, path: String) -> Result<Vec<u8>> {
        self.0.read(&path).map_err(format_magnus_error)
    }

    /// Write bytes into given path.
    pub fn write(&self, path: String, bs: Vec<u8>) -> Result<()> {
        self.0.write(&path, bs).map_err(format_magnus_error)
    }
}

fn format_magnus_error(err: opendal::Error) -> Error {
    Error::new(exception::runtime_error(), err.to_string())
}

#[magnus::init]
fn init() -> Result<()> {
    let class = define_class("Operator", class::object())?;
    class.define_singleton_method("new", function!(Operator::new, 2))?;
    class.define_method("read", method!(Operator::read, 1))?;
    class.define_method("write", method!(Operator::write, 2))?;
    define_global_function("hello_opendal", function!(hello_opendal, 0));
    Ok(())
}
