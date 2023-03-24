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
use magnus::define_module;
use magnus::error::Result;
use magnus::exception;
use magnus::function;
use magnus::method;
use magnus::prelude::*;
use magnus::Error;
use magnus::RString;
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

#[magnus::wrap(class = "OpenDAL::Operator", free_immediately, size)]
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
    pub fn read(&self, path: String) -> Result<RString> {
        let bytes = self.0.read(&path).map_err(format_magnus_error)?;
        Ok(RString::from_slice(&bytes))
    }

    /// Write string into given path.
    pub fn write(&self, path: String, bs: RString) -> Result<()> {
        self.0
            .write(&path, bs.to_bytes())
            .map_err(format_magnus_error)
    }

    /// Get current path's metadata **without cache** directly.
    pub fn stat(&self, path: String) -> Result<Metadata> {
        self.0
            .stat(&path)
            .map_err(format_magnus_error)
            .map(Metadata)
    }
}

#[magnus::wrap(class = "OpenDAL::Metadata", free_immediately, size)]
pub struct Metadata(od::Metadata);

impl Metadata {
    /// Content-Disposition of this object
    pub fn content_disposition(&self) -> Option<&str> {
        self.0.content_disposition()
    }

    /// Content length of this entry.
    pub fn content_length(&self) -> u64 {
        self.0.content_length()
    }

    /// Content MD5 of this entry.
    pub fn content_md5(&self) -> Option<&str> {
        self.0.content_md5()
    }

    /// Content Type of this entry.
    pub fn content_type(&self) -> Option<&str> {
        self.0.content_type()
    }

    /// ETag of this entry.
    pub fn etag(&self) -> Option<&str> {
        self.0.etag()
    }

    /// Returns `True` if this is a file.
    pub fn is_file(&self) -> bool {
        self.0.is_file()
    }

    /// Returns `True` if this is a directory.
    pub fn is_dir(&self) -> bool {
        self.0.is_dir()
    }
}

fn format_magnus_error(err: od::Error) -> Error {
    Error::new(exception::runtime_error(), err.to_string())
}

#[magnus::init]
fn init() -> Result<()> {
    let namespace = define_module("OpenDAL")?;
    let operator_class = namespace.define_class("Operator", class::object())?;
    operator_class.define_singleton_method("new", function!(Operator::new, 2))?;
    operator_class.define_method("read", method!(Operator::read, 1))?;
    operator_class.define_method("write", method!(Operator::write, 2))?;
    operator_class.define_method("stat", method!(Operator::stat, 1))?;

    let metadata_class = namespace.define_class("Metadata", class::object())?;
    metadata_class.define_method(
        "content_disposition",
        method!(Metadata::content_disposition, 0),
    )?;
    metadata_class.define_method("content_length", method!(Metadata::content_length, 0))?;
    metadata_class.define_method("content_md5", method!(Metadata::content_md5, 0))?;
    metadata_class.define_method("content_type", method!(Metadata::content_type, 0))?;
    metadata_class.define_method("etag", method!(Metadata::etag, 0))?;
    metadata_class.define_method("is_file", method!(Metadata::is_file, 0))?;
    metadata_class.define_method("is_dir", method!(Metadata::is_dir, 0))?;
    Ok(())
}
