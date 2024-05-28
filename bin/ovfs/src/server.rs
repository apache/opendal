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

use std::ffi::FromBytesWithNulError;
use std::io;

use crate::virtiofs_utils::{Reader, Writer};

/// The Error here represents filesystem message related errors.
#[derive(Debug)]
pub enum Error {
    DecodeMessage(io::Error),
    EncodeMessage(io::Error),
    MissingParameter,
    InvalidHeaderLength,
    InvalidCString(FromBytesWithNulError),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        use Error::*;

        unimplemented!()
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

/// The Server will parse the filesystem request from VMs and forward it to the specified method.
pub struct Server {}

impl Server {
    pub fn new() -> Server {
        Server {}
    }

    pub fn handle_message(&self, r: Reader, w: Writer) -> Result<usize> {
        unimplemented!()
    }
}
