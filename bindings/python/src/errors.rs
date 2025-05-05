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

use pyo3::create_exception;
use pyo3::exceptions::PyException;

use crate::*;

create_exception!(
    opendal.exceptions,
    Error,
    PyException,
    "OpenDAL Base Exception"
);
create_exception!(opendal.exceptions, Unexpected, Error, "Unexpected errors");
create_exception!(
    opendal.exceptions,
    Unsupported,
    Error,
    "Unsupported operation"
);
create_exception!(
    opendal.exceptions,
    ConfigInvalid,
    Error,
    "Config is invalid"
);
create_exception!(opendal.exceptions, NotFound, Error, "Not found");
create_exception!(
    opendal.exceptions,
    PermissionDenied,
    Error,
    "Permission denied"
);
create_exception!(opendal.exceptions, IsADirectory, Error, "Is a directory");
create_exception!(opendal.exceptions, NotADirectory, Error, "Not a directory");
create_exception!(opendal.exceptions, AlreadyExists, Error, "Already exists");
create_exception!(opendal.exceptions, IsSameFile, Error, "Is same file");
create_exception!(
    opendal.exceptions,
    ConditionNotMatch,
    Error,
    "Condition not match"
);

pub fn format_pyerr(err: ocore::Error) -> PyErr {
    let e = format!("{err:?}");
    match err.kind() {
        ocore::ErrorKind::Unexpected => Unexpected::new_err(e),
        ocore::ErrorKind::Unsupported => Unsupported::new_err(e),
        ocore::ErrorKind::ConfigInvalid => ConfigInvalid::new_err(e),
        ocore::ErrorKind::NotFound => NotFound::new_err(e),
        ocore::ErrorKind::PermissionDenied => PermissionDenied::new_err(e),
        ocore::ErrorKind::IsADirectory => IsADirectory::new_err(e),
        ocore::ErrorKind::NotADirectory => NotADirectory::new_err(e),
        ocore::ErrorKind::AlreadyExists => AlreadyExists::new_err(e),
        ocore::ErrorKind::IsSameFile => IsSameFile::new_err(e),
        ocore::ErrorKind::ConditionNotMatch => ConditionNotMatch::new_err(e),
        _ => Unexpected::new_err(e),
    }
}
