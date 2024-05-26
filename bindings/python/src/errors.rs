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

create_exception!(opendal, Error, PyException, "OpenDAL Base Exception");
create_exception!(opendal, UnexpectedError, Error, "Unexpected errors");
create_exception!(opendal, UnsupportedError, Error, "Unsupported operation");
create_exception!(opendal, ConfigInvalidError, Error, "Config is invalid");
create_exception!(opendal, NotFoundError, Error, "Not found");
create_exception!(opendal, PermissionDeniedError, Error, "Permission denied");
create_exception!(opendal, IsADirectoryError, Error, "Is a directory");
create_exception!(opendal, NotADirectoryError, Error, "Not a directory");
create_exception!(opendal, AlreadyExistsError, Error, "Already exists");
create_exception!(opendal, IsSameFileError, Error, "Is same file");
create_exception!(
    opendal,
    ConditionNotMatchError,
    Error,
    "Condition not match"
);

pub fn format_pyerr(err: ocore::Error) -> PyErr {
    use ocore::ErrorKind::*;
    match err.kind() {
        Unexpected => UnexpectedError::new_err(err.to_string()),
        Unsupported => UnsupportedError::new_err(err.to_string()),
        ConfigInvalid => ConfigInvalidError::new_err(err.to_string()),
        NotFound => NotFoundError::new_err(err.to_string()),
        PermissionDenied => PermissionDeniedError::new_err(err.to_string()),
        IsADirectory => IsADirectoryError::new_err(err.to_string()),
        NotADirectory => NotADirectoryError::new_err(err.to_string()),
        AlreadyExists => AlreadyExistsError::new_err(err.to_string()),
        IsSameFile => IsSameFileError::new_err(err.to_string()),
        ConditionNotMatch => ConditionNotMatchError::new_err(err.to_string()),
        _ => UnexpectedError::new_err(err.to_string()),
    }
}
