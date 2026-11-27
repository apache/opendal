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
use pyo3::exceptions::PyIOError;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyType};

use crate::*;

create_exception!(
    opendal.exceptions,
    Error,
    PyException,
    "OpenDAL Base Exception"
);
create_exception!(
    opendal.exceptions,
    Unexpected,
    PyException,
    "Unexpected errors"
);
create_exception!(
    opendal.exceptions,
    Unsupported,
    PyException,
    "Unsupported operation"
);
create_exception!(
    opendal.exceptions,
    ConfigInvalid,
    PyException,
    "Config is invalid"
);
create_exception!(opendal.exceptions, NotFound, PyException, "Not found");
create_exception!(
    opendal.exceptions,
    PermissionDenied,
    PyException,
    "Permission denied"
);
create_exception!(
    opendal.exceptions,
    IsADirectory,
    PyException,
    "Is a directory"
);
create_exception!(
    opendal.exceptions,
    NotADirectory,
    PyException,
    "Not a directory"
);
create_exception!(
    opendal.exceptions,
    AlreadyExists,
    PyException,
    "Already exists"
);
create_exception!(opendal.exceptions, IsSameFile, PyException, "Is same file");
create_exception!(
    opendal.exceptions,
    ConditionNotMatch,
    PyException,
    "Condition not match"
);
create_exception!(opendal.exceptions, RateLimited, PyException, "Rate limited");
create_exception!(
    opendal.exceptions,
    RangeNotSatisfied,
    PyException,
    "Range not satisfied"
);
create_exception!(
    opendal.exceptions,
    Conflict,
    PyException,
    "Operation conflicts with the current resource state"
);
// Preserve handlers for checksum failures previously mapped to Unexpected.
create_exception!(
    opendal.exceptions,
    ChecksumMismatch,
    Unexpected,
    "Checksum verification failed"
);

const FILE_CHECKSUM_MISMATCH_NAME: &str = "_FileChecksumMismatch";

fn file_checksum_mismatch_type(py: Python<'_>) -> PyResult<Bound<'_, PyType>> {
    static TYPE: PyOnceLock<Py<PyType>> = PyOnceLock::new();
    let class = TYPE.get_or_try_init(py, || -> PyResult<_> {
        let namespace = PyDict::new(py);
        namespace.set_item("__module__", "opendal.exceptions")?;
        // ChecksumMismatch uses Exception's layout. OSError supplies the only
        // specialized exception layout in these bases.
        let bases = (
            py.get_type::<ChecksumMismatch>(),
            py.get_type::<PyIOError>(),
        );
        let class =
            py.get_type::<PyType>()
                .call1((FILE_CHECKSUM_MISMATCH_NAME, bases, namespace))?;
        Ok(class.cast_into::<PyType>()?.unbind())
    })?;
    Ok(class.bind(py).clone())
}

pub(crate) fn add_file_checksum_mismatch(module: &Bound<'_, PyModule>) -> PyResult<()> {
    // Pickle resolves the type by name. setattr keeps it out of __all__.
    module.setattr(
        FILE_CHECKSUM_MISMATCH_NAME,
        file_checksum_mismatch_type(module.py())?,
    )
}

fn format_pyerr_impl(err: &ocore::Error) -> PyErr {
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
        ocore::ErrorKind::RateLimited => RateLimited::new_err(e),
        ocore::ErrorKind::RangeNotSatisfied => RangeNotSatisfied::new_err(e),
        ocore::ErrorKind::Conflict => Conflict::new_err(e),
        ocore::ErrorKind::ChecksumMismatch => ChecksumMismatch::new_err(e),
        _ => Unexpected::new_err(e),
    }
}

pub fn format_pyerr(err: ocore::Error) -> PyErr {
    format_pyerr_impl(&err)
}

/// Format an error from a file write or flush.
///
/// Preserve conditional and checksum exceptions for file writes and flushes.
/// Checksum failures also retain compatibility with existing `OSError` handlers.
pub fn format_pyerr_from_write_error(err: ocore::Error) -> PyErr {
    match err.kind() {
        ocore::ErrorKind::ConditionNotMatch => format_pyerr_impl(&err),
        ocore::ErrorKind::ChecksumMismatch => {
            Python::attach(|py| match file_checksum_mismatch_type(py) {
                Ok(class) => PyErr::from_type(class, err.to_string()),
                Err(error) => error,
            })
        }
        _ => PyIOError::new_err(err.to_string()),
    }
}

/// Format a std::io::Error into a PyErr, checking if it wraps an OpenDAL error
pub fn format_pyerr_from_io_error(err: std::io::Error) -> PyErr {
    // Check if this io::Error wraps an OpenDAL error
    if let Some(opendal_err) = err.get_ref().and_then(|e| e.downcast_ref::<ocore::Error>()) {
        // If it does, format it as an OpenDAL error to preserve the error type
        format_pyerr_impl(opendal_err)
    } else {
        // Otherwise, format it as a regular IO error
        PyIOError::new_err(err.to_string())
    }
}
