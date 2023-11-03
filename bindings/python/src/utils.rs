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

use std::os::raw::c_int;

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::exceptions::PyFileExistsError;
use pyo3::exceptions::PyFileNotFoundError;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::exceptions::PyPermissionError;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::AsPyPointer;

use crate::*;

create_exception!(opendal, Error, PyException, "OpenDAL related errors");

/// A bytes-like object that implements buffer protocol.
#[pyclass(module = "opendal")]
pub struct Buffer {
    inner: Vec<u8>,
}

#[pymethods]
impl Buffer {
    unsafe fn __getbuffer__(
        slf: PyRefMut<Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let bytes = slf.inner.as_slice();
        let ret = ffi::PyBuffer_FillInfo(
            view,
            slf.as_ptr() as *mut _,
            bytes.as_ptr() as *mut _,
            bytes.len().try_into().unwrap(),
            1, // read only
            flags,
        );
        if ret == -1 {
            return Err(PyErr::fetch(slf.py()));
        }
        Ok(())
    }
}

impl From<Vec<u8>> for Buffer {
    fn from(inner: Vec<u8>) -> Self {
        Self { inner }
    }
}

pub fn format_pyerr(err: ocore::Error) -> PyErr {
    use ocore::ErrorKind::*;
    match err.kind() {
        NotFound => PyFileNotFoundError::new_err(err.to_string()),
        AlreadyExists => PyFileExistsError::new_err(err.to_string()),
        PermissionDenied => PyPermissionError::new_err(err.to_string()),
        Unsupported => PyNotImplementedError::new_err(err.to_string()),
        _ => Error::new_err(err.to_string()),
    }
}
