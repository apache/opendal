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

use std::path::PathBuf;

use pyo3::prelude::*;
use pyo3::types::PyString;
use pyo3::{Borrowed, IntoPyObject};

/// A filesystem path stored as a `String`.
///
/// Accepts `str`/`os.PathLike` and renders the same type on output, so a config
/// field's constructor parameter and getter share one type.
#[derive(Clone, Debug)]
pub struct PyPath(pub String);

impl PyPath {
    pub fn into_string(self) -> String {
        self.0
    }
}

impl From<String> for PyPath {
    fn from(value: String) -> Self {
        PyPath(value)
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for PyPath {
    type Error = PyErr;

    const INPUT_TYPE: pyo3::inspect::PyStaticExpr = <PathBuf as FromPyObject>::INPUT_TYPE;

    fn extract(obj: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {
        let path = PathBuf::extract(obj)?;
        Ok(PyPath(path.to_string_lossy().into_owned()))
    }
}

impl<'py> IntoPyObject<'py> for PyPath {
    type Target = PyString;
    type Output = Bound<'py, PyString>;
    type Error = PyErr;

    // Override the output type to match the input type (`str | os.PathLike[str]`).
    const OUTPUT_TYPE: pyo3::inspect::PyStaticExpr = <PathBuf as FromPyObject>::INPUT_TYPE;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(PyString::new(py, &self.0))
    }
}
