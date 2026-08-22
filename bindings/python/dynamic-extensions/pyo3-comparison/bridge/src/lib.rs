// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod runtime_layer;

use std::ffi::CStr;
use std::sync::LazyLock;

use opendal_core::Operator;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyCapsule, PyCapsuleMethods};

pub use runtime_layer::RuntimeLayer;

const OPERATOR_CAPSULE_NAME: &CStr = c"opendal.poc.operator.v1";

pub fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("prototype Tokio runtime should build")
    });
    &RUNTIME
}

pub fn format_error(error: opendal_core::Error) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(error.to_string())
}

pub fn to_operator_capsule(py: Python<'_>, op: Operator) -> PyResult<Bound<'_, PyCapsule>> {
    PyCapsule::new_with_value(py, op, OPERATOR_CAPSULE_NAME)
}

pub fn from_operator_capsule(capsule: &Bound<'_, PyCapsule>) -> PyResult<Operator> {
    let pointer = capsule
        .pointer_checked(Some(OPERATOR_CAPSULE_NAME))?
        .cast::<Operator>();
    Ok(unsafe { pointer.as_ref().clone() })
}

#[pyclass(module = "opendal_poc", name = "Operator")]
pub struct PyOperator {
    op: Operator,
}

impl PyOperator {
    pub fn from_async(op: Operator) -> Self {
        Self { op }
    }

    pub fn async_operator(&self) -> &Operator {
        &self.op
    }
}

#[pymethods]
impl PyOperator {
    #[new]
    fn new() -> PyResult<Self> {
        let _guard = runtime().enter();
        let op = Operator::new(opendal_core::services::Memory::default()).map_err(format_error)?;
        Ok(Self { op })
    }

    #[staticmethod]
    fn _from_capsule(capsule: &Bound<'_, PyCapsule>) -> PyResult<Self> {
        Ok(Self {
            op: from_operator_capsule(capsule)?,
        })
    }

    fn layer(&self, py: Python<'_>, layer: &Bound<'_, PyAny>) -> PyResult<Self> {
        let capsule = to_operator_capsule(py, self.op.clone())?;
        let result = layer.call_method1(intern!(py, "_layer_apply"), (capsule,))?;
        let result = result.cast::<PyCapsule>()?;
        Self::_from_capsule(result)
    }

    fn scheme(&self) -> String {
        self.op.info().scheme().to_string()
    }

    fn content_type(&self, py: Python<'_>, path: String) -> PyResult<Option<String>> {
        let op = self.op.clone();
        py.detach(move || {
            runtime()
                .block_on(op.stat(&path))
                .map(|metadata| metadata.content_type().map(str::to_string))
                .map_err(format_error)
        })
    }

    fn write(&self, py: Python<'_>, path: String, content: Vec<u8>) -> PyResult<()> {
        let op = self.op.clone();
        py.detach(move || {
            runtime()
                .block_on(op.write(&path, content))
                .map(|_| ())
                .map_err(format_error)
        })
    }

    fn read<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyBytes>> {
        let op = self.op.clone();
        let content = py.detach(move || {
            runtime()
                .block_on(op.read(&path))
                .map(|buffer| buffer.to_vec())
                .map_err(format_error)
        })?;
        Ok(PyBytes::new(py, &content))
    }
}
