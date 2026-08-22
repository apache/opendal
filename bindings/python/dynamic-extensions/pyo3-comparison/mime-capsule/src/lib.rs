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

use opendal_layer_mime_guess::MimeGuessLayer as CoreMimeGuessLayer;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use pyo3_comparison_bridge::{from_operator_capsule, to_operator_capsule};

#[pyclass(name = "MimeGuessLayer")]
struct MimeGuessLayer;

#[pymethods]
impl MimeGuessLayer {
    #[new]
    fn new() -> Self {
        Self
    }

    fn _layer_apply<'py>(
        &self,
        py: Python<'py>,
        capsule: &Bound<'py, PyCapsule>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let operator = from_operator_capsule(capsule)?;
        to_operator_capsule(py, operator.layer(CoreMimeGuessLayer::default()))
    }
}

#[pymodule(gil_used = false)]
fn opendal_mime_capsule(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<MimeGuessLayer>()
}
