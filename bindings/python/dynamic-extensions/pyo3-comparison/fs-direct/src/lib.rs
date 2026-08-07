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

use opendal_core::Operator;
use opendal_service_fs::Fs;
use pyo3::prelude::*;
use pyo3_comparison_bridge::{PyOperator, RuntimeLayer, format_error, runtime};

fn build_fs(root: &str) -> PyResult<Operator> {
    let local_runtime = runtime();
    let _guard = local_runtime.enter();
    Operator::new(Fs::default().root(root))
        .map(|operator| operator.layer(RuntimeLayer::new(local_runtime.handle().clone())))
        .map_err(format_error)
}

#[pyfunction]
fn create(root: &str) -> PyResult<PyOperator> {
    build_fs(root).map(PyOperator::from_async)
}

#[pymodule(gil_used = false)]
fn opendal_fs_direct(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyOperator>()?;
    module.add_function(wrap_pyfunction!(create, module)?)
}
