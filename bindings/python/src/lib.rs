// Copyright 2022 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use ::opendal::services;
use ::opendal::Operator;
use pyo3::prelude::*;

#[pyfunction]
fn debug() -> PyResult<String> {
    let op = Operator::create(services::Memory::default())
        .unwrap()
        .finish();
    Ok(format!("{:?}", op.metadata()))
}

#[pymodule]
fn opendal(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(debug, m)?)?;
    Ok(())
}
