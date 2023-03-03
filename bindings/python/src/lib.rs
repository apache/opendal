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

use ::opendal as od;
use pyo3::exceptions::{PyBaseException, PyFileNotFoundError};
use pyo3::prelude::*;
use pyo3_asyncio::tokio::future_into_py;

#[pyclass]
struct Operator(od::Operator);

#[pymethods]
impl Operator {
    #[new]
    pub fn new() -> Self {
        let op = od::Operator::create(od::services::Memory::default())
            .unwrap()
            .finish();

        Operator(op)
    }

    pub fn object(&self, path: &str) -> Object {
        let o = self.0.object(path);

        Object(o)
    }
}

#[pyclass]
struct Object(od::Object);

#[pymethods]
impl Object {
    pub fn path(&self) -> &str {
        self.0.path()
    }

    pub fn blocking_read(&self) -> PyResult<Vec<u8>> {
        self.0.blocking_read().map_err(format_pyerr)
    }

    pub fn read<'p>(&'p self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let this = self.0.clone();
        future_into_py(py, async move {
            let res: Vec<u8> = this.read().await.map_err(format_pyerr)?;
            Ok(res)
        })
    }

    pub fn blocking_write(&self, bs: Vec<u8>) -> PyResult<()> {
        self.0.blocking_write(bs).map_err(format_pyerr)
    }
}

fn format_pyerr(err: od::Error) -> PyErr {
    use od::ErrorKind::*;
    match err.kind() {
        ObjectNotFound => PyFileNotFoundError::new_err(err.to_string()),
        _ => PyBaseException::new_err(err.to_string()),
    }
}

#[pymodule]
fn opendal(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Operator>()?;
    m.add_class::<Object>()?;
    Ok(())
}
