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

use std::sync::Arc;

use futures::TryStreamExt;
use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::{prelude::*, IntoPyObjectExt};
use pyo3_async_runtimes::tokio::future_into_py;
use tokio::sync::Mutex;

use crate::*;

#[pyclass(unsendable, module = "opendal")]
pub struct BlockingLister(ocore::BlockingLister);

impl BlockingLister {
    /// Create a new blocking lister.
    pub fn new(inner: ocore::BlockingLister) -> Self {
        Self(inner)
    }
}

#[pymethods]
impl BlockingLister {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<PyObject>> {
        match slf.0.next() {
            Some(Ok(entry)) => Ok(Some(Entry::new(entry).into_py_any(slf.py())?)),
            Some(Err(err)) => {
                let pyerr = format_pyerr(err);
                Err(pyerr)
            }
            None => Ok(None),
        }
    }
}

#[pyclass(module = "opendal")]
pub struct AsyncLister(Arc<Mutex<ocore::Lister>>);

impl AsyncLister {
    pub fn new(lister: ocore::Lister) -> Self {
        Self(Arc::new(Mutex::new(lister)))
    }
}

#[pymethods]
impl AsyncLister {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<Self> {
        slf
    }
    fn __anext__(slf: PyRefMut<'_, Self>) -> PyResult<Option<PyObject>> {
        let lister = slf.0.clone();
        let fut = future_into_py(slf.py(), async move {
            let mut lister = lister.lock().await;
            let entry = lister.try_next().await.map_err(format_pyerr)?;
            match entry {
                Some(entry) => Python::with_gil(|py| {
                    let py_obj = Entry::new(entry).into_py_any(py)?;
                    Ok(Some(py_obj))
                }),
                None => Err(PyStopAsyncIteration::new_err("stream exhausted")),
            }
        });

        match fut {
            Ok(fut) => Ok(Some(fut.into())),
            Err(e) => Err(e),
        }
    }
}
