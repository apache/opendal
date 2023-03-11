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

use std::collections::HashMap;
use std::io::SeekFrom;
use std::str::FromStr;
use std::sync::Arc;

use ::opendal as od;
use pyo3::exceptions::PyIOError;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::types::PyDict;
use pyo3_asyncio::tokio::future_into_py;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::sync::Mutex;

use crate::{build_operator, format_pyerr, Metadata};

#[pyclass(module = "opendal")]
pub struct AsyncOperator(od::Operator);

#[pymethods]
impl AsyncOperator {
    #[new]
    #[pyo3(signature = (scheme, **map))]
    pub fn new(scheme: &str, map: Option<&PyDict>) -> PyResult<Self> {
        let scheme = od::Scheme::from_str(scheme)
            .map_err(|err| {
                od::Error::new(od::ErrorKind::Unexpected, "unsupported scheme").set_source(err)
            })
            .map_err(format_pyerr)?;
        let map = map
            .map(|v| {
                v.extract::<HashMap<String, String>>()
                    .expect("must be valid hashmap")
            })
            .unwrap_or_default();

        Ok(AsyncOperator(build_operator(scheme, map)?))
    }

    pub fn read<'p>(&'p self, py: Python<'p>, path: String) -> PyResult<&'p PyAny> {
        let this = self.0.clone();
        future_into_py(py, async move {
            let res: Vec<u8> = this.read(&path).await.map_err(format_pyerr)?;
            let pybytes: PyObject = Python::with_gil(|py| PyBytes::new(py, &res).into());
            Ok(pybytes)
        })
    }

    pub fn open_reader<'p>(&'p self, py: Python<'p>, path: String) -> PyResult<&'p PyAny> {
        let this = self.0.clone();
        future_into_py(py, async move {
            let reader = this.reader(&path).await.map_err(format_pyerr)?;
            let pyreader: PyObject = Python::with_gil(|py| AsyncReader::new(reader).into_py(py));
            Ok(pyreader)
        })
    }

    pub fn write<'p>(&'p self, py: Python<'p>, path: String, bs: Vec<u8>) -> PyResult<&'p PyAny> {
        let this = self.0.clone();
        future_into_py(py, async move {
            this.write(&path, bs).await.map_err(format_pyerr)
        })
    }

    pub fn stat<'p>(&'p self, py: Python<'p>, path: String) -> PyResult<&'p PyAny> {
        let this = self.0.clone();
        future_into_py(py, async move {
            let res: Metadata = this.stat(&path).await.map_err(format_pyerr).map(Metadata)?;

            Ok(res)
        })
    }

    pub fn create_dir<'p>(&'p self, py: Python<'p>, path: String) -> PyResult<&'p PyAny> {
        let this = self.0.clone();
        future_into_py(py, async move {
            this.create_dir(&path).await.map_err(format_pyerr)
        })
    }

    pub fn delete<'p>(&'p self, py: Python<'p>, path: String) -> PyResult<&'p PyAny> {
        let this = self.0.clone();
        future_into_py(
            py,
            async move { this.delete(&path).await.map_err(format_pyerr) },
        )
    }
}

#[pyclass(module = "opendal")]
pub struct AsyncReader(Arc<Mutex<od::Reader>>);

impl AsyncReader {
    fn new(reader: od::Reader) -> Self {
        Self(Arc::new(Mutex::new(reader)))
    }
}

#[pymethods]
impl AsyncReader {
    pub fn read<'p>(&'p self, py: Python<'p>, size: Option<usize>) -> PyResult<&'p PyAny> {
        let reader = self.0.clone();
        future_into_py(py, async move {
            let mut reader = reader.lock().await;
            let buffer = match size {
                Some(size) => {
                    let mut buffer = vec![0; size];
                    reader
                        .read_exact(&mut buffer)
                        .await
                        .map_err(|err| PyIOError::new_err(err.to_string()))?;
                    buffer
                }
                None => {
                    let mut buffer = Vec::new();
                    reader
                        .read_to_end(&mut buffer)
                        .await
                        .map_err(|err| PyIOError::new_err(err.to_string()))?;
                    buffer
                }
            };
            let pybytes: PyObject = Python::with_gil(|py| PyBytes::new(py, &buffer).into());
            Ok(pybytes)
        })
    }

    pub fn write<'p>(&'p mut self, py: Python<'p>, _bs: &'p [u8]) -> PyResult<&'p PyAny> {
        future_into_py::<_, PyObject>(py, async move {
            Err(PyNotImplementedError::new_err(
                "AsyncReader does not support write",
            ))
        })
    }

    #[pyo3(signature = (pos, whence = 0))]
    pub fn seek<'p>(&'p mut self, py: Python<'p>, pos: i64, whence: u8) -> PyResult<&'p PyAny> {
        let whence = match whence {
            0 => SeekFrom::Start(pos as u64),
            1 => SeekFrom::Current(pos),
            2 => SeekFrom::End(pos),
            _ => return Err(PyValueError::new_err("invalid whence")),
        };
        let reader = self.0.clone();
        future_into_py(py, async move {
            let mut reader = reader.lock().await;
            let ret = reader
                .seek(whence)
                .await
                .map_err(|err| PyIOError::new_err(err.to_string()))?;
            Ok(Python::with_gil(|py| ret.into_py(py)))
        })
    }

    pub fn tell<'p>(&'p mut self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let reader = self.0.clone();
        future_into_py(py, async move {
            let mut reader = reader.lock().await;
            let pos = reader
                .stream_position()
                .await
                .map_err(|err| PyIOError::new_err(err.to_string()))?;
            Ok(Python::with_gil(|py| pos.into_py(py)))
        })
    }
}
