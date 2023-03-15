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

use std::collections::HashMap;
use std::io::SeekFrom;
use std::str::FromStr;
use std::sync::Arc;

use ::opendal as od;
use futures::TryStreamExt;
use pyo3::exceptions::PyIOError;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::types::PyDict;
use pyo3_asyncio::tokio::future_into_py;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::sync::Mutex;

use crate::{build_operator, format_pyerr, layers, Entry, Metadata};

#[pyclass(module = "opendal")]
pub struct AsyncOperator(od::Operator);

#[pymethods]
impl AsyncOperator {
    #[new]
    #[pyo3(signature = (scheme, *, layers=Vec::new(), **map))]
    pub fn new(scheme: &str, layers: Vec<layers::Layer>, map: Option<&PyDict>) -> PyResult<Self> {
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

        Ok(AsyncOperator(build_operator(scheme, map, layers)?))
    }

    pub fn read<'p>(&'p self, py: Python<'p>, path: String) -> PyResult<&'p PyAny> {
        let this = self.0.clone();
        future_into_py(py, async move {
            let res: Vec<u8> = this.read(&path).await.map_err(format_pyerr)?;
            let pybytes: PyObject = Python::with_gil(|py| PyBytes::new(py, &res).into());
            Ok(pybytes)
        })
    }

    pub fn open_reader(&self, path: String) -> PyResult<AsyncReader> {
        Ok(AsyncReader::new(ReaderState::Init {
            operator: self.0.clone(),
            path,
        }))
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

    pub fn list<'p>(&'p self, py: Python<'p>, path: String) -> PyResult<&'p PyAny> {
        let this = self.0.clone();
        future_into_py(py, async move {
            let lister = this.list(&path).await.map_err(format_pyerr)?;
            let pylister: PyObject = Python::with_gil(|py| AsyncLister::new(lister).into_py(py));
            Ok(pylister)
        })
    }

    pub fn scan<'p>(&'p self, py: Python<'p>, path: String) -> PyResult<&'p PyAny> {
        let this = self.0.clone();
        future_into_py(py, async move {
            let lister = this.scan(&path).await.map_err(format_pyerr)?;
            let pylister: PyObject = Python::with_gil(|py| AsyncLister::new(lister).into_py(py));
            Ok(pylister)
        })
    }
}

enum ReaderState {
    Init {
        operator: od::Operator,
        path: String,
    },
    Open(od::Reader),
    Closed,
}

impl ReaderState {
    async fn reader(&mut self) -> PyResult<&mut od::Reader> {
        let reader = match self {
            ReaderState::Init { operator, path } => {
                let reader = operator.reader(path).await.map_err(format_pyerr)?;
                *self = ReaderState::Open(reader);
                if let ReaderState::Open(ref mut reader) = self {
                    reader
                } else {
                    unreachable!()
                }
            }
            ReaderState::Open(ref mut reader) => reader,
            ReaderState::Closed => {
                return Err(PyValueError::new_err("I/O operation on closed file."));
            }
        };
        Ok(reader)
    }

    fn close(&mut self) {
        *self = ReaderState::Closed;
    }
}

#[pyclass(module = "opendal")]
pub struct AsyncReader(Arc<Mutex<ReaderState>>);

impl AsyncReader {
    fn new(reader: ReaderState) -> Self {
        Self(Arc::new(Mutex::new(reader)))
    }
}

#[pymethods]
impl AsyncReader {
    pub fn read<'p>(&'p self, py: Python<'p>, size: Option<usize>) -> PyResult<&'p PyAny> {
        let reader = self.0.clone();
        future_into_py(py, async move {
            let mut state = reader.lock().await;
            let reader = state.reader().await?;
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
            let mut state = reader.lock().await;
            let reader = state.reader().await?;
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
            let mut state = reader.lock().await;
            let reader = state.reader().await?;
            let pos = reader
                .stream_position()
                .await
                .map_err(|err| PyIOError::new_err(err.to_string()))?;
            Ok(Python::with_gil(|py| pos.into_py(py)))
        })
    }

    fn __aenter__<'a>(slf: PyRef<'a, Self>, py: Python<'a>) -> PyResult<&'a PyAny> {
        let slf = slf.into_py(py);
        future_into_py(py, async move { Ok(slf) })
    }

    fn __aexit__<'a>(
        &self,
        py: Python<'a>,
        _exc_type: &'a PyAny,
        _exc_value: &'a PyAny,
        _traceback: &'a PyAny,
    ) -> PyResult<&'a PyAny> {
        let reader = self.0.clone();
        future_into_py(py, async move {
            let mut state = reader.lock().await;
            state.close();
            Ok(())
        })
    }
}

#[pyclass(module = "opendal")]
struct AsyncLister(Arc<Mutex<od::Lister>>);

impl AsyncLister {
    fn new(lister: od::Lister) -> Self {
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
                Some(entry) => Ok(Python::with_gil(|py| Entry(entry).into_py(py))),
                None => Err(PyStopAsyncIteration::new_err("stream exhausted")),
            }
        })?;
        Ok(Some(fut.into()))
    }
}
