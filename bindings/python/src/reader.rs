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

use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::sync::Arc;

use futures::AsyncReadExt;
use futures::AsyncSeekExt;
use pyo3::exceptions::PyIOError;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::exceptions::PyValueError;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::AsPyPointer;
use pyo3_asyncio::tokio::future_into_py;
use tokio::sync::Mutex;

use crate::*;

/// A file-like blocking reader.
/// Can be used as a context manager.
#[pyclass(module = "opendal")]
pub struct Reader(Option<ocore::BlockingReader>);

impl Reader {
    pub fn new(reader: ocore::BlockingReader) -> Self {
        Self(Some(reader))
    }

    fn as_mut(&mut self) -> PyResult<&mut ocore::BlockingReader> {
        let reader = self
            .0
            .as_mut()
            .ok_or_else(|| PyValueError::new_err("I/O operation on closed file."))?;
        Ok(reader)
    }
}

#[pymethods]
impl Reader {
    /// Read and return size bytes, or if size is not given, until EOF.
    #[pyo3(signature = (size=None,))]
    pub fn read<'p>(&'p mut self, py: Python<'p>, size: Option<usize>) -> PyResult<&'p PyAny> {
        let reader = self.as_mut()?;
        let buffer = match size {
            Some(size) => {
                let mut buffer = vec![0; size];
                reader
                    .read_exact(&mut buffer)
                    .map_err(|err| PyIOError::new_err(err.to_string()))?;
                buffer
            }
            None => {
                let mut buffer = Vec::new();
                reader
                    .read_to_end(&mut buffer)
                    .map_err(|err| PyIOError::new_err(err.to_string()))?;
                buffer
            }
        };
        let buffer = Buffer::from(buffer).into_py(py);
        let memoryview =
            unsafe { py.from_owned_ptr_or_err(ffi::PyMemoryView_FromObject(buffer.as_ptr()))? };
        Ok(memoryview)
    }

    /// `Reader` doesn't support write.
    /// Raises a `NotImplementedError` if called.
    pub fn write(&mut self, _bs: &[u8]) -> PyResult<()> {
        Err(PyNotImplementedError::new_err(
            "Reader does not support write",
        ))
    }

    /// Change the stream position to the given byte offset.
    /// offset is interpreted relative to the position indicated by `whence`.
    /// The default value for whence is `SEEK_SET`. Values for `whence` are:
    ///
    /// * `SEEK_SET` or `0` – start of the stream (the default); offset should be zero or positive
    /// * `SEEK_CUR` or `1` – current stream position; offset may be negative
    /// * `SEEK_END` or `2` – end of the stream; offset is usually negative
    ///
    /// Return the new absolute position.
    #[pyo3(signature = (pos, whence = 0))]
    pub fn seek(&mut self, pos: i64, whence: u8) -> PyResult<u64> {
        let whence = match whence {
            0 => SeekFrom::Start(pos as u64),
            1 => SeekFrom::Current(pos),
            2 => SeekFrom::End(pos),
            _ => return Err(PyValueError::new_err("invalid whence")),
        };
        let reader = self.as_mut()?;
        reader
            .seek(whence)
            .map_err(|err| PyIOError::new_err(err.to_string()))
    }

    /// Return the current stream position.
    pub fn tell(&mut self) -> PyResult<u64> {
        let reader = self.as_mut()?;
        reader
            .stream_position()
            .map_err(|err| PyIOError::new_err(err.to_string()))
    }

    pub fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    pub fn __exit__(&mut self, _exc_type: PyObject, _exc_value: PyObject, _traceback: PyObject) {
        drop(self.0.take());
    }
}

enum ReaderState {
    Init {
        operator: ocore::Operator,
        path: String,
    },
    Open(ocore::Reader),
    Closed,
}

impl ReaderState {
    async fn reader(&mut self) -> PyResult<&mut ocore::Reader> {
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

/// A file-like async reader.
/// Can be used as an async context manager.
#[pyclass(module = "opendal")]
pub struct AsyncReader(Arc<Mutex<ReaderState>>);

impl AsyncReader {
    pub fn new(operator: ocore::Operator, path: String) -> Self {
        Self(Arc::new(Mutex::new(ReaderState::Init { operator, path })))
    }
}

#[pymethods]
impl AsyncReader {
    /// Read and return size bytes, or if size is not given, until EOF.
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
            Python::with_gil(|py| {
                let buffer = Buffer::from(buffer).into_py(py);
                unsafe {
                    PyObject::from_owned_ptr_or_err(
                        py,
                        ffi::PyMemoryView_FromObject(buffer.as_ptr()),
                    )
                }
            })
        })
    }

    /// `AsyncReader` doesn't support write.
    /// Raises a `NotImplementedError` if called.
    pub fn write<'p>(&'p mut self, py: Python<'p>, _bs: &'p [u8]) -> PyResult<&'p PyAny> {
        future_into_py::<_, PyObject>(py, async move {
            Err(PyNotImplementedError::new_err(
                "AsyncReader does not support write",
            ))
        })
    }

    /// Change the stream position to the given byte offset.
    /// offset is interpreted relative to the position indicated by `whence`.
    /// The default value for whence is `SEEK_SET`. Values for `whence` are:
    ///
    /// * `SEEK_SET` or `0` – start of the stream (the default); offset should be zero or positive
    /// * `SEEK_CUR` or `1` – current stream position; offset may be negative
    /// * `SEEK_END` or `2` – end of the stream; offset is usually negative
    ///
    /// Return the new absolute position.
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

    /// Return the current stream position.
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
