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

// Remove this allow after <https://github.com/rust-lang/rust-clippy/issues/12039> fixed.
#![allow(clippy::unnecessary_fallible_conversions)]

use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::ops::DerefMut;
use std::sync::Arc;

use futures::AsyncReadExt;
use futures::AsyncSeekExt;
use futures::AsyncWriteExt;
use pyo3::exceptions::PyIOError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_asyncio::tokio::future_into_py;
use tokio::sync::Mutex;

use crate::*;

/// A file-like object.
/// Can be used as a context manager.
#[pyclass(module = "opendal")]
pub struct File(FileState);

enum FileState {
    Reader(ocore::BlockingReader),
    Writer(ocore::BlockingWriter),
    Closed,
}

impl File {
    pub fn new_reader(reader: ocore::BlockingReader) -> Self {
        Self(FileState::Reader(reader))
    }

    pub fn new_writer(writer: ocore::BlockingWriter) -> Self {
        Self(FileState::Writer(writer))
    }
}

#[pymethods]
impl File {
    /// Read and return size bytes, or if size is not given, until EOF.
    #[pyo3(signature = (size=None,))]
    pub fn read<'p>(&'p mut self, py: Python<'p>, size: Option<usize>) -> PyResult<&'p PyAny> {
        let reader = match &mut self.0 {
            FileState::Reader(r) => r,
            FileState::Writer(_) => {
                return Err(PyIOError::new_err(
                    "I/O operation failed for reading on write only file.",
                ));
            }
            FileState::Closed => {
                return Err(PyIOError::new_err(
                    "I/O operation failed for reading on closed file.",
                ));
            }
        };

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

        Buffer::new(buffer).into_memory_view_ref(py)
    }

    /// Write bytes into the file.
    pub fn write(&mut self, bs: &[u8]) -> PyResult<()> {
        let writer = match &mut self.0 {
            FileState::Reader(_) => {
                return Err(PyIOError::new_err(
                    "I/O operation failed for reading on read only file.",
                ));
            }
            FileState::Writer(w) => w,
            FileState::Closed => {
                return Err(PyIOError::new_err(
                    "I/O operation failed for reading on closed file.",
                ));
            }
        };

        writer
            .write_all(bs)
            .map_err(|err| PyIOError::new_err(err.to_string()))
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
        let reader = match &mut self.0 {
            FileState::Reader(r) => r,
            FileState::Writer(_) => {
                return Err(PyIOError::new_err(
                    "I/O operation failed for reading on write only file.",
                ));
            }
            FileState::Closed => {
                return Err(PyIOError::new_err(
                    "I/O operation failed for reading on closed file.",
                ));
            }
        };

        let whence = match whence {
            0 => SeekFrom::Start(pos as u64),
            1 => SeekFrom::Current(pos),
            2 => SeekFrom::End(pos),
            _ => return Err(PyValueError::new_err("invalid whence")),
        };

        reader
            .seek(whence)
            .map_err(|err| PyIOError::new_err(err.to_string()))
    }

    /// Return the current stream position.
    pub fn tell(&mut self) -> PyResult<u64> {
        let reader = match &mut self.0 {
            FileState::Reader(r) => r,
            FileState::Writer(_) => {
                return Err(PyIOError::new_err(
                    "I/O operation failed for reading on write only file.",
                ));
            }
            FileState::Closed => {
                return Err(PyIOError::new_err(
                    "I/O operation failed for reading on closed file.",
                ));
            }
        };

        reader
            .stream_position()
            .map_err(|err| PyIOError::new_err(err.to_string()))
    }

    fn close(&mut self) -> PyResult<()> {
        match &mut self.0 {
            FileState::Writer(w) => {
                w.close()
                    .map_err(|err| PyIOError::new_err(err.to_string()))?;
            }
            _ => {}
        };
        self.0 = FileState::Closed;
        Ok(())
    }

    pub fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    pub fn __exit__(&mut self, _exc_type: PyObject, _exc_value: PyObject, _traceback: PyObject) {
        let _ = self.close();
    }
}

/// A file-like async reader.
/// Can be used as an async context manager.
#[pyclass(module = "opendal")]
pub struct AsyncFile(Arc<Mutex<AsyncFileState>>);

enum AsyncFileState {
    Reader(ocore::Reader),
    Writer(ocore::Writer),
    Closed,
}

impl AsyncFile {
    pub fn new_reader(reader: ocore::Reader) -> Self {
        Self(Arc::new(Mutex::new(AsyncFileState::Reader(reader))))
    }

    pub fn new_writer(writer: ocore::Writer) -> Self {
        Self(Arc::new(Mutex::new(AsyncFileState::Writer(writer))))
    }
}

#[pymethods]
impl AsyncFile {
    /// Read and return size bytes, or if size is not given, until EOF.
    pub fn read<'p>(&'p self, py: Python<'p>, size: Option<usize>) -> PyResult<&'p PyAny> {
        let state = self.0.clone();

        future_into_py(py, async move {
            let mut guard = state.lock().await;
            let reader = match guard.deref_mut() {
                AsyncFileState::Reader(r) => r,
                AsyncFileState::Writer(_) => {
                    return Err(PyIOError::new_err(
                        "I/O operation failed for reading on write only file.",
                    ));
                }
                _ => {
                    return Err(PyIOError::new_err(
                        "I/O operation failed for reading on closed file.",
                    ));
                }
            };

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

            Python::with_gil(|py| Buffer::new(buffer).into_memory_view(py))
        })
    }

    /// Write bytes into the file.
    pub fn write<'p>(&'p mut self, py: Python<'p>, bs: &'p [u8]) -> PyResult<&'p PyAny> {
        let state = self.0.clone();

        // FIXME: can we avoid this clone?
        let bs = bs.to_vec();

        future_into_py(py, async move {
            let mut guard = state.lock().await;
            let writer = match guard.deref_mut() {
                AsyncFileState::Reader(_) => {
                    return Err(PyIOError::new_err(
                        "I/O operation failed for reading on read only file.",
                    ));
                }
                AsyncFileState::Writer(w) => w,
                _ => {
                    return Err(PyIOError::new_err(
                        "I/O operation failed for reading on closed file.",
                    ));
                }
            };

            writer
                .write_all(&bs)
                .await
                .map_err(|err| PyIOError::new_err(err.to_string()))
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
        let state = self.0.clone();

        let whence = match whence {
            0 => SeekFrom::Start(pos as u64),
            1 => SeekFrom::Current(pos),
            2 => SeekFrom::End(pos),
            _ => return Err(PyValueError::new_err("invalid whence")),
        };

        future_into_py(py, async move {
            let mut guard = state.lock().await;
            let reader = match guard.deref_mut() {
                AsyncFileState::Reader(r) => r,
                AsyncFileState::Writer(_) => {
                    return Err(PyIOError::new_err(
                        "I/O operation failed for reading on write only file.",
                    ));
                }
                _ => {
                    return Err(PyIOError::new_err(
                        "I/O operation failed for reading on closed file.",
                    ));
                }
            };

            let ret = reader
                .seek(whence)
                .await
                .map_err(|err| PyIOError::new_err(err.to_string()))?;
            Ok(Python::with_gil(|py| ret.into_py(py)))
        })
    }

    /// Return the current stream position.
    pub fn tell<'p>(&'p mut self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let state = self.0.clone();

        future_into_py(py, async move {
            let mut guard = state.lock().await;
            let reader = match guard.deref_mut() {
                AsyncFileState::Reader(r) => r,
                AsyncFileState::Writer(_) => {
                    return Err(PyIOError::new_err(
                        "I/O operation failed for reading on write only file.",
                    ));
                }
                _ => {
                    return Err(PyIOError::new_err(
                        "I/O operation failed for reading on closed file.",
                    ));
                }
            };

            let pos = reader
                .stream_position()
                .await
                .map_err(|err| PyIOError::new_err(err.to_string()))?;
            Ok(Python::with_gil(|py| pos.into_py(py)))
        })
    }

    fn close<'p>(&'p mut self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let state = self.0.clone();
        future_into_py(py, async move {
            let mut state = state.lock().await;
            match &mut *state {
                AsyncFileState::Writer(w) => {
                    w.close()
                        .await
                        .map_err(|err| PyIOError::new_err(err.to_string()))?;
                }
                _ => {}
            }
            *state = AsyncFileState::Closed;
            Ok(())
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
        let state = self.0.clone();
        future_into_py(py, async move {
            let mut state = state.lock().await;
            match &mut *state {
                AsyncFileState::Writer(w) => {
                    w.close()
                        .await
                        .map_err(|err| PyIOError::new_err(err.to_string()))?;
                }
                _ => {}
            }
            *state = AsyncFileState::Closed;
            Ok(())
        })
    }
}
