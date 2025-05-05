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

use std::io::BufRead;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::ops::DerefMut;
use std::sync::Arc;

use futures::AsyncReadExt;
use futures::AsyncSeekExt;
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::PyIOError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use pyo3_async_runtimes::tokio::future_into_py;
use tokio::sync::Mutex;

use crate::*;

/// A file-like object.
/// Can be used as a context manager.
#[pyclass(module = "opendal")]
pub struct File(FileState);

enum FileState {
    Reader(ocore::StdReader),
    Writer(ocore::StdWriter),
    Closed,
}

impl File {
    pub fn new_reader(reader: ocore::StdReader) -> Self {
        Self(FileState::Reader(reader))
    }

    pub fn new_writer(writer: ocore::BlockingWriter) -> Self {
        Self(FileState::Writer(writer.into_std_write()))
    }
}

#[pymethods]
impl File {
    /// Read and return at most size bytes, or if size is not given, until EOF.
    #[pyo3(signature = (size=None,))]
    pub fn read<'p>(
        &'p mut self,
        py: Python<'p>,
        size: Option<usize>,
    ) -> PyResult<Bound<'p, PyAny>> {
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
                let mut bs = vec![0; size];
                let n = reader
                    .read(&mut bs)
                    .map_err(|err| PyIOError::new_err(err.to_string()))?;
                bs.truncate(n);
                bs
            }
            None => {
                let mut buffer = Vec::new();
                reader
                    .read_to_end(&mut buffer)
                    .map_err(|err| PyIOError::new_err(err.to_string()))?;
                buffer
            }
        };

        Buffer::new(buffer).into_bytes_ref(py)
    }

    /// Read a single line from the file.
    /// A newline character (`\n`) is left at the end of the string, and is only omitted on the last line of the file if the file doesn’t end in a newline.
    /// If size is specified, at most size bytes will be read.
    #[pyo3(signature = (size=None,))]
    pub fn readline<'p>(
        &'p mut self,
        py: Python<'p>,
        size: Option<usize>,
    ) -> PyResult<Bound<'p, PyAny>> {
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
            None => {
                let mut buffer = Vec::new();
                reader
                    .read_until(b'\n', &mut buffer)
                    .map_err(|err| PyIOError::new_err(err.to_string()))?;
                buffer
            }
            Some(size) => {
                let mut bs = vec![0; size];
                let mut reader = reader.take(size as u64);
                let n = reader
                    .read_until(b'\n', &mut bs)
                    .map_err(|err| PyIOError::new_err(err.to_string()))?;
                bs.truncate(n);
                bs
            }
        };

        Buffer::new(buffer).into_bytes_ref(py)
    }

    /// Read bytes into a pre-allocated, writable buffer
    pub fn readinto(&mut self, buffer: PyBuffer<u8>) -> PyResult<usize> {
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

        if buffer.readonly() {
            return Err(PyIOError::new_err("Buffer is not writable."));
        }

        if !buffer.is_c_contiguous() {
            return Err(PyIOError::new_err("Buffer is not C contiguous."));
        }

        Python::with_gil(|_py| {
            let ptr = buffer.buf_ptr();
            let nbytes = buffer.len_bytes();
            unsafe {
                let view: &mut [u8] = std::slice::from_raw_parts_mut(ptr as *mut u8, nbytes);
                let z = Read::read(reader, view)?;
                Ok(z)
            }
        })
    }

    /// Write bytes into the file.
    pub fn write(&mut self, bs: &[u8]) -> PyResult<usize> {
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
            .map(|_| bs.len())
            .map_err(|err| PyIOError::new_err(err.to_string()))
    }

    /// Change the stream position to the given byte offset.
    /// Offset is interpreted relative to the position indicated by `whence`.
    /// The default value for whence is `SEEK_SET`. Values for `whence` are:
    ///
    /// * `SEEK_SET` or `0` – start of the stream (the default); offset should be zero or positive
    /// * `SEEK_CUR` or `1` – current stream position; offset may be negative
    /// * `SEEK_END` or `2` – end of the stream; offset is usually negative
    ///
    /// Return the new absolute position.
    #[pyo3(signature = (pos, whence = 0))]
    pub fn seek(&mut self, pos: i64, whence: u8) -> PyResult<u64> {
        if !self.seekable()? {
            return Err(PyIOError::new_err(
                "Seek operation is not supported by the backing service.",
            ));
        }
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
        if let FileState::Writer(w) = &mut self.0 {
            w.close()
                .map_err(|err| PyIOError::new_err(err.to_string()))?;
        };
        self.0 = FileState::Closed;
        Ok(())
    }

    pub fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    pub fn __exit__(
        &mut self,
        _exc_type: PyObject,
        _exc_value: PyObject,
        _traceback: PyObject,
    ) -> PyResult<()> {
        self.close()
    }

    /// Flush the underlying writer. Is a no-op if the file is opened in reading mode.
    pub fn flush(&mut self) -> PyResult<()> {
        if matches!(self.0, FileState::Reader(_)) {
            Ok(())
        } else if let FileState::Writer(w) = &mut self.0 {
            match w.flush() {
                Ok(_) => Ok(()),
                Err(e) => Err(e.into()),
            }
        } else {
            Ok(())
        }
    }

    /// Return True if the stream can be read from.
    pub fn readable(&self) -> PyResult<bool> {
        Ok(matches!(self.0, FileState::Reader(_)))
    }

    /// Return True if the stream can be written to.
    pub fn writable(&self) -> PyResult<bool> {
        Ok(matches!(self.0, FileState::Writer(_)))
    }

    /// Return True if the stream can be repositioned.
    ///
    /// In OpenDAL this is limited to only *readable* streams.
    pub fn seekable(&self) -> PyResult<bool> {
        match &self.0 {
            FileState::Reader(_) => Ok(true),
            _ => Ok(false),
        }
    }

    /// Return True if the stream is closed.
    #[getter]
    pub fn closed(&self) -> PyResult<bool> {
        Ok(matches!(self.0, FileState::Closed))
    }
}

/// A file-like async reader.
/// Can be used as an async context manager.
#[pyclass(module = "opendal")]
pub struct AsyncFile(Arc<Mutex<AsyncFileState>>);

enum AsyncFileState {
    Reader(ocore::FuturesAsyncReader),
    Writer(ocore::Writer),
    Closed,
}

impl AsyncFile {
    pub fn new_reader(reader: ocore::FuturesAsyncReader) -> Self {
        Self(Arc::new(Mutex::new(AsyncFileState::Reader(reader))))
    }

    pub fn new_writer(writer: ocore::Writer) -> Self {
        Self(Arc::new(Mutex::new(AsyncFileState::Writer(writer))))
    }
}

#[pymethods]
impl AsyncFile {
    /// Read and return at most size bytes, or if size is not given, until EOF.
    #[pyo3(signature = (size=None))]
    pub fn read<'p>(&'p self, py: Python<'p>, size: Option<usize>) -> PyResult<Bound<'p, PyAny>> {
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
                    // TODO: optimize here by using uninit slice.
                    let mut bs = vec![0; size];
                    let n = reader
                        .read(&mut bs)
                        .await
                        .map_err(|err| PyIOError::new_err(err.to_string()))?;
                    bs.truncate(n);
                    bs
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

            Python::with_gil(|py| Buffer::new(buffer).into_bytes(py))
        })
    }

    /// Write bytes into the file.
    pub fn write<'p>(&'p mut self, py: Python<'p>, bs: &'p [u8]) -> PyResult<Bound<'p, PyAny>> {
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

            let len = bs.len();
            writer
                .write(bs)
                .await
                .map(|_| len)
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
    pub fn seek<'p>(
        &'p mut self,
        py: Python<'p>,
        pos: i64,
        whence: u8,
    ) -> PyResult<Bound<'p, PyAny>> {
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

            let pos = reader
                .seek(whence)
                .await
                .map_err(|err| PyIOError::new_err(err.to_string()))?;
            Ok(pos)
        })
        .and_then(|pos| pos.into_bound_py_any(py))
    }

    /// Return the current stream position.
    pub fn tell<'p>(&'p mut self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
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
            Ok(pos)
        })
        .and_then(|pos| pos.into_bound_py_any(py))
    }

    fn close<'p>(&'p mut self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let state = self.0.clone();
        future_into_py(py, async move {
            let mut state = state.lock().await;
            if let AsyncFileState::Writer(w) = &mut *state {
                w.close()
                    .await
                    .map_err(|err| PyIOError::new_err(err.to_string()))?;
            }
            *state = AsyncFileState::Closed;
            Ok(())
        })
    }

    fn __aenter__<'a>(slf: PyRef<'a, Self>, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let slf = slf.into_py_any(py)?;
        future_into_py(py, async move { Ok(slf) })
    }

    fn __aexit__<'a>(
        &'a mut self,
        py: Python<'a>,
        _exc_type: &Bound<'a, PyAny>,
        _exc_value: &Bound<'a, PyAny>,
        _traceback: &Bound<'a, PyAny>,
    ) -> PyResult<Bound<'a, PyAny>> {
        self.close(py)
    }

    /// Check if the stream may be read from.
    pub fn readable<'p>(&'p self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let state = self.0.clone();
        future_into_py(py, async move {
            let state = state.lock().await;
            Ok(matches!(*state, AsyncFileState::Reader(_)))
        })
    }

    /// Check if the stream may be written to.
    pub fn writable<'p>(&'p self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let state = self.0.clone();
        future_into_py(py, async move {
            let state = state.lock().await;
            Ok(matches!(*state, AsyncFileState::Writer(_)))
        })
    }

    /// Check if the stream reader may be re-located.
    pub fn seekable<'p>(&'p self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        if true {
            self.readable(py)
        } else {
            future_into_py(py, async move { Ok(false) })
        }
    }

    /// Check if the stream is closed.
    #[getter]
    pub fn closed<'p>(&'p self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let state = self.0.clone();
        future_into_py(py, async move {
            let state = state.lock().await;
            Ok(matches!(*state, AsyncFileState::Closed))
        })
    }
}
