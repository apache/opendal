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
use futures::AsyncWriteExt;
use pyo3::IntoPyObjectExt;
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::PyIOError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use tokio::sync::Mutex;

use crate::*;

/// A file-like object for reading and writing data.
///
/// Created by the `open` method of the `Operator` class.
#[gen_stub_pyclass]
#[pyclass(module = "opendal.file")]
pub struct File(FileState);

enum FileState {
    Reader(ocore::blocking::StdReader),
    Writer(ocore::blocking::StdWriter),
    Closed,
}

impl File {
    pub fn new_reader(reader: ocore::blocking::StdReader) -> Self {
        Self(FileState::Reader(reader))
    }

    pub fn new_writer(writer: ocore::blocking::Writer) -> Self {
        Self(FileState::Writer(writer.into_std_write()))
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl File {
    /// Read at most `size` bytes from this file.
    ///
    /// If `size` is not specified, read until EOF.
    ///
    /// Parameters
    /// ----------
    /// size : int, optional
    ///     The maximum number of bytes to read.
    ///
    /// Notes
    /// -----
    /// Fewer bytes may be returned than requested, read in a loop
    /// to ensure all bytes are read.
    ///
    /// Returns
    /// -------
    /// bytes
    ///     The bytes read from this file.
    #[gen_stub(override_return_type(type_repr = "builtins.bytes", imports=("builtins")))]
    #[pyo3(signature = (size=None))]
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

    /// Read one line from this file.
    ///
    /// If `size` is not specified, read until newline.
    ///
    /// Parameters
    /// ----------
    /// size : int, optional
    ///     The maximum number of bytes to read.
    ///
    /// Notes
    /// -----
    /// Retains newline characters after each line, unless
    /// the file’s last line has no terminating newline.
    ///
    /// Returns
    /// -------
    /// bytes
    ///     The bytes read from this file.
    #[gen_stub(override_return_type(type_repr = "builtins.bytes", imports=("builtins")))]
    #[pyo3(signature = (size=None))]
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

    /// Read bytes into a pre-allocated buffer.
    ///
    /// Parameters
    /// ----------
    /// buffer : bytes | bytearray
    ///     A writable, pre-allocated buffer to read into.
    ///
    /// Returns
    /// -------
    /// int
    ///     The number of bytes read.
    pub fn readinto(
        &mut self,
        #[gen_stub(override_type(type_repr = "builtins.bytes | builtins.bytearray", imports=("builtins")))]
        buffer: PyBuffer<u8>,
    ) -> PyResult<usize> {
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

        Python::attach(|_py| {
            let ptr = buffer.buf_ptr();
            let nbytes = buffer.len_bytes();
            unsafe {
                let view: &mut [u8] = std::slice::from_raw_parts_mut(ptr as *mut u8, nbytes);
                let z = Read::read(reader, view)?;
                Ok(z)
            }
        })
    }

    /// Write bytes to this file.
    ///
    /// Parameters
    /// ----------
    /// bs : bytes
    ///     The bytes to write to the file.
    ///
    /// Returns
    /// -------
    /// int
    ///     The number of bytes written.
    pub fn write(
        &mut self,
        #[gen_stub(override_type(type_repr = "builtins.bytes", imports=("builtins")))] bs: &[u8],
    ) -> PyResult<usize> {
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

    /// Change the position of this file to the given byte offset.
    ///
    /// Parameters
    /// ----------
    /// pos : int
    ///     The byte offset (position) to set.
    /// whence : int, optional
    ///     The reference point for the offset.
    ///     0: start of file (default); 1: current position; 2: end of file.
    ///
    /// Returns
    /// -------
    /// int
    ///     The new absolute position.
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

    /// Return the current position of this file.
    ///
    /// Returns
    /// -------
    /// int
    ///     The current absolute position.
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

    /// Close this file.
    ///
    /// This also flushes write buffers, if applicable.
    ///
    /// Notes
    /// -----
    /// A closed file cannot be used for further I/O operations.
    fn close(&mut self) -> PyResult<()> {
        if let FileState::Writer(w) = &mut self.0 {
            w.close().map_err(format_pyerr_from_io_error)?;
        };
        self.0 = FileState::Closed;
        Ok(())
    }

    pub fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[allow(unused_variables)]
    #[pyo3(signature = (exc_type, exc_value, traceback))]
    pub fn __exit__(
        &mut self,
        #[gen_stub(override_type(type_repr = "type[builtins.BaseException] | None", imports=("builtins")))]
        exc_type: Py<PyAny>,
        #[gen_stub(override_type(type_repr = "builtins.BaseException | None", imports=("builtins")))]
        exc_value: Py<PyAny>,
        #[gen_stub(override_type(type_repr = "types.TracebackType | None", imports=("types")))]
        traceback: Py<PyAny>,
    ) -> PyResult<()> {
        self.close()
    }

    /// Flush the underlying writer.
    ///
    /// Notes
    /// -----
    /// Is a no-op if the file is not `writable`.
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

    /// Whether this file can be read from.
    ///
    /// Returns
    /// -------
    /// bool
    ///     True if this file can be read from.
    pub fn readable(&self) -> PyResult<bool> {
        Ok(matches!(self.0, FileState::Reader(_)))
    }

    /// Whether this file can be written to.
    ///
    /// Returns
    /// -------
    /// bool
    ///     True if this file can be written to.
    pub fn writable(&self) -> PyResult<bool> {
        Ok(matches!(self.0, FileState::Writer(_)))
    }

    /// Whether this file can be repositioned.
    ///
    /// Notes
    /// -----
    /// This is only applicable to *readable* files.
    ///
    /// Returns
    /// -------
    /// bool
    ///     True if this file can be repositioned.
    pub fn seekable(&self) -> PyResult<bool> {
        match &self.0 {
            FileState::Reader(_) => Ok(true),
            _ => Ok(false),
        }
    }

    /// Whether this file is closed.
    ///
    /// Returns
    /// -------
    /// bool
    ///     True if this file is closed.
    #[getter]
    pub fn closed(&self) -> PyResult<bool> {
        Ok(matches!(self.0, FileState::Closed))
    }
}

/// An async file-like object for reading and writing data.
///
/// Created by the `open` method of the `AsyncOperator` class.
#[gen_stub_pyclass]
#[pyclass(module = "opendal.file")]
pub struct AsyncFile(Arc<Mutex<AsyncFileState>>);

enum AsyncFileState {
    Reader(ocore::FuturesAsyncReader),
    Writer(ocore::FuturesAsyncWriter),
    Closed,
}

impl AsyncFile {
    pub fn new_reader(reader: ocore::FuturesAsyncReader) -> Self {
        Self(Arc::new(Mutex::new(AsyncFileState::Reader(reader))))
    }

    pub fn new_writer(writer: ocore::FuturesAsyncWriter) -> Self {
        Self(Arc::new(Mutex::new(AsyncFileState::Writer(writer))))
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl AsyncFile {
    /// Read at most `size` bytes from this file asynchronously.
    ///
    /// If `size` is not specified, read until EOF.
    ///
    /// Parameters
    /// ----------
    /// size : int, optional
    ///     The maximum number of bytes to read.
    ///
    /// Notes
    /// -----
    /// Fewer bytes may be returned than requested, read in a loop
    /// to ensure all bytes are read.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that returns the bytes read from the stream.
    #[gen_stub(override_return_type(
        type_repr="collections.abc.Awaitable[builtins.bytes]",
        imports=("collections.abc", "builtins")
    ))]
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

            Python::attach(|py| Buffer::new(buffer).into_bytes(py))
        })
    }

    /// Write bytes to this file asynchronously.
    ///
    /// Parameters
    /// ----------
    /// bs : bytes
    ///     The bytes to write to the file.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that returns the number of bytes written.
    #[gen_stub(override_return_type(
        type_repr="collections.abc.Awaitable[builtins.int]",
        imports=("collections.abc", "builtins")
    ))]
    pub fn write<'p>(
        &'p mut self,
        py: Python<'p>,
        #[gen_stub(override_type(type_repr = "builtins.bytes", imports=("builtins")))]
        bs: &'p [u8],
    ) -> PyResult<Bound<'p, PyAny>> {
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
                .write_all(&bs)
                .await
                .map(|_| len)
                .map_err(|err| PyIOError::new_err(err.to_string()))
        })
    }

    /// Change the position of this file to the given byte offset.
    ///
    /// Parameters
    /// ----------
    /// pos : int
    ///     The byte offset (position) to set.
    /// whence : int, optional
    ///     The reference point for the offset.
    ///     0: start of file (default); 1: current position; 2: end of file.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that returns the current absolute position.
    #[gen_stub(override_return_type(
        type_repr="collections.abc.Awaitable[builtins.int]",
        imports=("collections.abc", "builtins")
    ))]
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

    /// Return the current position of this file.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that returns the current absolute position.
    #[gen_stub(override_return_type(
        type_repr="collections.abc.Awaitable[builtins.int]",
        imports=("collections.abc", "builtins")
    ))]
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

    /// Close this file.
    ///
    /// This also flushes write buffers, if applicable.
    ///
    /// Notes
    /// -----
    /// A closed file cannot be used for further I/O operations.
    #[gen_stub(override_return_type(
        type_repr="collections.abc.Awaitable[None]",
        imports=("collections.abc")
    ))]
    fn close<'p>(&'p mut self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let state = self.0.clone();
        future_into_py(py, async move {
            let mut state = state.lock().await;
            if let AsyncFileState::Writer(w) = &mut *state {
                w.close().await.map_err(format_pyerr_from_io_error)?;
            }
            *state = AsyncFileState::Closed;
            Ok(())
        })
    }

    #[gen_stub(override_return_type(type_repr="typing_extensions.Self", imports=("typing_extensions")))]
    fn __aenter__<'a>(slf: PyRef<'a, Self>, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let slf = slf.into_py_any(py)?;
        future_into_py(py, async move { Ok(slf) })
    }

    #[allow(unused_variables)]
    #[gen_stub(override_return_type(type_repr = "None"))]
    #[pyo3(signature = (exc_type, exc_value, traceback))]
    fn __aexit__<'a>(
        &'a mut self,
        py: Python<'a>,
        #[gen_stub(override_type(type_repr = "type[builtins.BaseException] | None", imports=("builtins")))]
        exc_type: &Bound<'a, PyAny>,
        #[gen_stub(override_type(type_repr = "builtins.BaseException | None", imports=("builtins")))]
        exc_value: &Bound<'a, PyAny>,
        #[gen_stub(override_type(type_repr = "types.TracebackType | None", imports=("types")))]
        traceback: &Bound<'a, PyAny>,
    ) -> PyResult<Bound<'a, PyAny>> {
        self.close(py)
    }

    /// Whether this file can be read from.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that returns True if this file can be read from.
    #[gen_stub(override_return_type(
        type_repr="collections.abc.Awaitable[builtins.bool]",
        imports=("collections.abc", "builtins")
    ))]
    pub fn readable<'p>(&'p self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let state = self.0.clone();
        future_into_py(py, async move {
            let state = state.lock().await;
            Ok(matches!(*state, AsyncFileState::Reader(_)))
        })
    }

    /// Whether this file can be written to.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that returns True if this file can be written to.
    #[gen_stub(override_return_type(
        type_repr="collections.abc.Awaitable[builtins.bool]",
        imports=("collections.abc", "builtins")
    ))]
    pub fn writable<'p>(&'p self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let state = self.0.clone();
        future_into_py(py, async move {
            let state = state.lock().await;
            Ok(matches!(*state, AsyncFileState::Writer(_)))
        })
    }

    /// Whether this file can be repositioned.
    ///
    /// Notes
    /// -----
    /// This is only applicable to *readable* files.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that returns True if this file can be repositioned.
    #[gen_stub(override_return_type(
        type_repr="collections.abc.Awaitable[builtins.bool]",
        imports=("collections.abc", "builtins")
    ))]
    pub fn seekable<'p>(&'p self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        if true {
            self.readable(py)
        } else {
            future_into_py(py, async move { Ok(false) })
        }
    }

    /// Whether this file is closed.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that returns True if this file is closed.
    #[gen_stub(override_return_type(
        type_repr="collections.abc.Awaitable[builtins.bool]",
        imports=("collections.abc", "builtins")
    ))]
    #[getter]
    pub fn closed<'p>(&'p self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let state = self.0.clone();
        future_into_py(py, async move {
            let state = state.lock().await;
            Ok(matches!(*state, AsyncFileState::Closed))
        })
    }
}
