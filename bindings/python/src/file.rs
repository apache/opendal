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
use std::io::{self, BufRead};
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use asyncband::mutex::Mutex;
use asyncband::mutex::OwnedMutexGuard;
use bytes::Buf;
use futures::AsyncSeekExt;
use futures::FutureExt;
use futures::SinkExt;
use pyo3::IntoPyObjectExt;
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::PyIOError;
use pyo3::exceptions::PyValueError;
use pyo3::types::PyBytes;

use crate::runtime::{check_runtime_process, future_into_py, get_runtime, runtime_is_inherited};
use crate::*;

/// A file-like object for reading and writing data.
///
/// Created by the `open` method of the `Operator` class.
///
/// Storage I/O releases the Python GIL while waiting. Concurrent operations on
/// the same file are rejected; use separate files for I/O from multiple threads.
#[pyclass(module = "opendal.file")]
pub struct File(FileState);

enum FileState {
    Reader(Box<ocore::blocking::StdReader>),
    Writer(ocore::BufferSink),
    Closed,
}

impl File {
    pub fn new_reader(reader: ocore::blocking::StdReader) -> Self {
        Self(FileState::Reader(Box::new(reader)))
    }

    pub fn new_writer(writer: ocore::Writer) -> Self {
        Self(FileState::Writer(writer.into_sink()))
    }
}

impl Drop for File {
    fn drop(&mut self) {
        if matches!(self.0, FileState::Closed) {
            return;
        }
        let state = std::mem::replace(&mut self.0, FileState::Closed);
        if runtime_is_inherited() {
            // Dropping a blocking reader or pending write can touch Tokio.
            std::mem::forget(state);
        } else if matches!(state, FileState::Writer(_)) {
            // Pending operations can own runtime resources even without close.
            let runtime = get_runtime().expect("runtime belongs to this process");
            let _guard = runtime.enter();
            drop(state);
        }
    }
}
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
    #[pyo3(signature = (size=None) -> "bytes")]
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

        check_runtime_process()?;
        let buffer = py
            .detach(|| match size {
                Some(size) => reader.read_buffer(size),
                None => reader.read_to_end_buffer(),
            })
            .map_err(|err| PyIOError::new_err(err.to_string()))?;

        buffer_into_py_bytes(py, buffer).map(Bound::into_any)
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
    #[pyo3(signature = (size=None) -> "bytes")]
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

        check_runtime_process()?;
        let buffer = py
            .detach(|| -> std::io::Result<Vec<u8>> {
                let mut buffer = Vec::new();
                match size {
                    None => reader.read_until(b'\n', &mut buffer)?,
                    Some(size) => reader.take(size as u64).read_until(b'\n', &mut buffer)?,
                };
                Ok(buffer)
            })
            .map_err(|err| PyIOError::new_err(err.to_string()))?;

        buffer_into_py_bytes(py, buffer.into()).map(Bound::into_any)
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
    #[pyo3(signature = (buffer: "bytearray | memoryview"))]
    pub fn readinto(&mut self, py: Python<'_>, buffer: PyBuffer<u8>) -> PyResult<usize> {
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

        check_runtime_process()?;
        let size = buffer.len_bytes();
        if size == 0 {
            return Ok(0);
        }
        // Wait using owned storage, without exposing the Python buffer to I/O.
        let data = py.detach(|| reader.read_buffer(size))?;
        let len = data.len();
        if len == 0 {
            return Ok(0);
        }
        let target = buffer
            .as_slice_ptr()
            .ok_or_else(|| PyIOError::new_err("Buffer pointer is null or not C contiguous."))?;
        assert!(len <= target.len());
        let mut offset = 0;
        for chunk in data {
            // SAFETY: the buffer is writable, C-contiguous, and `as_slice_ptr`
            // covers `target.len()` elements with `len <= target.len()`, so
            // writing `chunk.len()` bytes at `offset` stays within the buffer.
            // `data` is owned storage that cannot overlap the Python buffer.
            // Callers must not mutate or read the same buffer from another
            // thread while `readinto` runs (possible under free-threaded
            // Python). Only the prefix holding `len` bytes is overwritten; the
            // buffer tail is left untouched.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    chunk.as_ptr(),
                    target.as_ptr().cast::<u8>().add(offset),
                    chunk.len(),
                );
            }
            offset += chunk.len();
        }
        Ok(len)
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
    #[pyo3(signature = (bs: "bytes"))]
    pub fn write(&mut self, py: Python<'_>, bs: &Bound<PyBytes>) -> PyResult<usize> {
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

        let bs = py_bytes_like_into_buffer(bs.as_any())?;
        let len = bs.len();
        if len == 0 {
            return Ok(0);
        }
        let runtime = get_runtime()?;
        py.detach(|| runtime.handle().block_on(writer.send(bs)))
            .map(|_| len)
            .map_err(format_pyerr_from_write_error)
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
    /// Notes
    /// -----
    /// Unbounded readable files allow non-negative positions beyond EOF. Reads
    /// from those positions return empty bytes when the backing service reports
    /// an unsatisfied unbounded range. Other service errors propagate. Explicit
    /// bounded views reject positions beyond their range.
    ///
    /// Returns
    /// -------
    /// int
    ///     The new absolute position.
    #[pyo3(signature = (pos, whence = 0))]
    pub fn seek(&mut self, py: Python<'_>, pos: i64, whence: u8) -> PyResult<u64> {
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
            0 => SeekFrom::Start(
                u64::try_from(pos)
                    .map_err(|_| PyValueError::new_err(format!("negative seek value {pos}")))?,
            ),
            1 => SeekFrom::Current(pos),
            2 => SeekFrom::End(pos),
            _ => return Err(PyValueError::new_err("invalid whence")),
        };

        check_runtime_process()?;
        py.detach(|| reader.seek(whence))
            .map_err(|err| PyIOError::new_err(err.to_string()))
    }

    /// Return the current position of this file.
    ///
    /// In write mode, the position is the number of bytes accepted by the
    /// writer, including buffered bytes.
    ///
    /// Returns
    /// -------
    /// int
    ///     The current absolute position.
    ///
    /// Raises
    /// ------
    /// ValueError
    ///     If the file is closed.
    pub fn tell(&mut self, py: Python<'_>) -> PyResult<u64> {
        let reader = match &mut self.0 {
            FileState::Reader(r) => r,
            FileState::Writer(w) => return Ok(w.position()),
            FileState::Closed => {
                return Err(PyValueError::new_err("I/O operation on closed file."));
            }
        };

        check_runtime_process()?;
        py.detach(|| reader.stream_position())
            .map_err(|err| PyIOError::new_err(err.to_string()))
    }

    /// Close this file.
    ///
    /// This also flushes write buffers, if applicable.
    ///
    /// Notes
    /// -----
    /// A closed file cannot be used for further I/O operations.
    fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        let runtime = get_runtime()?;
        py.detach(|| {
            let _guard = runtime.enter();
            if let FileState::Writer(w) = &mut self.0 {
                runtime.handle().block_on(w.close())?;
            }
            self.0 = FileState::Closed;
            Ok(())
        })
        .map_err(format_pyerr)
    }

    pub fn __enter__(slf: PyRef<'_, Self>) -> Py<Self> {
        slf.into()
    }

    #[allow(unused_variables)]
    #[pyo3(signature = (
        exc_type: "type[BaseException] | None",
        exc_value: "BaseException | None",
        traceback: "types.TracebackType | None"))]
    pub fn __exit__(
        &mut self,
        py: Python<'_>,
        exc_type: Py<PyAny>,
        exc_value: Py<PyAny>,
        traceback: Py<PyAny>,
    ) -> PyResult<()> {
        self.close(py)
    }

    /// Flush the underlying writer.
    ///
    /// Notes
    /// -----
    /// Passes pending input to the core writer. Storage chunks can remain
    /// buffered until more data arrives or `close()` completes the write.
    /// Is a no-op if the file is not `writable`.
    pub fn flush(&mut self, py: Python<'_>) -> PyResult<()> {
        if matches!(self.0, FileState::Reader(_)) {
            Ok(())
        } else if let FileState::Writer(w) = &mut self.0 {
            let runtime = get_runtime()?;
            match py.detach(|| runtime.handle().block_on(w.flush())) {
                Ok(_) => Ok(()),
                Err(e) => Err(format_pyerr_from_write_error(e)),
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
#[pyclass(module = "opendal.file")]
pub struct AsyncFile(Arc<Mutex<AsyncFileState>>);

enum AsyncFileState {
    Reader(Box<AsyncFileReader>),
    Writer(ocore::BufferSink),
    Closed,
}

impl AsyncFileState {
    fn reader_mut(&mut self) -> PyResult<&mut AsyncFileReader> {
        match self {
            Self::Reader(reader) => Ok(reader),
            Self::Writer(_) => Err(PyIOError::new_err(
                "I/O operation failed for reading on write only file.",
            )),
            Self::Closed => Err(PyIOError::new_err(
                "I/O operation failed for reading on closed file.",
            )),
        }
    }
}

struct AsyncFileReader {
    inner: ocore::FuturesAsyncReader,
    // The core cursor has advanced past these bytes. Keep them until asyncio
    // accepts the result so a cancelled buffered read can be resumed.
    unread: ocore::Buffer,
    deferred_error: Option<io::Error>,
}

impl AsyncFileReader {
    async fn read(&mut self, size: Option<usize>) -> io::Result<ocore::Buffer> {
        if size == Some(0) {
            return self.inner.read_buffer(0).await;
        }
        if self.unread.is_empty() {
            if let Some(err) = self.deferred_error.take() {
                return Err(err);
            }
            return match size {
                Some(size) => self.inner.read_buffer(size).await,
                None => self.inner.read_to_end_buffer().await,
            };
        }

        if let Some(size) = size {
            return Ok(self.unread.split_to(size.min(self.unread.len())));
        }
        if self.deferred_error.is_some() {
            return Ok(std::mem::take(&mut self.unread));
        }
        let rest = self.inner.read_to_end_buffer().await?;
        Ok(Iterator::chain(std::mem::take(&mut self.unread), rest).collect())
    }

    async fn buffer_ready(&mut self, size: usize) {
        if self.unread.is_empty() && self.deferred_error.is_none() {
            match self.inner.read_buffer(size).await {
                Ok(first) => self.unread = first,
                Err(err) => self.deferred_error = Some(err),
            }
        }
        if self.unread.is_empty() || self.deferred_error.is_some() {
            return;
        }

        // A pending poll leaves the stream in the core reader. Never suspend
        // after consuming the prefix, even if the next buffer is delayed.
        let mut len = self.unread.len();
        let mut parts = Vec::new();
        while len < size {
            match self.inner.read_buffer(size - len).now_or_never() {
                Some(Ok(next)) if !next.is_empty() => {
                    len += next.len();
                    if parts.is_empty() {
                        parts.extend(std::mem::take(&mut self.unread));
                    }
                    parts.extend(next);
                }
                Some(Err(err)) => {
                    self.deferred_error = Some(err);
                    break;
                }
                _ => break,
            }
        }
        if !parts.is_empty() {
            self.unread = parts.into();
        }
    }

    async fn position(&mut self) -> io::Result<u64> {
        Ok(self.inner.stream_position().await? - self.unread.len() as u64)
    }

    async fn seek(&mut self, mut whence: SeekFrom) -> io::Result<u64> {
        if let SeekFrom::Current(offset) = whence
            && !self.unread.is_empty()
        {
            let pos = self.position().await?;
            let target = pos.checked_add_signed(offset).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid seek to a negative or overflowing position",
                )
            })?;
            whence = SeekFrom::Start(target);
        }
        let pos = self.inner.seek(whence).await?;
        self.unread = ocore::Buffer::new();
        self.deferred_error = None;
        Ok(pos)
    }
}

#[derive(Default)]
enum BufferedReadDelivery {
    #[default]
    Waiting,
    Ready(OwnedMutexGuard<AsyncFileState>, usize),
    Finished,
}

#[pyclass]
struct BufferedReadDone(Arc<StdMutex<BufferedReadDelivery>>);

#[pymethods]
impl BufferedReadDone {
    fn __call__(&self, future: &Bound<PyAny>) -> PyResult<()> {
        let cancelled = future.call_method0("cancelled")?.is_truthy()?;
        let delivery =
            std::mem::replace(&mut *self.0.lock().unwrap(), BufferedReadDelivery::Finished);
        if let BufferedReadDelivery::Ready(mut guard, len) = delivery
            && !cancelled
        {
            let reader = guard.reader_mut()?;
            if len == 0 {
                reader.deferred_error = None;
            } else if len == reader.unread.len() {
                reader.unread = ocore::Buffer::new();
            } else {
                reader.unread.advance(len);
            }
        }
        Ok(())
    }
}

impl AsyncFile {
    pub fn new_reader(reader: ocore::FuturesAsyncReader) -> Self {
        Self(Arc::new(Mutex::new(AsyncFileState::Reader(Box::new(
            AsyncFileReader {
                inner: reader,
                unread: ocore::Buffer::new(),
                deferred_error: None,
            },
        )))))
    }

    pub fn new_writer(writer: ocore::Writer) -> Self {
        Self(Arc::new(Mutex::new(AsyncFileState::Writer(
            writer.into_sink(),
        ))))
    }
}
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
    #[pyo3(signature = (size=None) -> "collections.abc.Awaitable[bytes]")]
    pub fn read<'p>(&'p self, py: Python<'p>, size: Option<usize>) -> PyResult<Bound<'p, PyAny>> {
        let state = self.0.clone();

        future_into_py(py, async move {
            let mut guard = state.lock().await;
            let buffer = guard
                .reader_mut()?
                .read(size)
                .await
                .map_err(|err| PyIOError::new_err(err.to_string()))?;

            Python::attach(|py| buffer_into_py_bytes(py, buffer).map(Bound::unbind))
        })
    }

    /// Read up to `size` bytes by combining immediately available data.
    ///
    /// A positive-sized read waits for the first bytes, then returns when
    /// more data would require waiting, at EOF, or at the requested limit.
    /// Keep reading until an empty result to consume the file. A zero-sized
    /// read returns empty bytes without consuming data or a pending error.
    ///
    /// If a read encounters an error after collecting bytes, it returns those
    /// bytes and the next positive-sized read raises the error. A successful
    /// seek clears a pending error. Cancelling a pending read preserves the
    /// file position and any bytes collected for that read.
    ///
    /// Parameters
    /// ----------
    /// size : int
    ///     The maximum number of bytes to read. Must be non-negative.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that returns the bytes read from the stream.
    #[pyo3(signature = (size) -> "collections.abc.Awaitable[bytes]")]
    pub fn read_buffered<'p>(&'p self, py: Python<'p>, size: usize) -> PyResult<Bound<'p, PyAny>> {
        if size == 0 {
            return self.read(py, Some(0));
        }
        let state = self.0.clone();
        let delivery = Arc::new(StdMutex::new(BufferedReadDelivery::Waiting));
        let callback = BufferedReadDone(delivery.clone());
        let future = future_into_py(py, async move {
            let mut guard = state.lock_owned().await;
            let reader = guard.reader_mut()?;
            reader.buffer_ready(size).await;

            let len = size.min(reader.unread.len());
            let result = if len == 0 {
                if let Some(err) = &reader.deferred_error {
                    Err(PyIOError::new_err(err.to_string()))
                } else {
                    return Python::attach(|py| {
                        buffer_into_py_bytes(py, ocore::Buffer::new()).map(Bound::unbind)
                    });
                }
            } else {
                let mut buffer = reader.unread.clone();
                buffer.truncate(len);
                Python::attach(|py| buffer_into_py_bytes(py, buffer).map(Bound::unbind))
            };

            // Keep the file locked until Python either accepts or cancels this
            // result. If the cancellation callback ran first, leave the data
            // available for the next operation and release the lock here.
            if len == 0 || result.is_ok() {
                let mut delivery = delivery.lock().unwrap();
                if matches!(*delivery, BufferedReadDelivery::Waiting) {
                    *delivery = BufferedReadDelivery::Ready(guard, len);
                }
            }
            result
        })?;
        future.call_method1("add_done_callback", (callback,))?;
        Ok(future)
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
    #[pyo3(signature = (bs: "bytes") -> "collections.abc.Awaitable[int]")]
    pub fn write<'p>(&'p self, py: Python<'p>, bs: &Bound<PyBytes>) -> PyResult<Bound<'p, PyAny>> {
        let state = self.0.clone();
        let bs = py_bytes_like_into_buffer(bs.as_any())?;

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
            if len == 0 {
                return Ok(0);
            }
            writer
                .send(bs)
                .await
                .map(|_| len)
                .map_err(format_pyerr_from_write_error)
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
    /// Notes
    /// -----
    /// Unbounded readable files allow non-negative positions beyond EOF. Reads
    /// from those positions return empty bytes when the backing service reports
    /// an unsatisfied unbounded range. Other service errors propagate. Explicit
    /// bounded views reject positions beyond their range.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that returns the current absolute position.
    #[pyo3(signature = (pos, whence = 0) -> "collections.abc.Awaitable[int]")]
    pub fn seek<'p>(&'p self, py: Python<'p>, pos: i64, whence: u8) -> PyResult<Bound<'p, PyAny>> {
        let state = self.0.clone();

        let whence = match whence {
            0 => SeekFrom::Start(
                u64::try_from(pos)
                    .map_err(|_| PyValueError::new_err(format!("negative seek value {pos}")))?,
            ),
            1 => SeekFrom::Current(pos),
            2 => SeekFrom::End(pos),
            _ => return Err(PyValueError::new_err("invalid whence")),
        };

        future_into_py(py, async move {
            let mut guard = state.lock().await;
            guard
                .reader_mut()?
                .seek(whence)
                .await
                .map_err(|err| PyIOError::new_err(err.to_string()))
        })
        .and_then(|pos| pos.into_bound_py_any(py))
    }

    /// Return the current position of this file.
    ///
    /// In write mode, the position is the number of bytes accepted by the
    /// writer, including buffered bytes.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that returns the current absolute position.
    ///
    /// Raises
    /// ------
    /// ValueError
    ///     If the file is closed.
    #[pyo3(signature = () -> "collections.abc.Awaitable[int]")]
    pub fn tell<'p>(&'p self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let state = self.0.clone();

        future_into_py(py, async move {
            let mut guard = state.lock().await;
            let reader = match guard.deref_mut() {
                AsyncFileState::Reader(r) => r,
                AsyncFileState::Writer(w) => return Ok(w.position()),
                AsyncFileState::Closed => {
                    return Err(PyValueError::new_err("I/O operation on closed file."));
                }
            };

            reader
                .position()
                .await
                .map_err(|err| PyIOError::new_err(err.to_string()))
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
    #[pyo3(signature = () -> "collections.abc.Awaitable[None]")]
    fn close<'p>(&'p self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let state = self.0.clone();
        future_into_py(py, async move {
            let mut state = state.lock().await;
            if let AsyncFileState::Writer(w) = &mut *state {
                w.close().await.map_err(format_pyerr)?;
            }
            *state = AsyncFileState::Closed;
            Ok(())
        })
    }

    #[pyo3(signature = () -> "collections.abc.Awaitable[typing.Self]")]
    fn __aenter__<'a>(slf: PyRef<'a, Self>, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let slf = slf.into_py_any(py)?;
        future_into_py(py, async move { Ok(slf) })
    }

    #[allow(unused_variables)]
    #[pyo3(signature = (
        exc_type: "type[BaseException] | None",
        exc_value: "BaseException | None",
        traceback: "types.TracebackType | None") -> "collections.abc.Awaitable[None]")]
    fn __aexit__<'a>(
        &'a self,
        py: Python<'a>,
        exc_type: &Bound<'a, PyAny>,
        exc_value: &Bound<'a, PyAny>,
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
    #[pyo3(signature = () -> "collections.abc.Awaitable[bool]")]
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
    #[pyo3(signature = () -> "collections.abc.Awaitable[bool]")]
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
    #[pyo3(signature = () -> "collections.abc.Awaitable[bool]")]
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
    #[getter]
    pub fn closed<'p>(&'p self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let state = self.0.clone();
        future_into_py(py, async move {
            let state = state.lock().await;
            Ok(matches!(*state, AsyncFileState::Closed))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use asyncband::semaphore::Semaphore;
    use futures::poll;
    use ocore::layers::ConcurrentLimitLayer;

    use super::*;

    #[tokio::test]
    async fn test_cancelled_send_keeps_owned_buffer() {
        Python::initialize();
        let (buffer, source_ptr) = Python::attach(|py| {
            let source = PyBytes::new(py, b"owned Python bytes");
            let source_ptr = source.as_bytes().as_ptr();
            let buffer = py_bytes_like_into_buffer(source.as_any()).unwrap();
            (buffer, source_ptr)
        });
        let semaphore = Arc::new(Semaphore::new(0));
        let op = ocore::Operator::via_iter("memory", [])
            .unwrap()
            .layer(ConcurrentLimitLayer::with_semaphore(semaphore.clone()));
        let file = AsyncFile::new_writer(op.writer_with("cancelled").chunk(4).await.unwrap());
        let mut state = file.0.lock().await;
        let AsyncFileState::Writer(sink) = &mut *state else {
            unreachable!();
        };

        // Poll into an underlying write blocked on the semaphore, then drop
        // the caller's future. The sink must retain both the future and input.
        {
            let send = sink.send(buffer);
            futures::pin_mut!(send);
            assert!(poll!(send).is_pending());
        }
        semaphore.release(1);
        sink.close().await.unwrap();
        drop(state);
        drop(file);

        let result = op.read("cancelled").await.unwrap();
        assert_eq!(result.to_vec(), b"owned Python bytes");
        assert_eq!(result.current().as_ptr(), source_ptr);
    }

    struct RuntimeOwnedBytes(Arc<AtomicBool>);

    impl AsRef<[u8]> for RuntimeOwnedBytes {
        fn as_ref(&self) -> &[u8] {
            b"pending input"
        }
    }

    impl Drop for RuntimeOwnedBytes {
        fn drop(&mut self) {
            self.0.store(
                tokio::runtime::Handle::try_current().is_ok(),
                Ordering::SeqCst,
            );
        }
    }

    #[test]
    fn test_unclosed_file_drops_pending_input_in_runtime() {
        let op = ocore::Operator::via_iter("memory", []).unwrap();
        let runtime = get_runtime().unwrap();
        let writer = runtime
            .block_on(async { op.writer_with("pending").chunk(256 * 1024).await })
            .unwrap();
        let dropped_in_runtime = Arc::new(AtomicBool::new(false));
        let data = bytes::Bytes::from_owner(RuntimeOwnedBytes(dropped_in_runtime.clone()));
        let mut file = File::new_writer(writer);
        let FileState::Writer(sink) = &mut file.0 else {
            unreachable!();
        };
        runtime.block_on(sink.send(data.into())).unwrap();
        assert!(!dropped_in_runtime.load(Ordering::SeqCst));
        assert!(tokio::runtime::Handle::try_current().is_err());
        drop(file);
        assert!(dropped_in_runtime.load(Ordering::SeqCst));
    }
}
