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

// Suppress clippy::redundant_closure warning from pyo3 generated code
#![allow(clippy::redundant_closure)]

use std::collections::HashMap;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::str::FromStr;

use ::opendal as od;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::exceptions::PyFileNotFoundError;
use pyo3::exceptions::PyIOError;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::types::PyDict;

mod asyncio;
mod layers;

use crate::asyncio::*;

create_exception!(opendal, Error, PyException, "OpenDAL related errors");

fn add_layers(mut op: od::Operator, layers: Vec<layers::Layer>) -> PyResult<od::Operator> {
    for layer in layers {
        match layer {
            layers::Layer::Retry(layers::RetryLayer(inner)) => op = op.layer(inner),
            layers::Layer::ImmutableIndex(layers::ImmutableIndexLayer(inner)) => {
                op = op.layer(inner)
            }
            layers::Layer::ConcurrentLimit(layers::ConcurrentLimitLayer(inner)) => {
                op = op.layer(inner)
            }
        }
    }
    Ok(op)
}

fn build_operator(
    scheme: od::Scheme,
    map: HashMap<String, String>,
    layers: Vec<layers::Layer>,
) -> PyResult<od::Operator> {
    use od::services::*;

    let op = match scheme {
        od::Scheme::Azblob => od::Operator::from_map::<Azblob>(map)
            .map_err(format_pyerr)?
            .finish(),
        od::Scheme::Azdfs => od::Operator::from_map::<Azdfs>(map)
            .map_err(format_pyerr)?
            .finish(),
        od::Scheme::Fs => od::Operator::from_map::<Fs>(map)
            .map_err(format_pyerr)?
            .finish(),
        od::Scheme::Gcs => od::Operator::from_map::<Gcs>(map)
            .map_err(format_pyerr)?
            .finish(),
        od::Scheme::Ghac => od::Operator::from_map::<Ghac>(map)
            .map_err(format_pyerr)?
            .finish(),
        od::Scheme::Http => od::Operator::from_map::<Http>(map)
            .map_err(format_pyerr)?
            .finish(),
        od::Scheme::Ipmfs => od::Operator::from_map::<Ipmfs>(map)
            .map_err(format_pyerr)?
            .finish(),
        od::Scheme::Memory => od::Operator::from_map::<Memory>(map)
            .map_err(format_pyerr)?
            .finish(),
        od::Scheme::Obs => od::Operator::from_map::<Obs>(map)
            .map_err(format_pyerr)?
            .finish(),
        od::Scheme::Oss => od::Operator::from_map::<Oss>(map)
            .map_err(format_pyerr)?
            .finish(),
        od::Scheme::S3 => od::Operator::from_map::<S3>(map)
            .map_err(format_pyerr)?
            .finish(),
        od::Scheme::Webdav => od::Operator::from_map::<Webdav>(map)
            .map_err(format_pyerr)?
            .finish(),
        od::Scheme::Webhdfs => od::Operator::from_map::<Webhdfs>(map)
            .map_err(format_pyerr)?
            .finish(),
        _ => Err(PyNotImplementedError::new_err(format!(
            "unsupported scheme `{scheme}`"
        )))?,
    };

    add_layers(op, layers)
}

/// `Operator` is the entry for all public blocking APIs
///
/// Create a new blocking `Operator` with the given `scheme` and options(`**kwargs`).
#[pyclass(module = "opendal")]
struct Operator(od::BlockingOperator);

#[pymethods]
impl Operator {
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

        Ok(Operator(build_operator(scheme, map, layers)?.blocking()))
    }

    /// Read the whole path into bytes.
    pub fn read<'p>(&'p self, py: Python<'p>, path: &str) -> PyResult<&'p PyAny> {
        self.0
            .read(path)
            .map_err(format_pyerr)
            .map(|res| PyBytes::new(py, &res).into())
    }

    /// Open a file-like reader for the given path.
    pub fn open_reader(&self, path: &str) -> PyResult<Reader> {
        self.0
            .reader(path)
            .map(|reader| Reader(Some(reader)))
            .map_err(format_pyerr)
    }

    /// Write bytes into given path.
    pub fn write(&self, path: &str, bs: Vec<u8>) -> PyResult<()> {
        self.0.write(path, bs).map_err(format_pyerr)
    }

    /// Get current path's metadata **without cache** directly.
    pub fn stat(&self, path: &str) -> PyResult<Metadata> {
        self.0.stat(path).map_err(format_pyerr).map(Metadata)
    }

    /// Create a dir at given path.
    ///
    /// # Notes
    ///
    /// To indicate that a path is a directory, it is compulsory to include
    /// a trailing / in the path. Failure to do so may result in
    /// `NotADirectory` error being returned by OpenDAL.
    ///
    /// # Behavior
    ///
    /// - Create on existing dir will succeed.
    /// - Create dir is always recursive, works like `mkdir -p`
    pub fn create_dir(&self, path: &str) -> PyResult<()> {
        self.0.create_dir(path).map_err(format_pyerr)
    }

    /// Delete given path.
    ///
    /// # Notes
    ///
    /// - Delete not existing error won't return errors.
    pub fn delete(&self, path: &str) -> PyResult<()> {
        self.0.delete(path).map_err(format_pyerr)
    }

    /// List current dir path.
    pub fn list(&self, path: &str) -> PyResult<BlockingLister> {
        Ok(BlockingLister(self.0.list(path).map_err(format_pyerr)?))
    }

    /// List dir in flat way.
    pub fn scan(&self, path: &str) -> PyResult<BlockingLister> {
        Ok(BlockingLister(self.0.scan(path).map_err(format_pyerr)?))
    }

    fn __repr__(&self) -> String {
        let info = self.0.info();
        let name = info.name();
        if name.is_empty() {
            format!("Operator(\"{}\", root=\"{}\")", info.scheme(), info.root())
        } else {
            format!(
                "Operator(\"{}\", root=\"{}\", name=\"{name}\")",
                info.scheme(),
                info.root()
            )
        }
    }
}

/// A file-like blocking reader.
/// Can be used as a context manager.
#[pyclass(module = "opendal")]
struct Reader(Option<od::BlockingReader>);

impl Reader {
    fn as_mut(&mut self) -> PyResult<&mut od::BlockingReader> {
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
        Ok(PyBytes::new(py, &buffer).into())
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

#[pyclass(unsendable, module = "opendal")]
struct BlockingLister(od::BlockingLister);

#[pymethods]
impl BlockingLister {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<PyObject>> {
        match slf.0.next() {
            Some(Ok(entry)) => Ok(Some(Entry(entry).into_py(slf.py()))),
            Some(Err(err)) => {
                let pyerr = format_pyerr(err);
                Err(pyerr)
            }
            None => Ok(None),
        }
    }
}

#[pyclass(module = "opendal")]
struct Entry(od::Entry);

#[pymethods]
impl Entry {
    /// Path of entry. Path is relative to operator's root.
    #[getter]
    pub fn path(&self) -> &str {
        self.0.path()
    }

    fn __str__(&self) -> &str {
        self.0.path()
    }

    fn __repr__(&self) -> String {
        format!("Entry({:?})", self.0.path())
    }
}

#[pyclass(module = "opendal")]
struct Metadata(od::Metadata);

#[pymethods]
impl Metadata {
    #[getter]
    pub fn content_disposition(&self) -> Option<&str> {
        self.0.content_disposition()
    }

    /// Content length of this entry.
    #[getter]
    pub fn content_length(&self) -> u64 {
        self.0.content_length()
    }

    /// Content MD5 of this entry.
    #[getter]
    pub fn content_md5(&self) -> Option<&str> {
        self.0.content_md5()
    }

    /// Content Type of this entry.
    #[getter]
    pub fn content_type(&self) -> Option<&str> {
        self.0.content_type()
    }

    /// ETag of this entry.
    #[getter]
    pub fn etag(&self) -> Option<&str> {
        self.0.etag()
    }

    /// mode represent this entry's mode.
    #[getter]
    pub fn mode(&self) -> EntryMode {
        EntryMode(self.0.mode())
    }
}

#[pyclass(module = "opendal")]
struct EntryMode(od::EntryMode);

#[pymethods]
impl EntryMode {
    /// Returns `True` if this is a file.
    pub fn is_file(&self) -> bool {
        self.0.is_file()
    }

    /// Returns `True` if this is a directory.
    pub fn is_dir(&self) -> bool {
        self.0.is_dir()
    }

    pub fn __repr__(&self) -> &'static str {
        match self.0 {
            od::EntryMode::FILE => "EntryMode.FILE",
            od::EntryMode::DIR => "EntryMode.DIR",
            od::EntryMode::Unknown => "EntryMode.UNKNOWN",
        }
    }
}

fn format_pyerr(err: od::Error) -> PyErr {
    use od::ErrorKind::*;
    match err.kind() {
        NotFound => PyFileNotFoundError::new_err(err.to_string()),
        _ => Error::new_err(err.to_string()),
    }
}

/// OpenDAL Python binding
///
/// ## Installation
///
/// ```bash
/// pip install opendal
/// ```
///
/// ## Usage
///
/// ```python
/// import opendal
///
/// op = opendal.Operator("fs", root="/tmp")
/// op.write("test.txt", b"Hello World")
/// print(op.read("test.txt"))
/// print(op.stat("test.txt").content_length)
/// ```
///
/// Or using the async API:
///
/// ```python
/// import asyncio
///
/// async def main():
/// op = opendal.AsyncOperator("fs", root="/tmp")
/// await op.write("test.txt", b"Hello World")
/// print(await op.read("test.txt"))
///
/// asyncio.run(main())
/// ```
#[pymodule]
fn _opendal(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Operator>()?;
    m.add_class::<Reader>()?;
    m.add_class::<AsyncOperator>()?;
    m.add_class::<AsyncReader>()?;
    m.add_class::<Entry>()?;
    m.add_class::<EntryMode>()?;
    m.add_class::<Metadata>()?;
    m.add("Error", py.get_type::<Error>())?;

    let layers = layers::create_submodule(py)?;
    m.add_submodule(layers)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("opendal.layers", layers)?;

    Ok(())
}
