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
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::types::PyDict;
use pyo3::types::PyTuple;
use pyo3_async_runtimes::tokio::future_into_py;

use crate::*;

fn build_operator(
    scheme: ocore::Scheme,
    map: HashMap<String, String>,
) -> PyResult<ocore::Operator> {
    let op = ocore::Operator::via_iter(scheme, map).map_err(format_pyerr)?;
    Ok(op)
}

fn build_blocking_operator(
    scheme: ocore::Scheme,
    map: HashMap<String, String>,
) -> PyResult<ocore::blocking::Operator> {
    let op = ocore::Operator::via_iter(scheme, map).map_err(format_pyerr)?;

    let runtime = pyo3_async_runtimes::tokio::get_runtime();
    let _guard = runtime.enter();
    let op = ocore::blocking::Operator::new(op).map_err(format_pyerr)?;
    Ok(op)
}

/// The blocking equivalent of `AsyncOperator`.
///
/// `Operator` is the entry point for all blocking APIs.
///
/// See also
/// --------
/// AsyncOperator
#[gen_stub_pyclass]
#[pyclass(module = "opendal.operator")]
pub struct Operator {
    core: ocore::blocking::Operator,
    __scheme: ocore::Scheme,
    __map: HashMap<String, String>,
}

#[gen_stub_pymethods]
#[pymethods]
impl Operator {
    /// Create a new blocking `Operator`.
    ///
    /// Parameters
    /// ----------
    /// scheme : str
    ///     The scheme of the service.
    /// **kwargs : dict
    ///     The options for the service.
    ///
    /// Returns
    /// -------
    /// Operator
    ///     The new operator.
    #[new]
    #[pyo3(signature = (scheme, **kwargs))]
    pub fn new(scheme: &str, kwargs: Option<&Bound<PyDict>>) -> PyResult<Self> {
        let scheme = ocore::Scheme::from_str(scheme)
            .map_err(|err| {
                ocore::Error::new(ocore::ErrorKind::Unexpected, "unsupported scheme")
                    .set_source(err)
            })
            .map_err(format_pyerr)?;
        let map = kwargs
            .map(|v| {
                v.extract::<HashMap<String, String>>()
                    .expect("must be valid hashmap")
            })
            .unwrap_or_default();

        Ok(Operator {
            core: build_blocking_operator(scheme, map.clone())?,
            __scheme: scheme,
            __map: map,
        })
    }

    /// Add a new layer to this operator.
    ///
    /// Parameters
    /// ----------
    /// layer : Layer
    ///     The layer to add.
    ///
    /// Returns
    /// -------
    /// Operator
    ///     A new operator with the layer added.
    pub fn layer(&self, layer: &layers::Layer) -> PyResult<Self> {
        let op = layer.0.layer(self.core.clone().into());

        let runtime = pyo3_async_runtimes::tokio::get_runtime();
        let _guard = runtime.enter();
        let op = ocore::blocking::Operator::new(op).map_err(format_pyerr)?;
        Ok(Self {
            core: op,
            __scheme: self.__scheme,
            __map: self.__map.clone(),
        })
    }

    /// Open a file-like object for the given path.
    ///
    /// The returning file-like object is a context manager.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     The path to the file.
    /// mode : str
    ///     The mode to open the file in. Only "rb" and "wb" are supported.
    /// **kwargs : dict
    ///     Additional options for the underlying reader or writer.
    ///
    /// Returns
    /// -------
    /// File
    ///     A file-like object.
    #[pyo3(signature = (path, mode, **kwargs))]
    pub fn open(
        &self,
        path: PathBuf,
        mode: String,
        kwargs: Option<&Bound<PyDict>>,
    ) -> PyResult<File> {
        let this = self.core.clone();
        let path = path.to_string_lossy().to_string();

        let reader_opts = kwargs
            .map(|v| v.extract::<ReadOptions>())
            .transpose()?
            .unwrap_or_default();

        let writer_opts = kwargs
            .map(|v| v.extract::<WriteOptions>())
            .transpose()?
            .unwrap_or_default();

        if mode == "rb" {
            let range = reader_opts.make_range();
            let reader = this
                .reader_options(&path, reader_opts.into())
                .map_err(format_pyerr)?;

            let r = reader
                .into_std_read(range.to_range())
                .map_err(format_pyerr)?;
            Ok(File::new_reader(r))
        } else if mode == "wb" {
            let writer = this
                .writer_options(&path, writer_opts.into())
                .map_err(format_pyerr)?;
            Ok(File::new_writer(writer))
        } else {
            Err(Unsupported::new_err(format!(
                "OpenDAL doesn't support mode: {mode}"
            )))
        }
    }

    /// Read the entire contents of a file at the given path.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     The path to the file.
    /// **kwargs : ReadOptions
    ///     Additional options for the underlying reader.
    ///
    /// Returns
    /// -------
    /// bytes
    ///     The contents of the file.
    #[pyo3(signature = (path, **kwargs))]
    pub fn read<'p>(
        &'p self,
        py: Python<'p>,
        path: PathBuf,
        #[gen_stub(override_type(type_repr = "builtins.dict[builtins.str, typing.Any]", imports=("builtins", "typing")))]
        kwargs: Option<ReadOptions>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let path = path.to_string_lossy().to_string();
        let kwargs = kwargs.unwrap_or_default();
        let buffer = self
            .core
            .read_options(&path, kwargs.into())
            .map_err(format_pyerr)?
            .to_vec();

        Buffer::new(buffer).into_bytes_ref(py)
    }

    /// Write bytes to a file at the given path.
    ///
    /// This function will create a file if it does not exist, and will
    /// overwrite its contents if it does.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     The path to the file.
    /// bs : bytes
    ///     The contents to write to the file.
    /// **kwargs : WriteOptions
    ///     Additional options for the underlying writer.
    #[pyo3(signature = (path, bs, **kwargs))]
    pub fn write(
        &self,
        path: PathBuf,
        bs: Vec<u8>,
        #[gen_stub(override_type(type_repr = "builtins.dict[builtins.str, typing.Any]", imports=("builtins", "typing")))]
        kwargs: Option<WriteOptions>,
    ) -> PyResult<()> {
        let path = path.to_string_lossy().to_string();
        let kwargs = kwargs.unwrap_or_default();
        self.core
            .write_options(&path, bs, kwargs.into())
            .map(|_| ())
            .map_err(format_pyerr)
    }

    /// Get the metadata of a file at the given path.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     The path to the file.
    /// **kwargs : StatOptions
    ///     Additional options for the underlying stat operation.
    ///
    /// Returns
    /// -------
    /// Metadata
    ///     The metadata of the file.
    #[pyo3(signature = (path, **kwargs))]
    pub fn stat(&self, path: PathBuf, kwargs: Option<&Bound<PyDict>>) -> PyResult<Metadata> {
        let path = path.to_string_lossy().to_string();
        let kwargs = kwargs
            .map(|v| v.extract::<StatOptions>())
            .transpose()?
            .unwrap_or_default();
        self.core
            .stat_options(&path, kwargs.into())
            .map_err(format_pyerr)
            .map(Metadata::new)
    }

    /// Copy a file from one path to another.
    ///
    /// Parameters
    /// ----------
    /// source : str
    ///     The path to the source file.
    /// target : str
    ///     The path to the target file.
    pub fn copy(&self, source: PathBuf, target: PathBuf) -> PyResult<()> {
        let source = source.to_string_lossy().to_string();
        let target = target.to_string_lossy().to_string();
        self.core.copy(&source, &target).map_err(format_pyerr)
    }

    /// Rename (move) a file from one path to another.
    ///
    /// Parameters
    /// ----------
    /// source : str
    ///     The path to the source file.
    /// target : str
    ///     The path to the target file.
    pub fn rename(&self, source: PathBuf, target: PathBuf) -> PyResult<()> {
        let source = source.to_string_lossy().to_string();
        let target = target.to_string_lossy().to_string();
        self.core.rename(&source, &target).map_err(format_pyerr)
    }

    /// Recursively remove all files and directories at the given path.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     The path to remove.
    pub fn remove_all(&self, path: PathBuf) -> PyResult<()> {
        let path = path.to_string_lossy().to_string();
        self.core.remove_all(&path).map_err(format_pyerr)
    }

    /// Create a directory at the given path.
    ///
    /// Notes
    /// -----
    /// To indicate that a path is a directory, it must end with a `/`.
    /// This operation is always recursive, like `mkdir -p`.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     The path to the directory.
    pub fn create_dir(&self, path: PathBuf) -> PyResult<()> {
        let path = path.to_string_lossy().to_string();
        self.core.create_dir(&path).map_err(format_pyerr)
    }

    /// Delete a file at the given path.
    ///
    /// Notes
    /// -----
    /// This operation will not return an error if the path does not exist.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     The path to the file.
    pub fn delete(&self, path: PathBuf) -> PyResult<()> {
        let path = path.to_string_lossy().to_string();
        self.core.delete(&path).map_err(format_pyerr)
    }

    /// Check if a path exists.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     The path to check.
    ///
    /// Returns
    /// -------
    /// bool
    ///     True if the path exists, False otherwise.
    pub fn exists(&self, path: PathBuf) -> PyResult<bool> {
        let path = path.to_string_lossy().to_string();
        self.core.exists(&path).map_err(format_pyerr)
    }

    /// List entries in the given directory.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     The path to the directory.
    /// **kwargs : ListOptions
    ///     Additional options for the underlying list operation.
    ///
    /// Returns
    /// -------
    /// BlockingLister
    ///     An iterator over the entries in the directory.
    #[pyo3(signature = (path, **kwargs))]
    pub fn list(&self, path: PathBuf, kwargs: Option<&Bound<PyDict>>) -> PyResult<BlockingLister> {
        let path = path.to_string_lossy().to_string();

        let kwargs = kwargs
            .map(|v| v.extract::<ListOptions>())
            .transpose()?
            .unwrap_or_default();

        let l = self
            .core
            .lister_options(&path, kwargs.into())
            .map_err(format_pyerr)?;
        Ok(BlockingLister::new(l))
    }

    /// Recursively list entries in the given directory.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     The path to the directory.
    /// **kwargs : ListOptions
    ///     Additional options for the underlying list operation.
    ///
    /// Returns
    /// -------
    /// BlockingLister
    ///     An iterator over the entries in the directory.
    ///
    /// Deprecated
    /// ----------
    ///     Use `list()` with `recursive=True` instead.
    #[gen_stub(skip)]
    #[pyo3(signature = (path, **kwargs))]
    pub fn scan<'p>(
        &self,
        py: Python<'p>,
        path: PathBuf,
        kwargs: Option<&Bound<PyDict>>,
    ) -> PyResult<BlockingLister> {
        let d = PyDict::new(py);
        let kwargs = kwargs.unwrap_or(&d);
        kwargs.set_item("recursive", true)?;

        self.list(path, Some(kwargs))
    }

    /// Get all capabilities of this operator.
    ///
    /// Returns
    /// -------
    /// Capability
    ///     The capability of the operator.
    pub fn capability(&self) -> PyResult<capability::Capability> {
        Ok(capability::Capability::new(
            self.core.info().full_capability(),
        ))
    }

    /// Check if the operator is able to work correctly.
    ///
    /// Raises
    /// ------
    /// Exception
    ///     If the operator is not able to work correctly.
    pub fn check(&self) -> PyResult<()> {
        self.core.check().map_err(format_pyerr)
    }

    /// Create a new `AsyncOperator` from this blocking operator.
    ///
    /// Returns
    /// -------
    /// AsyncOperator
    ///     The async operator.
    pub fn to_async_operator(&self) -> PyResult<AsyncOperator> {
        Ok(AsyncOperator {
            core: self.core.clone().into(),
            __scheme: self.__scheme,
            __map: self.__map.clone(),
        })
    }

    fn __repr__(&self) -> String {
        let info = self.core.info();
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

    #[gen_stub(skip)]
    fn __getnewargs_ex__(&self, py: Python) -> PyResult<Py<PyAny>> {
        let args = vec![self.__scheme.to_string()];
        let args = PyTuple::new(py, args)?.into_py_any(py)?;
        let kwargs = self.__map.clone().into_py_any(py)?;
        PyTuple::new(py, [args, kwargs])?.into_py_any(py)
    }
}

/// The async equivalent of `Operator`.
///
/// `AsyncOperator` is the entry point for all async APIs.
///
/// See also
/// --------
/// Operator
#[gen_stub_pyclass]
#[pyclass(module = "opendal.operator")]
pub struct AsyncOperator {
    core: ocore::Operator,
    __scheme: ocore::Scheme,
    __map: HashMap<String, String>,
}

#[gen_stub_pymethods]
#[pymethods]
impl AsyncOperator {
    /// Create a new `AsyncOperator`.
    ///
    /// Parameters
    /// ----------
    /// scheme : str
    ///     The scheme of the service.
    /// **kwargs : dict
    ///     The options for the service.
    ///
    /// Returns
    /// -------
    /// AsyncOperator
    ///     The new async operator.
    #[new]
    #[pyo3(signature = (scheme, **kwargs))]
    pub fn new(scheme: &str, kwargs: Option<&Bound<PyDict>>) -> PyResult<Self> {
        let scheme = ocore::Scheme::from_str(scheme)
            .map_err(|err| {
                ocore::Error::new(ocore::ErrorKind::Unexpected, "unsupported scheme")
                    .set_source(err)
            })
            .map_err(format_pyerr)?;
        let map = kwargs
            .map(|v| {
                v.extract::<HashMap<String, String>>()
                    .expect("must be valid hashmap")
            })
            .unwrap_or_default();

        Ok(AsyncOperator {
            core: build_operator(scheme, map.clone())?,
            __scheme: scheme,
            __map: map,
        })
    }

    /// Add a new layer to the operator.
    ///
    /// Parameters
    /// ----------
    /// layer : Layer
    ///     The layer to add.
    ///
    /// Returns
    /// -------
    /// AsyncOperator
    ///     A new operator with the layer added.
    pub fn layer(&self, layer: &layers::Layer) -> PyResult<Self> {
        let op = layer.0.layer(self.core.clone());
        Ok(Self {
            core: op,
            __scheme: self.__scheme,
            __map: self.__map.clone(),
        })
    }

    /// Open a file-like object for the given path.
    ///
    /// The returning file-like object is a context manager.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     The path to the file.
    /// mode : str
    ///     The mode to open the file in. Only "rb" and "wb" are supported.
    /// **kwargs : dict
    ///     Additional options for the underlying reader or writer.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that returns a file-like object.
    #[pyo3(signature = (path, mode, **kwargs))]
    pub fn open<'p>(
        &'p self,
        py: Python<'p>,
        path: PathBuf,
        mode: String,
        kwargs: Option<&Bound<PyDict>>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let this = self.core.clone();
        let path = path.to_string_lossy().to_string();

        let reader_opts = kwargs
            .map(|v| v.extract::<ReadOptions>())
            .transpose()?
            .unwrap_or_default();

        let writer_opts = kwargs
            .map(|v| v.extract::<WriteOptions>())
            .transpose()?
            .unwrap_or_default();

        future_into_py(py, async move {
            if mode == "rb" {
                let range = reader_opts.make_range();
                let reader = this
                    .reader_options(&path, reader_opts.into())
                    .await
                    .map_err(format_pyerr)?;

                let r = reader
                    .into_futures_async_read(range.to_range())
                    .await
                    .map_err(format_pyerr)?;
                Ok(AsyncFile::new_reader(r))
            } else if mode == "wb" {
                let writer = this
                    .writer_options(&path, writer_opts.into())
                    .await
                    .map_err(format_pyerr)?;
                let w = writer.into_futures_async_write();
                Ok(AsyncFile::new_writer(w))
            } else {
                Err(Unsupported::new_err(format!(
                    "OpenDAL doesn't support mode: {mode}"
                )))
            }
        })
    }

    /// Read the entire contents of a file at the given path.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     The path to the file.
    /// **kwargs : ReadOptions
    ///     Additional options for the underlying reader.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that returns the contents of the file as bytes.
    #[pyo3(signature = (path, **kwargs))]
    pub fn read<'p>(
        &'p self,
        py: Python<'p>,
        path: PathBuf,
        #[gen_stub(override_type(type_repr = "builtins.dict[builtins.str, typing.Any]", imports=("builtins", "typing")))]
        kwargs: Option<ReadOptions>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let this = self.core.clone();
        let path = path.to_string_lossy().to_string();
        let kwargs = kwargs.unwrap_or_default();
        future_into_py(py, async move {
            let range = kwargs.make_range();
            let res = this
                .reader_options(&path, kwargs.into())
                .await
                .map_err(format_pyerr)?
                .read(range.to_range())
                .await
                .map_err(format_pyerr)?
                .to_vec();
            Python::attach(|py| Buffer::new(res).into_bytes(py))
        })
    }

    /// Write bytes to a file at the given path.
    ///
    /// This function will create a file if it does not exist, and will
    /// overwrite its contents if it does.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     The path to the file.
    /// bs : bytes
    ///     The contents to write to the file.
    /// **kwargs : WriteOptions
    ///     Additional options for the underlying writer.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that completes when the write is finished.
    #[pyo3(signature = (path, bs, **kwargs))]
    pub fn write<'p>(
        &'p self,
        py: Python<'p>,
        path: PathBuf,
        bs: &Bound<PyBytes>,
        #[gen_stub(override_type(type_repr = "builtins.dict[builtins.str, typing.Any]", imports=("builtins", "typing")))]
        kwargs: Option<WriteOptions>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let mut kwargs = kwargs.unwrap_or_default();
        let this = self.core.clone();
        let bs = bs.as_bytes().to_vec();
        let path = path.to_string_lossy().to_string();
        future_into_py(py, async move {
            let mut write = this
                .write_with(&path, bs)
                .append(kwargs.append.unwrap_or(false));
            if let Some(buffer) = kwargs.chunk {
                write = write.chunk(buffer);
            }
            if let Some(content_type) = &kwargs.content_type {
                write = write.content_type(content_type);
            }
            if let Some(content_disposition) = &kwargs.content_disposition {
                write = write.content_disposition(content_disposition);
            }
            if let Some(cache_control) = &kwargs.cache_control {
                write = write.cache_control(cache_control);
            }
            if let Some(user_metadata) = kwargs.user_metadata.take() {
                write = write.user_metadata(user_metadata);
            }

            write.await.map(|_| ()).map_err(format_pyerr)
        })
    }

    /// Get the metadata of a file at the given path.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     The path to the file.
    /// **kwargs : StatOptions
    ///     Additional options for the underlying stat operation.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that returns the metadata of the file.
    #[pyo3(signature = (path, **kwargs))]
    pub fn stat<'p>(
        &'p self,
        py: Python<'p>,
        path: PathBuf,
        kwargs: Option<&Bound<PyDict>>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let this = self.core.clone();
        let path = path.to_string_lossy().to_string();
        let kwargs = kwargs
            .map(|v| v.extract::<StatOptions>())
            .transpose()?
            .unwrap_or_default();

        future_into_py(py, async move {
            let res: Metadata = this
                .stat_options(&path, kwargs.into())
                .await
                .map_err(format_pyerr)
                .map(Metadata::new)?;

            Ok(res)
        })
    }

    /// Copy a file from one path to another.
    ///
    /// Parameters
    /// ----------
    /// source : str
    ///     The path to the source file.
    /// target : str
    ///     The path to the target file.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that completes when the copy is finished.
    pub fn copy<'p>(
        &'p self,
        py: Python<'p>,
        source: PathBuf,
        target: PathBuf,
    ) -> PyResult<Bound<'p, PyAny>> {
        let this = self.core.clone();
        let source = source.to_string_lossy().to_string();
        let target = target.to_string_lossy().to_string();
        future_into_py(py, async move {
            this.copy(&source, &target).await.map_err(format_pyerr)
        })
    }

    /// Rename (move) a file from one path to another.
    ///
    /// Parameters
    /// ----------
    /// source : str
    ///     The path to the source file.
    /// target : str
    ///     The path to the target file.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that completes when the rename is finished.
    pub fn rename<'p>(
        &'p self,
        py: Python<'p>,
        source: PathBuf,
        target: PathBuf,
    ) -> PyResult<Bound<'p, PyAny>> {
        let this = self.core.clone();
        let source = source.to_string_lossy().to_string();
        let target = target.to_string_lossy().to_string();
        future_into_py(py, async move {
            this.rename(&source, &target).await.map_err(format_pyerr)
        })
    }

    /// Recursively remove all files and directories at the given path.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     The path to remove.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that completes when the removal is finished.
    pub fn remove_all<'p>(&'p self, py: Python<'p>, path: PathBuf) -> PyResult<Bound<'p, PyAny>> {
        let this = self.core.clone();
        let path = path.to_string_lossy().to_string();
        future_into_py(py, async move {
            this.remove_all(&path).await.map_err(format_pyerr)
        })
    }

    /// Check if the operator is able to work correctly.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that completes when the check is finished.
    ///
    /// Raises
    /// ------
    /// Exception
    ///     If the operator is not able to work correctly.
    pub fn check<'p>(&'p self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let this = self.core.clone();
        future_into_py(py, async move { this.check().await.map_err(format_pyerr) })
    }

    /// Create a directory at the given path.
    ///
    /// Notes
    /// -----
    /// To indicate that a path is a directory, it must end with a `/`.
    /// This operation is always recursive, like `mkdir -p`.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     The path to the directory.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that completes when the directory is created.
    pub fn create_dir<'p>(&'p self, py: Python<'p>, path: PathBuf) -> PyResult<Bound<'p, PyAny>> {
        let this = self.core.clone();
        let path = path.to_string_lossy().to_string();
        future_into_py(py, async move {
            this.create_dir(&path).await.map_err(format_pyerr)
        })
    }

    /// Delete a file at the given path.
    ///
    /// Notes
    /// -----
    /// This operation will not return an error if the path does not exist.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     The path to the file.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that completes when the file is deleted.
    pub fn delete<'p>(&'p self, py: Python<'p>, path: PathBuf) -> PyResult<Bound<'p, PyAny>> {
        let this = self.core.clone();
        let path = path.to_string_lossy().to_string();
        future_into_py(
            py,
            async move { this.delete(&path).await.map_err(format_pyerr) },
        )
    }

    /// Check if a path exists.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     The path to check.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that returns True if the path exists, False otherwise.
    pub fn exists<'p>(&'p self, py: Python<'p>, path: PathBuf) -> PyResult<Bound<'p, PyAny>> {
        let this = self.core.clone();
        let path = path.to_string_lossy().to_string();
        future_into_py(
            py,
            async move { this.exists(&path).await.map_err(format_pyerr) },
        )
    }

    /// List entries in the given directory.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     The path to the directory.
    /// **kwargs : ListOptions
    ///     Additional options for the underlying list operation.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that returns an async iterator over the entries.
    #[pyo3(signature = (path, **kwargs))]
    pub fn list<'p>(
        &'p self,
        py: Python<'p>,
        path: PathBuf,
        kwargs: Option<&Bound<PyDict>>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let this = self.core.clone();
        let path = path.to_string_lossy().to_string();
        let kwargs = kwargs
            .map(|v| v.extract::<ListOptions>())
            .transpose()?
            .unwrap_or_default();

        future_into_py(py, async move {
            let lister = this
                .lister_options(&path, kwargs.into())
                .await
                .map_err(format_pyerr)?;
            let pylister = Python::attach(|py| AsyncLister::new(lister).into_py_any(py))?;

            Ok(pylister)
        })
    }

    /// Recursively list entries in the given directory.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     The path to the directory.
    /// **kwargs : ListOptions
    ///     Additional options for the underlying list operation.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that returns an async iterator over the entries.
    ///
    /// Deprecated
    /// ----------
    ///     Use `list()` with `recursive=True` instead.
    #[gen_stub(skip)]
    #[pyo3(signature = (path, **kwargs))]
    pub fn scan<'p>(
        &'p self,
        py: Python<'p>,
        path: PathBuf,
        kwargs: Option<&Bound<PyDict>>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let d = PyDict::new(py);
        let kwargs = kwargs.unwrap_or(&d);
        kwargs.set_item("recursive", true)?;

        self.list(py, path, Some(kwargs))
    }

    /// Create a presigned request for a stat operation.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     The path of the object to stat.
    /// expire_second : int
    ///     The number of seconds until the presigned URL expires.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that returns a presigned request object.
    #[gen_stub(override_return_type(type_repr = "opendal.types.PresignedRequest", imports=("opendal.types")))]
    pub fn presign_stat<'p>(
        &'p self,
        py: Python<'p>,
        path: PathBuf,
        expire_second: u64,
    ) -> PyResult<Bound<'p, PyAny>> {
        let this = self.core.clone();
        let path = path.to_string_lossy().to_string();
        future_into_py(py, async move {
            let res = this
                .presign_stat(&path, Duration::from_secs(expire_second))
                .await
                .map_err(format_pyerr)
                .map(PresignedRequest)?;

            Ok(res)
        })
    }

    /// Create a presigned request for a read operation.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     The path of the object to read.
    /// expire_second : int
    ///     The number of seconds until the presigned URL expires.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that returns a presigned request object.
    #[gen_stub(override_return_type(type_repr = "opendal.types.PresignedRequest", imports=("opendal.types")))]
    pub fn presign_read<'p>(
        &'p self,
        py: Python<'p>,
        path: PathBuf,
        expire_second: u64,
    ) -> PyResult<Bound<'p, PyAny>> {
        let this = self.core.clone();
        let path = path.to_string_lossy().to_string();
        future_into_py(py, async move {
            let res = this
                .presign_read(&path, Duration::from_secs(expire_second))
                .await
                .map_err(format_pyerr)
                .map(PresignedRequest)?;

            Ok(res)
        })
    }

    /// Create a presigned request for a write operation.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     The path of the object to write to.
    /// expire_second : int
    ///     The number of seconds until the presigned URL expires.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that returns a presigned request object.
    #[gen_stub(override_return_type(type_repr = "opendal.types.PresignedRequest", imports=("opendal.types")))]
    pub fn presign_write<'p>(
        &'p self,
        py: Python<'p>,
        path: PathBuf,
        expire_second: u64,
    ) -> PyResult<Bound<'p, PyAny>> {
        let this = self.core.clone();
        let path = path.to_string_lossy().to_string();
        future_into_py(py, async move {
            let res = this
                .presign_write(&path, Duration::from_secs(expire_second))
                .await
                .map_err(format_pyerr)
                .map(PresignedRequest)?;

            Ok(res)
        })
    }

    /// Create a presigned request for a delete operation.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     The path of the object to delete.
    /// expire_second : int
    ///     The number of seconds until the presigned URL expires.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that returns a presigned request object.
    #[gen_stub(override_return_type(type_repr = "opendal.types.PresignedRequest", imports=("opendal.types")))]
    pub fn presign_delete<'p>(
        &'p self,
        py: Python<'p>,
        path: PathBuf,
        expire_second: u64,
    ) -> PyResult<Bound<'p, PyAny>> {
        let this = self.core.clone();
        let path = path.to_string_lossy().to_string();
        future_into_py(py, async move {
            let res = this
                .presign_delete(&path, Duration::from_secs(expire_second))
                .await
                .map_err(format_pyerr)
                .map(PresignedRequest)?;

            Ok(res)
        })
    }

    /// Get all capabilities of this operator.
    ///
    /// Returns
    /// -------
    /// Capability
    ///     The capability of the operator.
    pub fn capability(&self) -> PyResult<Capability> {
        Ok(capability::Capability::new(
            self.core.info().full_capability(),
        ))
    }

    /// Create a new blocking `Operator` from this async operator.
    ///
    /// Returns
    /// -------
    /// Operator
    ///     The blocking operator.
    pub fn to_operator(&self) -> PyResult<Operator> {
        let runtime = pyo3_async_runtimes::tokio::get_runtime();
        let _guard = runtime.enter();
        let op = ocore::blocking::Operator::new(self.core.clone()).map_err(format_pyerr)?;

        Ok(Operator {
            core: op,
            __scheme: self.__scheme,
            __map: self.__map.clone(),
        })
    }

    fn __repr__(&self) -> String {
        let info = self.core.info();
        let name = info.name();
        if name.is_empty() {
            format!(
                "AsyncOperator(\"{}\", root=\"{}\")",
                info.scheme(),
                info.root()
            )
        } else {
            format!(
                "AsyncOperator(\"{}\", root=\"{}\", name=\"{name}\")",
                info.scheme(),
                info.root()
            )
        }
    }

    #[gen_stub(skip)]
    fn __getnewargs_ex__(&self, py: Python) -> PyResult<Py<PyAny>> {
        let args = vec![self.__scheme.to_string()];
        let args = PyTuple::new(py, args)?.into_py_any(py)?;
        let kwargs = self.__map.clone().into_py_any(py)?;
        PyTuple::new(py, [args, kwargs])?.into_py_any(py)
    }
}

/// A presigned request.
///
/// This contains the information required to make a request to the
/// underlying service, including the URL, method, and headers.
#[gen_stub_pyclass]
#[pyclass(module = "opendal.types")]
pub struct PresignedRequest(ocore::raw::PresignedRequest);

#[gen_stub_pymethods]
#[pymethods]
impl PresignedRequest {
    /// The URL of this request.
    #[getter]
    pub fn url(&self) -> String {
        self.0.uri().to_string()
    }

    /// The HTTP method of this request.
    #[getter]
    pub fn method(&self) -> &str {
        self.0.method().as_str()
    }

    /// The HTTP headers of this request.
    ///
    /// Returns
    /// -------
    /// dict
    ///     The HTTP headers of this request.
    #[getter]
    pub fn headers(&self) -> PyResult<HashMap<&str, &str>> {
        let mut headers = HashMap::new();
        for (k, v) in self.0.header().iter() {
            let k = k.as_str();
            let v = v
                .to_str()
                .map_err(|err| Unexpected::new_err(err.to_string()))?;
            if headers.insert(k, v).is_some() {
                return Err(Unexpected::new_err("duplicate header"));
            }
        }
        Ok(headers)
    }
}
