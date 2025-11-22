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
    /// scheme : str | opendal.services.Scheme
    ///     The scheme of the service.
    /// **kwargs : dict
    ///     The options for the service.
    ///
    /// Returns
    /// -------
    /// Operator
    ///     The new operator.
    #[gen_stub(skip)]
    #[new]
    #[pyo3(signature = (scheme, *, **kwargs))]
    pub fn new(
        #[gen_stub(override_type(type_repr = "builtins.str | opendal.services.Scheme", imports=("builtins", "opendal.services")))]
        scheme: Bound<PyAny>,
        kwargs: Option<&Bound<PyDict>>,
    ) -> PyResult<Self> {
        let scheme = if let Ok(scheme_str) = scheme.extract::<&str>() {
            ocore::Scheme::from_str(scheme_str)
                .map_err(|err| {
                    ocore::Error::new(ocore::ErrorKind::Unexpected, "unsupported scheme")
                        .set_source(err)
                })
                .map_err(format_pyerr)
        } else if let Ok(py_scheme) = scheme.extract::<PyScheme>() {
            Ok(py_scheme.into())
        } else {
            Err(Unsupported::new_err(
                "Invalid type for scheme, expected str or Scheme",
            ))
        }?;
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
    /// **kwargs
    ///     Additional options for the underlying reader or writer.
    ///
    /// Returns
    /// -------
    /// File
    ///     A file-like object.
    #[pyo3(signature = (path, mode, *, **kwargs))]
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
    /// version : str, optional
    ///     The version of the file.
    /// concurrent : int, optional
    ///     The number of concurrent readers.
    /// chunk : int, optional
    ///     The size of each chunk.
    /// gap : int, optional
    ///     The gap between each chunk.
    /// offset : int, optional
    ///     The offset of the file.
    /// prefetch : int, optional
    ///     The number of bytes to prefetch.
    /// size : int, optional
    ///     The size of the file.
    /// if_match : str, optional
    ///     The ETag of the file.
    /// if_none_match : str, optional
    ///     The ETag of the file.
    /// if_modified_since : str, optional
    ///     The last modified time of the file.
    /// if_unmodified_since : str, optional
    ///     The last modified time of the file.
    /// content_type : str, optional
    ///     The content type of the file.
    /// cache_control : str, optional
    ///     The cache control of the file.
    /// content_disposition : str, optional
    ///     The content disposition of the file.
    ///
    /// Returns
    /// -------
    /// bytes
    ///     The contents of the file as bytes.
    #[allow(clippy::too_many_arguments)]
    #[gen_stub(override_return_type(type_repr = "builtins.bytes", imports=("builtins")))]
    #[pyo3(signature = (path, *,
        version=None,
        concurrent=None,
        chunk=None,
        gap=None,
        offset=None,
        prefetch=None,
        size=None,
        if_match=None,
        if_none_match=None,
        if_modified_since=None,
        if_unmodified_since=None,
        content_type=None,
        cache_control=None,
        content_disposition=None))]
    pub fn read<'p>(
        &'p self,
        py: Python<'p>,
        path: PathBuf,
        version: Option<String>,
        concurrent: Option<usize>,
        chunk: Option<usize>,
        gap: Option<usize>,
        offset: Option<usize>,
        prefetch: Option<usize>,
        size: Option<usize>,
        if_match: Option<String>,
        if_none_match: Option<String>,
        #[gen_stub(override_type(type_repr = "datetime.datetime", imports=("datetime")))]
        if_modified_since: Option<jiff::Timestamp>,
        #[gen_stub(override_type(type_repr = "datetime.datetime", imports=("datetime")))]
        if_unmodified_since: Option<jiff::Timestamp>,
        content_type: Option<String>,
        cache_control: Option<String>,
        content_disposition: Option<String>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let path = path.to_string_lossy().to_string();
        let opts = ReadOptions {
            version,
            concurrent,
            chunk,
            gap,
            offset,
            prefetch,
            size,
            if_match,
            if_none_match,
            if_modified_since,
            if_unmodified_since,
            content_type,
            cache_control,
            content_disposition,
        };
        let buffer = self
            .core
            .read_options(&path, opts.into())
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
    /// append : bool, optional
    ///     Whether to append to the file instead of overwriting it.
    /// chunk : int, optional
    ///     The chunk size to use when writing the file.
    /// concurrent : int, optional
    ///     The number of concurrent requests to make when writing the file.
    /// cache_control : str, optional
    ///     The cache control header to set on the file.
    /// content_type : str, optional
    ///     The content type header to set on the file.
    /// content_disposition : str, optional
    ///     The content disposition header to set on the file.
    /// content_encoding : str, optional
    ///     The content encoding header to set on the file.
    /// if_match : str, optional
    ///     The ETag to match when writing the file.
    /// if_none_match : str, optional
    ///     The ETag to not match when writing the file.
    /// if_not_exists : bool, optional
    ///     Whether to fail if the file already exists.
    /// user_metadata : dict, optional
    ///     The user metadata to set on the file.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (path, bs, *,
        append= None,
        chunk = None,
        concurrent = None,
        cache_control = None,
        content_type = None,
        content_disposition = None,
        content_encoding = None,
        if_match = None,
        if_none_match = None,
        if_not_exists = None,
        user_metadata = None))]
    pub fn write(
        &self,
        path: PathBuf,
        #[gen_stub(override_type(type_repr = "builtins.bytes", imports=("builtins")))] bs: Vec<u8>,
        append: Option<bool>,
        chunk: Option<usize>,
        concurrent: Option<usize>,
        cache_control: Option<String>,
        content_type: Option<String>,
        content_disposition: Option<String>,
        content_encoding: Option<String>,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_not_exists: Option<bool>,
        user_metadata: Option<HashMap<String, String>>,
    ) -> PyResult<()> {
        let path = path.to_string_lossy().to_string();
        let opts = WriteOptions {
            append,
            chunk,
            concurrent,
            cache_control,
            content_type,
            content_disposition,
            content_encoding,
            if_match,
            if_none_match,
            if_not_exists,
            user_metadata,
        };

        self.core
            .write_options(&path, bs, opts.into())
            .map(|_| ())
            .map_err(format_pyerr)
    }

    /// Get the metadata of a file at the given path.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     The path to the file.
    /// version : str, optional
    ///     The version of the file.
    /// if_match : str, optional
    ///     The ETag of the file.
    /// if_none_match : str, optional
    ///     The ETag of the file.
    /// if_modified_since : datetime, optional
    ///     The last modified time of the file.
    /// if_unmodified_since : datetime, optional
    ///     The last modified time of the file.
    /// content_type : str, optional
    ///     The content type of the file.
    /// cache_control : str, optional
    ///     The cache control of the file.
    /// content_disposition : str, optional
    ///     The content disposition of the file.
    ///
    /// Returns
    /// -------
    /// Metadata
    ///     The metadata of the file.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (path, *,
        version=None,
        if_match=None,
        if_none_match=None,
        if_modified_since=None,
        if_unmodified_since=None,
        content_type=None,
        cache_control=None,
        content_disposition=None))]
    pub fn stat(
        &self,
        path: PathBuf,
        version: Option<String>,
        if_match: Option<String>,
        if_none_match: Option<String>,
        #[gen_stub(override_type(type_repr = "datetime.datetime", imports=("datetime")))]
        if_modified_since: Option<jiff::Timestamp>,
        #[gen_stub(override_type(type_repr = "datetime.datetime", imports=("datetime")))]
        if_unmodified_since: Option<jiff::Timestamp>,
        content_type: Option<String>,
        cache_control: Option<String>,
        content_disposition: Option<String>,
    ) -> PyResult<Metadata> {
        let path = path.to_string_lossy().to_string();
        let opts = StatOptions {
            version,
            if_match,
            if_none_match,
            if_modified_since,
            if_unmodified_since,
            content_type,
            cache_control,
            content_disposition,
        };
        self.core
            .stat_options(&path, opts.into())
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
        use ocore::options::ListOptions;
        let path = path.to_string_lossy().to_string();
        let entries = self
            .core
            .list_options(
                &path,
                ListOptions {
                    recursive: true,
                    ..Default::default()
                },
            )
            .map_err(format_pyerr)?;
        self.core
            .delete_try_iter(entries.into_iter().map(Ok))
            .map_err(format_pyerr)
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
    /// limit : int, optional
    ///     The maximum number of entries to return.
    /// start_after : str, optional
    ///     The entry to start after.
    /// recursive : bool, optional
    ///     Whether to list recursively.
    /// versions : bool, optional
    ///     Whether to list versions.
    /// deleted : bool, optional
    ///     Whether to list deleted entries.
    ///
    /// Returns
    /// -------
    /// BlockingLister
    ///     An iterator over the entries in the directory.
    #[gen_stub(override_return_type(
        type_repr="collections.abc.Iterable[opendal.types.Entry]",
        imports=("collections.abc", "opendal.types")
    ))]
    #[pyo3(signature = (path, *,
        limit=None,
        start_after=None,
        recursive=None,
        versions=None,
        deleted=None))]
    pub fn list(
        &self,
        path: PathBuf,
        limit: Option<usize>,
        start_after: Option<String>,
        recursive: Option<bool>,
        versions: Option<bool>,
        deleted: Option<bool>,
    ) -> PyResult<BlockingLister> {
        let path = path.to_string_lossy().to_string();

        let opts = ListOptions {
            limit,
            start_after,
            recursive,
            versions,
            deleted,
        };

        let l = self
            .core
            .lister_options(&path, opts.into())
            .map_err(format_pyerr)?;
        Ok(BlockingLister::new(l))
    }

    /// Recursively list entries in the given directory.
    ///
    /// Deprecated
    /// ----------
    ///     Use `list()` with `recursive=True` instead.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     The path to the directory.
    /// limit : int, optional
    ///     The maximum number of entries to return.
    /// start_after : str, optional
    ///     The entry to start after.
    /// versions : bool, optional
    ///     Whether to list versions.
    /// deleted : bool, optional
    ///     Whether to list deleted entries.
    ///
    /// Returns
    /// -------
    /// BlockingLister
    ///     An iterator over the entries in the directory.
    #[gen_stub(override_return_type(
        type_repr="collections.abc.Iterable[opendal.types.Entry]",
        imports=("collections.abc", "opendal.types")
    ))]
    #[pyo3(signature = (path, *,
        limit=None,
        start_after=None,
        versions=None,
        deleted=None))]
    pub fn scan(
        &self,
        path: PathBuf,
        limit: Option<usize>,
        start_after: Option<String>,
        versions: Option<bool>,
        deleted: Option<bool>,
    ) -> PyResult<BlockingLister> {
        self.list(path, limit, start_after, Some(true), versions, deleted)
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
    /// scheme : str | opendal.services.Scheme
    ///     The scheme of the service.
    /// **kwargs : dict
    ///     The options for the service.
    ///
    /// Returns
    /// -------
    /// AsyncOperator
    ///     The new async operator.
    #[gen_stub(skip)]
    #[new]
    #[pyo3(signature = (scheme, * ,**kwargs))]
    pub fn new(
        #[gen_stub(override_type(type_repr = "builtins.str | opendal.services.Scheme", imports=("builtins", "opendal.services")))]
        scheme: Bound<PyAny>,
        kwargs: Option<&Bound<PyDict>>,
    ) -> PyResult<Self> {
        let scheme = if let Ok(scheme_str) = scheme.extract::<&str>() {
            ocore::Scheme::from_str(scheme_str)
                .map_err(|err| {
                    ocore::Error::new(ocore::ErrorKind::Unexpected, "unsupported scheme")
                        .set_source(err)
                })
                .map_err(format_pyerr)
        } else if let Ok(py_scheme) = scheme.extract::<PyScheme>() {
            Ok(py_scheme.into())
        } else {
            Err(Unsupported::new_err(
                "Invalid type for scheme, expected str or Scheme",
            ))
        }?;

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

    /// Open an async file-like object for the given path.
    ///
    /// The returning async file-like object is a context manager.
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
    #[gen_stub(override_return_type(
        type_repr="collections.abc.Awaitable[opendal.file.AsyncFile]",
        imports=("collections.abc", "opendal.file")
    ))]
    #[pyo3(signature = (path, mode, *, **kwargs))]
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
    /// version : str, optional
    ///     The version of the file.
    /// concurrent : int, optional
    ///     The number of concurrent readers.
    /// chunk : int, optional
    ///     The size of each chunk.
    /// gap : int, optional
    ///     The gap between each chunk.
    /// offset : int, optional
    ///     The offset of the file.
    /// prefetch : int, optional
    ///     The number of bytes to prefetch.
    /// size : int, optional
    ///     The size of the file.
    /// if_match : str, optional
    ///     The ETag of the file.
    /// if_none_match : str, optional
    ///     The ETag of the file.
    /// if_modified_since : str, optional
    ///     The last modified time of the file.
    /// if_unmodified_since : str, optional
    ///     The last modified time of the file.
    /// content_type : str, optional
    ///     The content type of the file.
    /// cache_control : str, optional
    ///     The cache control of the file.
    /// content_disposition : str, optional
    ///     The content disposition of the file.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that returns the contents of the file as bytes.
    #[allow(clippy::too_many_arguments)]
    #[gen_stub(override_return_type(
        type_repr="collections.abc.Awaitable[builtins.bytes]",
        imports=("collections.abc", "builtins")
    ))]
    #[pyo3(signature = (path, *,
        version=None,
        concurrent=None,
        chunk=None,
        gap=None,
        offset=None,
        prefetch=None,
        size=None,
        if_match=None,
        if_none_match=None,
        if_modified_since=None,
        if_unmodified_since=None,
        content_type=None,
        cache_control=None,
        content_disposition=None))]
    pub fn read<'p>(
        &'p self,
        py: Python<'p>,
        path: PathBuf,
        version: Option<String>,
        concurrent: Option<usize>,
        chunk: Option<usize>,
        gap: Option<usize>,
        offset: Option<usize>,
        prefetch: Option<usize>,
        size: Option<usize>,
        if_match: Option<String>,
        if_none_match: Option<String>,
        #[gen_stub(override_type(type_repr = "datetime.datetime", imports=("datetime")))]
        if_modified_since: Option<jiff::Timestamp>,
        #[gen_stub(override_type(type_repr = "datetime.datetime", imports=("datetime")))]
        if_unmodified_since: Option<jiff::Timestamp>,
        content_type: Option<String>,
        cache_control: Option<String>,
        content_disposition: Option<String>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let this = self.core.clone();
        let path = path.to_string_lossy().to_string();
        let opts = ReadOptions {
            version,
            concurrent,
            chunk,
            gap,
            offset,
            prefetch,
            size,
            if_match,
            if_none_match,
            if_modified_since,
            if_unmodified_since,
            content_type,
            cache_control,
            content_disposition,
        };
        future_into_py(py, async move {
            let range = opts.make_range();
            let res = this
                .reader_options(&path, opts.into())
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
    /// append : bool, optional
    ///     Whether to append to the file instead of overwriting it.
    /// chunk : int, optional
    ///     The chunk size to use when writing the file.
    /// concurrent : int, optional
    ///     The number of concurrent requests to make when writing the file.
    /// cache_control : str, optional
    ///     The cache control header to set on the file.
    /// content_type : str, optional
    ///     The content type header to set on the file.
    /// content_disposition : str, optional
    ///     The content disposition header to set on the file.
    /// content_encoding : str, optional
    ///     The content encoding header to set on the file.
    /// if_match : str, optional
    ///     The ETag to match when writing the file.
    /// if_none_match : str, optional
    ///     The ETag to not match when writing the file.
    /// if_not_exists : bool, optional
    ///     Whether to fail if the file already exists.
    /// user_metadata : dict, optional
    ///     The user metadata to set on the file.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that completes when the write is finished.
    #[allow(clippy::too_many_arguments)]
    #[gen_stub(override_return_type(
        type_repr="collections.abc.Awaitable[None]",
        imports=("collections.abc")
    ))]
    #[pyo3(signature = (path, bs, *,
        append= None,
        chunk = None,
        concurrent = None,
        cache_control = None,
        content_type = None,
        content_disposition = None,
        content_encoding = None,
        if_match = None,
        if_none_match = None,
        if_not_exists = None,
        user_metadata = None))]
    pub fn write<'p>(
        &'p self,
        py: Python<'p>,
        path: PathBuf,
        #[gen_stub(override_type(type_repr = "builtins.bytes", imports=("builtins")))] bs: &Bound<
            PyBytes,
        >,
        append: Option<bool>,
        chunk: Option<usize>,
        concurrent: Option<usize>,
        cache_control: Option<String>,
        content_type: Option<String>,
        content_disposition: Option<String>,
        content_encoding: Option<String>,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_not_exists: Option<bool>,
        user_metadata: Option<HashMap<String, String>>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let opts = WriteOptions {
            append,
            chunk,
            concurrent,
            cache_control,
            content_type,
            content_disposition,
            content_encoding,
            if_match,
            if_none_match,
            if_not_exists,
            user_metadata,
        };
        let this = self.core.clone();
        let bs = bs.as_bytes().to_vec();
        let path = path.to_string_lossy().to_string();
        future_into_py(py, async move {
            this.write_options(&path, bs, opts.into())
                .await
                .map(|_| ())
                .map_err(format_pyerr)
        })
    }

    /// Get the metadata of a file at the given path.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     The path to the file.
    /// version : str, optional
    ///     The version of the file.
    /// if_match : str, optional
    ///     The ETag of the file.
    /// if_none_match : str, optional
    ///     The ETag of the file.
    /// if_modified_since : datetime, optional
    ///     The last modified time of the file.
    /// if_unmodified_since : datetime, optional
    ///     The last modified time of the file.
    /// content_type : str, optional
    ///     The content type of the file.
    /// cache_control : str, optional
    ///     The cache control of the file.
    /// content_disposition : str, optional
    ///     The content disposition of the file.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that returns the metadata of the file.
    #[allow(clippy::too_many_arguments)]
    #[gen_stub(override_return_type(
        type_repr="collections.abc.Awaitable[Metadata]",
        imports=("collections.abc")
    ))]
    #[pyo3(signature = (path, *,
        version=None,
        if_match=None,
        if_none_match=None,
        if_modified_since=None,
        if_unmodified_since=None,
        content_type=None,
        cache_control=None,
        content_disposition=None))]
    pub fn stat<'p>(
        &'p self,
        py: Python<'p>,
        path: PathBuf,
        version: Option<String>,
        if_match: Option<String>,
        if_none_match: Option<String>,
        #[gen_stub(override_type(type_repr = "datetime.datetime", imports=("datetime")))]
        if_modified_since: Option<jiff::Timestamp>,
        #[gen_stub(override_type(type_repr = "datetime.datetime", imports=("datetime")))]
        if_unmodified_since: Option<jiff::Timestamp>,
        content_type: Option<String>,
        cache_control: Option<String>,
        content_disposition: Option<String>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let this = self.core.clone();
        let path = path.to_string_lossy().to_string();
        let opts = StatOptions {
            version,
            if_match,
            if_none_match,
            if_modified_since,
            if_unmodified_since,
            content_type,
            cache_control,
            content_disposition,
        };

        future_into_py(py, async move {
            let res: Metadata = this
                .stat_options(&path, opts.into())
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
    #[gen_stub(override_return_type(
        type_repr="collections.abc.Awaitable[None]",
        imports=("collections.abc")
    ))]
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
    #[gen_stub(override_return_type(
        type_repr="collections.abc.Awaitable[None]",
        imports=("collections.abc")
    ))]
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
    #[gen_stub(override_return_type(
        type_repr="collections.abc.Awaitable[None]",
        imports=("collections.abc")
    ))]
    pub fn remove_all<'p>(&'p self, py: Python<'p>, path: PathBuf) -> PyResult<Bound<'p, PyAny>> {
        let this = self.core.clone();
        let path = path.to_string_lossy().to_string();
        future_into_py(py, async move {
            let lister = this
                .lister_with(&path)
                .recursive(true)
                .await
                .map_err(format_pyerr)?;
            this.delete_try_stream(lister).await.map_err(format_pyerr)
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
    #[gen_stub(override_return_type(
        type_repr="collections.abc.Awaitable[None]",
        imports=("collections.abc")
    ))]
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
    #[gen_stub(override_return_type(
        type_repr="collections.abc.Awaitable[None]",
        imports=("collections.abc")
    ))]
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
    #[gen_stub(override_return_type(
        type_repr="collections.abc.Awaitable[None]",
        imports=("collections.abc")
    ))]
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
    #[gen_stub(override_return_type(
        type_repr="collections.abc.Awaitable[builtins.bool]",
        imports=("collections.abc", "builtins")
    ))]
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
    /// limit : int, optional
    ///     The maximum number of entries to return.
    /// start_after : str, optional
    ///     The entry to start after.
    /// recursive : bool, optional
    ///     Whether to list recursively.
    /// versions : bool, optional
    ///     Whether to list versions.
    /// deleted : bool, optional
    ///     Whether to list deleted entries.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that returns an async iterator over the entries.
    #[allow(clippy::too_many_arguments)]
    #[gen_stub(override_return_type(
        type_repr="collections.abc.Awaitable[collections.abc.AsyncIterable[opendal.types.Entry]]",
        imports=("collections.abc", "opendal.types")
    ))]
    #[pyo3(signature = (path, *,
        limit=None,
        start_after=None,
        recursive=None,
        versions=None,
        deleted=None))]
    pub fn list<'p>(
        &'p self,
        py: Python<'p>,
        path: PathBuf,
        limit: Option<usize>,
        start_after: Option<String>,
        recursive: Option<bool>,
        versions: Option<bool>,
        deleted: Option<bool>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let this = self.core.clone();
        let path = path.to_string_lossy().to_string();
        let opts = ListOptions {
            limit,
            start_after,
            recursive,
            versions,
            deleted,
        };

        future_into_py(py, async move {
            let lister = this
                .lister_options(&path, opts.into())
                .await
                .map_err(format_pyerr)?;
            let pylister = Python::attach(|py| AsyncLister::new(lister).into_py_any(py))?;

            Ok(pylister)
        })
    }

    /// Recursively list entries in the given directory.
    ///
    /// Deprecated
    /// ----------
    ///     Use `list()` with `recursive=True` instead.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     The path to the directory.
    /// limit : int, optional
    ///     The maximum number of entries to return.
    /// start_after : str, optional
    ///     The entry to start after.
    /// versions : bool, optional
    ///     Whether to list versions.
    /// deleted : bool, optional
    ///     Whether to list deleted entries.
    ///
    /// Returns
    /// -------
    /// coroutine
    ///     An awaitable that returns an async iterator over the entries.
    #[gen_stub(override_return_type(
        type_repr="collections.abc.Awaitable[collections.abc.AsyncIterable[opendal.types.Entry]]",
        imports=("collections.abc", "opendal.types")
    ))]
    #[gen_stub(skip)]
    #[pyo3(signature = (path, *,
        limit=None,
        start_after=None,
        versions=None,
        deleted=None))]
    pub fn scan<'p>(
        &'p self,
        py: Python<'p>,
        path: PathBuf,
        limit: Option<usize>,
        start_after: Option<String>,
        versions: Option<bool>,
        deleted: Option<bool>,
    ) -> PyResult<Bound<'p, PyAny>> {
        self.list(py, path, limit, start_after, Some(true), versions, deleted)
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
    #[gen_stub(override_return_type(
        type_repr="collections.abc.Awaitable[opendal.types.PresignedRequest]",
        imports=("collections.abc", "opendal.types")
    ))]
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
    #[gen_stub(override_return_type(
        type_repr="collections.abc.Awaitable[opendal.types.PresignedRequest]",
        imports=("collections.abc", "opendal.types")
    ))]
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
    #[gen_stub(override_return_type(
        type_repr="collections.abc.Awaitable[opendal.types.PresignedRequest]",
        imports=("collections.abc", "opendal.types")
    ))]
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
    #[gen_stub(override_return_type(
        type_repr="collections.abc.Awaitable[opendal.types.PresignedRequest]",
        imports=("collections.abc", "opendal.types")
    ))]
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
