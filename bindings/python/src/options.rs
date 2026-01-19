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

use opendal::{self as ocore, raw::BytesRange};
use pyo3::Borrowed;
use pyo3::FromPyObject;
use pyo3::PyAny;
use pyo3::PyErr;
use pyo3::PyResult;
use pyo3::conversion::FromPyObjectOwned;
use pyo3::exceptions::PyTypeError;
use pyo3::pyclass;
use pyo3::types::PyAnyMethods;
use pyo3::types::PyDict;
use pyo3::types::PyDictMethods;
use std::collections::HashMap;

#[pyclass(module = "opendal")]
#[derive(Default)]
pub struct ReadOptions {
    pub version: Option<String>,
    pub concurrent: Option<usize>,
    pub chunk: Option<usize>,
    pub gap: Option<usize>,
    pub offset: Option<usize>,
    pub prefetch: Option<usize>,
    pub size: Option<usize>,
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    pub if_modified_since: Option<jiff::Timestamp>,
    pub if_unmodified_since: Option<jiff::Timestamp>,
    pub content_type: Option<String>,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
}

fn map_exception(name: &str, err: PyErr) -> PyErr {
    PyErr::new::<PyTypeError, _>(format!("Unable to convert key: {name}. Error: {err}"))
}

fn extract_optional<'py, T>(dict: &pyo3::Bound<'py, PyDict>, name: &str) -> PyResult<Option<T>>
where
    T: FromPyObjectOwned<'py>,
{
    match dict.get_item(name)? {
        Some(v) => v
            .extract::<T>()
            .map(Some)
            .map_err(|err| map_exception(name, err.into())),
        None => Ok(None),
    }
}

fn downcast_kwargs<'a, 'py>(obj: Borrowed<'a, 'py, PyAny>) -> PyResult<pyo3::Bound<'py, PyDict>> {
    let obj: &pyo3::Bound<'_, PyAny> = &obj;
    obj.cast::<PyDict>()
        .cloned()
        .map_err(|_| PyErr::new::<PyTypeError, _>("Invalid type to convert, expected dict"))
}

impl<'a, 'py> FromPyObject<'a, 'py> for ReadOptions {
    type Error = PyErr;

    fn extract(obj: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {
        let dict = downcast_kwargs(obj)?;

        Ok(Self {
            version: extract_optional(&dict, "version")?,
            concurrent: extract_optional(&dict, "concurrent")?,
            chunk: extract_optional(&dict, "chunk")?,
            gap: extract_optional(&dict, "gap")?,
            offset: extract_optional(&dict, "offset")?,
            prefetch: extract_optional(&dict, "prefetch")?,
            size: extract_optional(&dict, "size")?,
            if_match: extract_optional(&dict, "if_match")?,
            if_none_match: extract_optional(&dict, "if_none_match")?,
            if_modified_since: extract_optional(&dict, "if_modified_since")?,
            if_unmodified_since: extract_optional(&dict, "if_unmodified_since")?,
            content_type: extract_optional(&dict, "content_type")?,
            cache_control: extract_optional(&dict, "cache_control")?,
            content_disposition: extract_optional(&dict, "content_disposition")?,
        })
    }
}

impl ReadOptions {
    pub fn make_range(&self) -> BytesRange {
        let offset = self.offset.unwrap_or_default() as u64;
        let size = self.size.map(|v| v as u64);

        BytesRange::new(offset, size)
    }
}

#[pyclass(module = "opendal")]
#[derive(Default)]
pub struct WriteOptions {
    pub append: Option<bool>,
    pub chunk: Option<usize>,
    pub concurrent: Option<usize>,
    pub cache_control: Option<String>,
    pub content_type: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    pub if_not_exists: Option<bool>,
    pub user_metadata: Option<HashMap<String, String>>,
}

impl<'a, 'py> FromPyObject<'a, 'py> for WriteOptions {
    type Error = PyErr;

    fn extract(obj: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {
        let dict = downcast_kwargs(obj)?;

        Ok(Self {
            append: extract_optional(&dict, "append")?,
            chunk: extract_optional(&dict, "chunk")?,
            concurrent: extract_optional(&dict, "concurrent")?,
            cache_control: extract_optional(&dict, "cache_control")?,
            content_type: extract_optional(&dict, "content_type")?,
            content_disposition: extract_optional(&dict, "content_disposition")?,
            content_encoding: extract_optional(&dict, "content_encoding")?,
            if_match: extract_optional(&dict, "if_match")?,
            if_none_match: extract_optional(&dict, "if_none_match")?,
            if_not_exists: extract_optional(&dict, "if_not_exists")?,
            user_metadata: extract_optional(&dict, "user_metadata")?,
        })
    }
}

impl From<ReadOptions> for ocore::options::ReadOptions {
    fn from(opts: ReadOptions) -> Self {
        Self {
            range: opts.make_range(),
            version: opts.version,
            if_match: opts.if_match,
            if_none_match: opts.if_none_match,
            if_modified_since: opts.if_modified_since.map(Into::into),
            if_unmodified_since: opts.if_unmodified_since.map(Into::into),
            concurrent: opts.concurrent.unwrap_or_default(),
            chunk: opts.chunk,
            gap: opts.gap,
            override_content_type: opts.content_type,
            override_cache_control: opts.cache_control,
            override_content_disposition: opts.content_disposition,
        }
    }
}

impl From<ReadOptions> for ocore::options::ReaderOptions {
    fn from(opts: ReadOptions) -> Self {
        Self {
            version: opts.version,
            if_match: opts.if_match,
            if_none_match: opts.if_none_match,
            if_modified_since: opts.if_modified_since.map(Into::into),
            if_unmodified_since: opts.if_unmodified_since.map(Into::into),
            concurrent: opts.concurrent.unwrap_or_default(),
            chunk: opts.chunk,
            gap: opts.gap,
            prefetch: opts.prefetch.unwrap_or_default(),
        }
    }
}

impl From<WriteOptions> for ocore::options::WriteOptions {
    fn from(opts: WriteOptions) -> Self {
        Self {
            append: opts.append.unwrap_or(false),
            concurrent: opts.concurrent.unwrap_or_default(),
            chunk: opts.chunk,
            content_type: opts.content_type,
            content_disposition: opts.content_disposition,
            cache_control: opts.cache_control,
            content_encoding: opts.content_encoding,
            user_metadata: opts.user_metadata,
            if_match: opts.if_match,
            if_none_match: opts.if_none_match,
            if_not_exists: opts.if_not_exists.unwrap_or(false),
        }
    }
}

#[pyclass(module = "opendal")]
#[derive(Default, Debug)]
pub struct ListOptions {
    pub limit: Option<usize>,
    pub start_after: Option<String>,
    pub recursive: Option<bool>,
    pub versions: Option<bool>,
    pub deleted: Option<bool>,
}

impl<'a, 'py> FromPyObject<'a, 'py> for ListOptions {
    type Error = PyErr;

    fn extract(obj: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {
        let dict = downcast_kwargs(obj)?;

        Ok(Self {
            limit: extract_optional(&dict, "limit")?,
            start_after: extract_optional(&dict, "start_after")?,
            recursive: extract_optional(&dict, "recursive")?,
            versions: extract_optional(&dict, "versions")?,
            deleted: extract_optional(&dict, "deleted")?,
        })
    }
}

impl From<ListOptions> for ocore::options::ListOptions {
    fn from(opts: ListOptions) -> Self {
        Self {
            limit: opts.limit,
            start_after: opts.start_after,
            recursive: opts.recursive.unwrap_or(false),
            versions: opts.versions.unwrap_or(false),
            deleted: opts.deleted.unwrap_or(false),
        }
    }
}

#[pyclass(module = "opendal")]
#[derive(Default, Debug)]
pub struct StatOptions {
    pub version: Option<String>,
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    pub if_modified_since: Option<jiff::Timestamp>,
    pub if_unmodified_since: Option<jiff::Timestamp>,
    pub content_type: Option<String>,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
}

impl<'a, 'py> FromPyObject<'a, 'py> for StatOptions {
    type Error = PyErr;

    fn extract(obj: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {
        let dict = downcast_kwargs(obj)?;

        Ok(Self {
            version: extract_optional(&dict, "version")?,
            if_match: extract_optional(&dict, "if_match")?,
            if_none_match: extract_optional(&dict, "if_none_match")?,
            if_modified_since: extract_optional(&dict, "if_modified_since")?,
            if_unmodified_since: extract_optional(&dict, "if_unmodified_since")?,
            content_type: extract_optional(&dict, "content_type")?,
            cache_control: extract_optional(&dict, "cache_control")?,
            content_disposition: extract_optional(&dict, "content_disposition")?,
        })
    }
}

impl From<StatOptions> for ocore::options::StatOptions {
    fn from(opts: StatOptions) -> Self {
        Self {
            version: opts.version,
            if_match: opts.if_match,
            if_none_match: opts.if_none_match,
            if_modified_since: opts.if_modified_since.map(Into::into),
            if_unmodified_since: opts.if_unmodified_since.map(Into::into),
            override_content_type: opts.content_type,
            override_cache_control: opts.cache_control,
            override_content_disposition: opts.content_disposition,
        }
    }
}
