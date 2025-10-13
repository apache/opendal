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

use crate::*;
use jiff::Timestamp;
use std::collections::HashMap;

/// Entry representing a path and its associated metadata.
///
/// If this entry is a directory, ``path`` **must** end with ``/``.
/// Otherwise, ``path`` **must not** end with ``/``.
#[gen_stub_pyclass]
#[pyclass(module = "opendal.types")]
pub struct Entry(ocore::Entry);

impl Entry {
    pub fn new(entry: ocore::Entry) -> Self {
        Self(entry)
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl Entry {
    /// Path of entry relative to operator's root.
    #[getter]
    pub fn path(&self) -> &str {
        self.0.path()
    }

    /// Name of entry representing the last segment of path.
    #[getter]
    pub fn name(&self) -> &str {
        self.0.name()
    }

    /// Metadata of this entry.
    #[getter]
    pub fn metadata(&self) -> Metadata {
        Metadata::new(self.0.metadata().clone())
    }

    #[gen_stub(skip)]
    fn __str__(&self) -> &str {
        self.0.path()
    }

    #[gen_stub(skip)]
    fn __repr__(&self) -> String {
        format!(
            "Entry(path={:?}, metadata={})",
            self.path(),
            self.metadata().__repr__()
        )
    }
}

/// Metadata of an entry.
///
/// Depending on the request context, metadata for the same path may vary.
/// For example, two versions of the same path might have different content
/// lengths. Metadata is always tied to a specific context and is not a global
/// state.
///
/// In systems that support versioning, such as AWS S3, the metadata may
/// represent a specific version of a file.
///
/// Users can access :meth:`Metadata.version` to retrieve the file version, if
/// available. They can also use :meth:`Metadata.is_current` and
/// :meth:`Metadata.is_deleted` to determine whether the metadata represents the
/// latest version or a deleted one.
#[gen_stub_pyclass]
#[pyclass(module = "opendal.types")]
pub struct Metadata(ocore::Metadata);

impl Metadata {
    pub fn new(meta: ocore::Metadata) -> Self {
        Self(meta)
    }
}

#[gen_stub_pymethods]
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

    /// Content Type of this entry.
    #[getter]
    pub fn content_encoding(&self) -> Option<&str> {
        self.0.content_encoding()
    }

    /// ETag of this entry.
    #[getter]
    pub fn etag(&self) -> Option<&str> {
        self.0.etag()
    }

    /// Mode represents this entry's mode.
    #[getter]
    pub fn mode(&self) -> EntryMode {
        EntryMode::new(self.0.mode())
    }

    /// Return ``True`` if this metadata is for a file.
    #[getter]
    pub fn is_file(&self) -> bool {
        self.mode().is_file()
    }

    /// Return ``True`` if this metadata is for a directory.
    #[getter]
    pub fn is_dir(&self) -> bool {
        self.mode().is_dir()
    }

    /// Last modified time
    #[getter]
    pub fn last_modified(&self) -> Option<Timestamp> {
        self.0.last_modified()
    }

    /// Version of this entry, if available.
    #[getter]
    pub fn version(&self) -> Option<&str> {
        self.0.version()
    }

    /// User defined metadata of this entry
    #[getter]
    pub fn user_metadata(&self) -> Option<&HashMap<String, String>> {
        self.0.user_metadata()
    }

    #[gen_stub(skip)]
    pub fn __repr__(&self) -> String {
        let mut parts = vec![];

        parts.push(format!("mode={}", self.0.mode()));
        parts.push(format!(
            "content_disposition={:?}",
            self.0.content_disposition()
        ));
        parts.push(format!("content_length={}", self.0.content_length()));
        parts.push(format!("content_md5={:?}", self.0.content_md5()));
        parts.push(format!("content_type={:?}", self.0.content_type()));
        parts.push(format!("content_encoding={:?}", self.0.content_encoding()));
        parts.push(format!("etag={:?}", self.0.etag()));
        parts.push(format!("last_modified={:?}", self.0.last_modified()));
        parts.push(format!("version={:?}", self.0.version()));
        parts.push(format!("user_metadata={:?}", self.0.user_metadata()));

        format!("Metadata({})", parts.join(", "))
    }
}

/// EntryMode represents the mode.
#[gen_stub_pyclass_enum]
#[pyclass(eq, eq_int, hash, frozen, module = "opendal.types")]
#[pyo3(rename_all = "PascalCase")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntryMode {
    /// FILE means the path has data to read.
    FILE,
    /// DIR means the path can be listed.
    DIR,
    /// Unknown means we don't know what we can do on this path.
    Unknown,
}

impl EntryMode {
    pub fn new(mode: ocore::EntryMode) -> Self {
        match mode {
            ocore::EntryMode::FILE => Self::FILE,
            ocore::EntryMode::DIR => Self::DIR,
            ocore::EntryMode::Unknown => Self::Unknown,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl EntryMode {
    /// Return ``True`` if this is a file.
    pub fn is_file(&self) -> bool {
        matches!(self, EntryMode::FILE)
    }

    /// Return ``True`` if this is a directory.
    pub fn is_dir(&self) -> bool {
        matches!(self, EntryMode::DIR)
    }
}
