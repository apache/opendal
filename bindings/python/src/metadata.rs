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
use pyo3::prelude::*;
use std::collections::HashMap;

#[pyclass(module = "opendal")]
pub struct Entry(ocore::Entry);

impl Entry {
    pub fn new(entry: ocore::Entry) -> Self {
        Self(entry)
    }
}

#[pymethods]
impl Entry {
    /// Path of entry. Path is relative to the operator's root.
    #[getter]
    pub fn path(&self) -> &str {
        self.0.path()
    }

    /// Metadata of entry.
    #[getter]
    pub fn metadata(&self) -> Metadata {
        Metadata::new(self.0.metadata().clone())
    }

    fn __str__(&self) -> &str {
        self.0.path()
    }

    fn __repr__(&self) -> String {
        format!(
            "Entry(path={:?}, metadata={})",
            self.path(),
            self.metadata().__repr__()
        )
    }
}

#[pyclass(module = "opendal")]
pub struct Metadata(ocore::Metadata);

impl Metadata {
    pub fn new(meta: ocore::Metadata) -> Self {
        Self(meta)
    }
}

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

    /// mode represents this entry's mode.
    #[getter]
    pub fn mode(&self) -> EntryMode {
        EntryMode(self.0.mode())
    }

    /// Returns `true` if this metadata is for a file.
    #[getter]
    pub fn is_file(&self) -> bool {
        self.mode().is_file()
    }

    /// Returns `true` if this metadata is for a directory.
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

#[pyclass(module = "opendal")]
pub struct EntryMode(ocore::EntryMode);

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
            ocore::EntryMode::FILE => "EntryMode.FILE",
            ocore::EntryMode::DIR => "EntryMode.DIR",
            ocore::EntryMode::Unknown => "EntryMode.UNKNOWN",
        }
    }
}
