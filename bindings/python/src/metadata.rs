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

use chrono::prelude::*;
use pyo3::prelude::*;

use crate::*;

#[pyclass(module = "opendal")]
pub struct Entry(ocore::Entry);

impl Entry {
    pub fn new(entry: ocore::Entry) -> Self {
        Self(entry)
    }
}

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

    /// Last modified time
    #[getter]
    pub fn last_modified(&self) -> Option<DateTime<Utc>> {
        self.0.last_modified()
    }
    pub fn __repr__(&self) -> String {
        let last_modified_str = match self.0.last_modified() {
            Some(dt) => dt.format("%Y-%m-%dT%H:%M:%S").to_string(),
            None => "None".to_string(),
        };

        format!(
            "Metadata(mode={}, content_length={}, content_type={}, last_modified={}, etag={})",
            self.0.mode(),
            self.0.content_length(),
            self.0.content_type().unwrap_or("None"),
            last_modified_str,
            self.0.etag().unwrap_or("None"),
        )
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
