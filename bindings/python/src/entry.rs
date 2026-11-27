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

/// Entry
///
/// An entry representing a path and its associated metadata.
///
/// Notes
/// -----
/// If this entry is a directory, ``path`` **must** end with ``/``.
/// Otherwise, ``path`` **must not** end with ``/``.
#[pyclass(module = "opendal.types")]
pub struct Entry(ocore::Entry);

impl Entry {
    pub fn new(entry: ocore::Entry) -> Self {
        Self(entry)
    }
}
#[pymethods]
impl Entry {
    /// The path of entry relative to the operator's root.
    #[getter]
    pub fn path(&self) -> &str {
        self.0.path()
    }

    /// The name of entry, representing the last segment of the path.
    #[getter]
    pub fn name(&self) -> &str {
        self.0.name()
    }

    /// The metadata of this entry.
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

/// EntryMode
///
/// The mode of an entry, indicating if it is a file or a directory.
#[pyclass(eq, eq_int, hash, frozen, module = "opendal.types")]
#[pyo3(rename_all = "PascalCase")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntryMode {
    /// The entry is a file and has data to read.
    FILE,
    /// The entry is a directory and can be listed.
    DIR,
    /// The mode of the entry is unknown.
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
#[pymethods]
impl EntryMode {
    /// Check if the entry mode is `File`.
    ///
    /// Returns
    /// -------
    /// bool
    ///     True if the entry is a file.
    pub fn is_file(&self) -> bool {
        matches!(self, EntryMode::FILE)
    }

    /// Check if the entry mode is `Dir`.
    ///
    /// Returns
    /// -------
    /// bool
    ///     True if the entry is a directory.
    pub fn is_dir(&self) -> bool {
        matches!(self, EntryMode::DIR)
    }
}
