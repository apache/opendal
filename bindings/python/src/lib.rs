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

// expose the opendal rust core as `core`.
// We will use `ocore::Xxx` to represents all types from opendal rust core.
pub use ::opendal as ocore;
use pyo3::prelude::*;

mod capability;
pub use capability::*;
mod layers;
pub use layers::*;
mod lister;
pub use lister::*;
mod metadata;
pub use metadata::*;
mod operator;
pub use operator::*;
mod file;
pub use file::*;
mod utils;
pub use utils::*;
mod errors;
pub use errors::*;
mod options;
pub use options::*;
use pyo3_stub_gen::{define_stub_info_gatherer, derive::*};

#[pymodule(gil_used = false)]
fn _opendal(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Add version
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    // Operator module
    add_pymodule!(py, m, "operator", [Operator, AsyncOperator])?;

    // File module
    add_pymodule!(py, m, "file", [File, AsyncFile])?;

    // Capability module
    add_pymodule!(py, m, "capability", [Capability])?;

    // Layers module
    add_pymodule!(
        py,
        m,
        "layers",
        [Layer, RetryLayer, ConcurrentLimitLayer, MimeGuessLayer]
    )?;

    // Types module
    add_pymodule!(
        py,
        m,
        "types",
        [Entry, EntryMode, Metadata, PresignedRequest]
    )?;

    m.add_class::<WriteOptions>()?;
    m.add_class::<ReadOptions>()?;
    m.add_class::<ListOptions>()?;
    m.add_class::<StatOptions>()?;

    // Exceptions module
    add_pyexceptions!(
        py,
        m,
        "exceptions",
        [
            Error,
            Unexpected,
            Unsupported,
            ConfigInvalid,
            NotFound,
            PermissionDenied,
            IsADirectory,
            NotADirectory,
            AlreadyExists,
            IsSameFile,
            ConditionNotMatch
        ]
    )?;
    Ok(())
}

define_stub_info_gatherer!(stub_info);
