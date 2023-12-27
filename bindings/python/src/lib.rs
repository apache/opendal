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

/// Apache OpenDAL Python binding
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
    m.add_class::<AsyncOperator>()?;

    m.add_class::<File>()?;
    m.add_class::<AsyncFile>()?;

    m.add_class::<Entry>()?;
    m.add_class::<EntryMode>()?;
    m.add_class::<Metadata>()?;
    m.add_class::<PresignedRequest>()?;
    m.add_class::<Capability>()?;

    // Layer module
    let layers_module = PyModule::new(py, "layers")?;
    layers_module.add_class::<Layer>()?;
    layers_module.add_class::<RetryLayer>()?;
    m.add_submodule(layers_module)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("opendal.layers", layers_module)?;

    let exception_module = PyModule::new(py, "exceptions")?;
    exception_module.add("Error", py.get_type::<Error>())?;
    exception_module.add("Unexpected", py.get_type::<UnexpectedError>())?;
    exception_module.add("Unsupported", py.get_type::<UnsupportedError>())?;
    exception_module.add("ConfigInvalid", py.get_type::<ConfigInvalidError>())?;
    exception_module.add("NotFound", py.get_type::<NotFoundError>())?;
    exception_module.add("PermissionDenied", py.get_type::<PermissionDeniedError>())?;
    exception_module.add("IsADirectory", py.get_type::<IsADirectoryError>())?;
    exception_module.add("NotADirectory", py.get_type::<NotADirectoryError>())?;
    exception_module.add("AlreadyExists", py.get_type::<AlreadyExistsError>())?;
    exception_module.add("IsSameFile", py.get_type::<IsSameFileError>())?;
    exception_module.add("ConditionNotMatch", py.get_type::<ConditionNotMatchError>())?;
    exception_module.add("ContentTruncated", py.get_type::<ContentTruncatedError>())?;
    exception_module.add("ContentIncomplete", py.get_type::<ContentIncompleteError>())?;
    exception_module.add("InvalidInput", py.get_type::<InvalidInputError>())?;
    m.add_submodule(exception_module)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("opendal.exceptions", exception_module)?;
    Ok(())
}
