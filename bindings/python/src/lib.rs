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

/// Apache OpenDAL™ Python binding
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
#[pymodule(gil_used = false)]
fn _opendal(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Operator>()?;
    m.add_class::<AsyncOperator>()?;

    m.add_class::<File>()?;
    m.add_class::<AsyncFile>()?;

    m.add_class::<Entry>()?;
    m.add_class::<EntryMode>()?;
    m.add_class::<Metadata>()?;
    m.add_class::<PresignedRequest>()?;
    m.add_class::<Capability>()?;

    m.add_class::<WriteOptions>()?;

    // Layer module
    let layers_module = PyModule::new(py, "layers")?;
    layers_module.add_class::<Layer>()?;
    layers_module.add_class::<RetryLayer>()?;
    layers_module.add_class::<ConcurrentLimitLayer>()?;
    m.add_submodule(&layers_module)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("opendal.layers", layers_module)?;

    let exception_module = PyModule::new(py, "exceptions")?;
    exception_module.add("Error", py.get_type::<Error>())?;
    exception_module.add("Unexpected", py.get_type::<Unexpected>())?;
    exception_module.add("Unsupported", py.get_type::<Unsupported>())?;
    exception_module.add("ConfigInvalid", py.get_type::<ConfigInvalid>())?;
    exception_module.add("NotFound", py.get_type::<NotFound>())?;
    exception_module.add("PermissionDenied", py.get_type::<PermissionDenied>())?;
    exception_module.add("IsADirectory", py.get_type::<IsADirectory>())?;
    exception_module.add("NotADirectory", py.get_type::<NotADirectory>())?;
    exception_module.add("AlreadyExists", py.get_type::<AlreadyExists>())?;
    exception_module.add("IsSameFile", py.get_type::<IsSameFile>())?;
    exception_module.add("ConditionNotMatch", py.get_type::<ConditionNotMatch>())?;
    m.add_submodule(&exception_module)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("opendal.exceptions", exception_module)?;
    Ok(())
}
