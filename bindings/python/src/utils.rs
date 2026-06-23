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

use std::os::raw::c_int;

use pyo3::IntoPyObjectExt;
use pyo3::ffi;
use pyo3::prelude::*;

/// A bytes-like object that implements buffer protocol.
#[pyclass(module = "opendal")]
pub struct Buffer {
    inner: Vec<u8>,
}

impl Buffer {
    pub fn new(inner: Vec<u8>) -> Self {
        Buffer { inner }
    }

    /// Consume self to build a bytes
    pub fn into_bytes(self, py: Python) -> PyResult<Py<PyAny>> {
        let buffer = self.into_py_any(py)?;

        unsafe {
            Bound::from_owned_ptr_or_err(py, ffi::PyBytes_FromObject(buffer.as_ptr()))
                .map(Bound::unbind)
        }
    }

    /// Consume self to build a bytes
    pub fn into_bytes_ref(self, py: Python) -> PyResult<Bound<PyAny>> {
        let buffer = self.into_py_any(py)?;
        let view =
            unsafe { Bound::from_owned_ptr_or_err(py, ffi::PyBytes_FromObject(buffer.as_ptr()))? };

        Ok(view)
    }
}

#[pymethods]
impl Buffer {
    unsafe fn __getbuffer__(
        slf: PyRefMut<Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let bytes = slf.inner.as_slice();
        let ret = unsafe {
            ffi::PyBuffer_FillInfo(
                view,
                slf.as_ptr() as *mut _,
                bytes.as_ptr() as *mut _,
                bytes.len().try_into().unwrap(),
                1, // read only
                flags,
            )
        };
        if ret == -1 {
            return Err(PyErr::fetch(slf.py()));
        }
        Ok(())
    }
}

/// Insert nested `#[pymodule]` submodules into `sys.modules` under `parent_name`
/// and qualify their `__name__`, so `from opendal.operator import ...` resolves.
///
/// PyO3 attaches submodules as attributes but skips `sys.modules` (PyO3 #759);
/// `parent_name` lets us use the public `opendal` name, not the `_opendal` lib.
pub fn register_submodules(module: &Bound<'_, PyModule>, parent_name: &str) -> PyResult<()> {
    let sys_modules = module.py().import("sys")?.getattr("modules")?;
    register_submodules_inner(module, parent_name, &sys_modules)
}

fn register_submodules_inner(
    module: &Bound<'_, PyModule>,
    parent_name: &str,
    sys_modules: &Bound<'_, PyAny>,
) -> PyResult<()> {
    for attr_name in module.index()? {
        let attr_name: String = attr_name.extract()?;
        let attr = module.getattr(&attr_name)?;
        if let Ok(submodule) = attr.cast::<PyModule>() {
            let qualified_name = format!("{parent_name}.{attr_name}");
            submodule.setattr("__name__", &qualified_name)?;
            sys_modules.set_item(&qualified_name, submodule)?;
            register_submodules_inner(submodule, &qualified_name, sys_modules)?;
        }
    }
    Ok(())
}

/// Add exception types to a module by their Rust identifier.
///
/// `create_exception!` types are `PyErr` subtypes, not `#[pyclass]`es, so they
/// cannot be listed with `#[pymodule_export]`.
#[macro_export]
macro_rules! add_exceptions {
    ($module:expr, [$($exc:ty),* $(,)?]) => {{
        $(
            $module.add(stringify!($exc), $module.py().get_type::<$exc>())?;
        )*
        Ok::<_, pyo3::PyErr>(())
    }};
}
