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

#[cfg(unix)]
use std::future::Future;
#[cfg(unix)]
use std::process;
#[cfg(unix)]
use std::sync::atomic::{AtomicU32, Ordering};

#[cfg(unix)]
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use tokio::runtime::Runtime;

#[cfg(unix)]
static RUNTIME_PID: AtomicU32 = AtomicU32::new(0);

#[cfg(unix)]
fn fork_error() -> PyErr {
    PyRuntimeError::new_err(
        "OpenDAL's Tokio runtime cannot be used after fork(). \
         Use multiprocessing.get_context(\"spawn\") or initialize OpenDAL only after fork().",
    )
}

pub(crate) fn runtime_is_inherited() -> bool {
    #[cfg(unix)]
    {
        let owner = RUNTIME_PID.load(Ordering::Acquire);
        owner != 0 && owner != process::id()
    }
    #[cfg(not(unix))]
    {
        false
    }
}

pub(crate) fn check_runtime_process() -> PyResult<()> {
    #[cfg(unix)]
    if runtime_is_inherited() {
        return Err(fork_error());
    }
    Ok(())
}

pub(crate) fn get_runtime() -> PyResult<&'static Runtime> {
    #[cfg(unix)]
    {
        let pid = process::id();
        let owner = RUNTIME_PID.load(Ordering::Acquire);
        let owner = if owner == 0 {
            // Publish ownership before touching Tokio's OnceLock, which a fork
            // can inherit while initialization is still in progress.
            RUNTIME_PID
                .compare_exchange(0, pid, Ordering::AcqRel, Ordering::Acquire)
                .err()
                .unwrap_or(pid)
        } else {
            owner
        };
        if owner != pid {
            return Err(fork_error());
        }
    }
    Ok(pyo3_async_runtimes::tokio::get_runtime())
}

#[cfg(not(unix))]
pub(crate) use pyo3_async_runtimes::tokio::future_into_py;

#[cfg(unix)]
pub(crate) fn future_into_py<F, T>(py: Python<'_>, future: F) -> PyResult<Bound<'_, PyAny>>
where
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: for<'py> IntoPyObject<'py> + Send + 'static,
{
    // Check before touching asyncio state inherited from the parent. Resolve
    // the Python loop before claiming the runtime, since this can fail.
    check_runtime_process()?;
    let locals = pyo3_async_runtimes::tokio::get_current_locals(py)?;
    get_runtime()?;
    pyo3_async_runtimes::tokio::future_into_py_with_locals(py, locals, future)
}
