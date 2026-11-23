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

use bytes::Buf;
use bytes::Bytes;
use pyo3::exceptions::PyOverflowError;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::PyAny;
use pyo3::types::PyBytes;

use crate::ocore;

/// Convert a timestamp from this binding's `jiff` version to OpenDAL's.
///
/// PyO3's `jiff-02` feature keeps the Python binding on jiff 0.2, while
/// `opendal-core` uses jiff 1.0. Both versions cover the same timestamp range.
pub fn timestamp_from_jiff(ts: jiff::Timestamp) -> ocore::raw::Timestamp {
    let duration = ts.as_duration();
    ocore::raw::Timestamp::new(duration.as_secs(), duration.subsec_nanos())
        .expect("jiff timestamp is within OpenDAL timestamp range")
}

/// Convert an OpenDAL timestamp to this binding's `jiff` version.
pub fn timestamp_to_jiff(ts: ocore::raw::Timestamp) -> jiff::Timestamp {
    let duration = ts.into_inner().as_duration();
    jiff::Timestamp::new(duration.as_secs(), duration.subsec_nanos())
        .expect("OpenDAL timestamp is within jiff timestamp range")
}

/// Keep immutable Python bytes alive inside an OpenDAL buffer without copying.
pub fn py_bytes_like_into_buffer(value: &Bound<PyAny>) -> PyResult<ocore::Buffer> {
    if let Ok(value) = value.cast::<PyBytes>() {
        let owner = PyBackedBytes::from(value.clone());
        return Ok(Bytes::from_owner(owner).into());
    }

    value.extract::<Vec<u8>>().map(Into::into)
}

/// Copy an OpenDAL buffer directly into a new Python `bytes` object.
pub fn buffer_into_py_bytes<'py>(
    py: Python<'py>,
    mut buffer: ocore::Buffer,
) -> PyResult<Bound<'py, PyBytes>> {
    let len = buffer.remaining();
    let py_len = len
        .try_into()
        .map_err(|_| PyOverflowError::new_err("buffer is too large for Python bytes"))?;

    unsafe {
        let bytes = Bound::from_owned_ptr_or_err(
            py,
            ffi::PyBytes_FromStringAndSize(std::ptr::null(), py_len),
        )?
        .cast_into_unchecked::<PyBytes>();

        // PyBytes_FromStringAndSize with a null source allocates an uninitialized
        // buffer. `buffer` has exactly `len` remaining bytes, so copy_to_slice
        // initializes the entire Python object before it becomes observable.
        let dst =
            std::slice::from_raw_parts_mut(ffi::PyBytes_AsString(bytes.as_ptr()).cast::<u8>(), len);
        buffer.copy_to_slice(dst);

        Ok(bytes)
    }
}

/// Recursively insert a module's nested `#[pymodule]` submodules into
/// `sys.modules` under `parent_name` and qualify their `__name__`, so
/// `from opendal.operator import ...` resolves.
///
/// PyO3 attaches submodules as attributes but skips `sys.modules` (PyO3 #759);
/// `parent_name` lets us use the public `opendal` name, not the `_opendal` lib.
pub fn register_submodules(module: &Bound<'_, PyModule>, parent_name: &str) -> PyResult<()> {
    let sys_modules = module.py().import("sys")?.getattr("modules")?;
    for attr_name in module.index()? {
        let attr_name: String = attr_name.extract()?;
        let attr = module.getattr(&attr_name)?;
        if let Ok(submodule) = attr.cast::<PyModule>() {
            let qualified_name = format!("{parent_name}.{attr_name}");
            submodule.setattr("__name__", &qualified_name)?;
            sys_modules.set_item(&qualified_name, submodule)?;
            register_submodules(submodule, &qualified_name)?;
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

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use pyo3::types::PyBytesMethods;

    use super::*;

    #[test]
    fn test_timestamp_jiff_conversion_preserves_nanoseconds() {
        let ts = jiff::Timestamp::new(1_646_122_354, 123_456_789).unwrap();
        let opendal_ts = timestamp_from_jiff(ts);

        assert_eq!(opendal_ts.into_inner().as_second(), 1_646_122_354);
        assert_eq!(opendal_ts.into_inner().subsec_nanosecond(), 123_456_789);
        assert_eq!(timestamp_to_jiff(opendal_ts), ts);
    }

    #[test]
    fn test_timestamp_jiff_conversion_preserves_pre_epoch_nanoseconds() {
        let ts = "1969-12-31T23:59:59.123456789Z"
            .parse::<jiff::Timestamp>()
            .unwrap();
        let opendal_ts = timestamp_from_jiff(ts);

        assert_eq!(
            opendal_ts.into_inner().to_string(),
            "1969-12-31T23:59:59.123456789Z"
        );
        assert_eq!(timestamp_to_jiff(opendal_ts), ts);
    }

    #[test]
    fn test_buffer_into_py_bytes() {
        Python::initialize();
        Python::attach(|py| {
            let buffer = [Bytes::from_static(b"hello, "), Bytes::from_static(b"world")]
                .into_iter()
                .collect();
            let result = buffer_into_py_bytes(py, buffer).unwrap();
            assert_eq!(result.as_bytes(), b"hello, world");

            let result = buffer_into_py_bytes(py, ocore::Buffer::new()).unwrap();
            assert!(result.as_bytes().is_empty());
        });
    }

    #[test]
    fn test_py_bytes_like_into_buffer() {
        Python::initialize();
        let (buffer, source_ptr) = Python::attach(|py| {
            let value = PyBytes::new(py, b"hello, world");
            let source_ptr = value.as_bytes().as_ptr();
            let buffer = py_bytes_like_into_buffer(value.as_any()).unwrap();
            (buffer, source_ptr)
        });

        assert_eq!(buffer.current().as_ptr(), source_ptr);
        assert_eq!(buffer.to_vec(), b"hello, world");

        let buffer = Python::attach(|py| {
            let value = pyo3::types::PyByteArray::new(py, b"mutable");
            let buffer = py_bytes_like_into_buffer(value.as_any()).unwrap();
            value.set_item(0, b'M').unwrap();
            buffer
        });
        assert_eq!(buffer.to_vec(), b"mutable");
    }
}
