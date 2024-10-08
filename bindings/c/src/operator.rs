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

use std::collections::HashMap;
use std::ffi::c_void;
use std::os::raw::c_char;
use std::str::FromStr;

use ::opendal as core;
use once_cell::sync::Lazy;

use super::*;

static RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
});

/// \brief Used to access almost all OpenDAL APIs. It represents an
/// operator that provides the unified interfaces provided by OpenDAL.
///
/// @see opendal_operator_new This function construct the operator
/// @see opendal_operator_free This function frees the heap memory of the operator
///
/// \note The opendal_operator actually owns a pointer to
/// an opendal::BlockingOperator, which is inside the Rust core code.
///
/// \remark You may use the field `ptr` to check whether this is a NULL
/// operator.
#[repr(C)]
pub struct opendal_operator {
    /// The pointer to the opendal::BlockingOperator in the Rust code.
    /// Only touch this on judging whether it is NULL.
    inner: *mut c_void,
}

impl opendal_operator {
    pub(crate) fn deref(&self) -> &core::BlockingOperator {
        // Safety: the inner should never be null once constructed
        // The use-after-free is undefined behavior
        unsafe { &*(self.inner as *mut core::BlockingOperator) }
    }
}

impl opendal_operator {
    /// \brief Free the heap-allocated operator pointed by opendal_operator.
    ///
    /// Please only use this for a pointer pointing at a valid opendal_operator.
    /// Calling this function on NULL does nothing, but calling this function on pointers
    /// of other type will lead to segfault.
    ///
    /// # Example
    ///
    /// ```C
    /// opendal_operator *op = opendal_operator_new("fs", NULL);
    /// // ... use this op, maybe some reads and writes
    ///
    /// // free this operator
    /// opendal_operator_free(op);
    /// ```
    #[no_mangle]
    pub unsafe extern "C" fn opendal_operator_free(ptr: *const opendal_operator) {
        if !ptr.is_null() {
            drop(Box::from_raw((*ptr).inner as *mut core::BlockingOperator));
            drop(Box::from_raw(ptr as *mut opendal_operator));
        }
    }
}

fn build_operator(
    schema: core::Scheme,
    map: HashMap<String, String>,
) -> core::Result<core::Operator> {
    let mut op = match core::Operator::via_iter(schema, map) {
        Ok(o) => o,
        Err(e) => return Err(e),
    };
    if !op.info().full_capability().blocking {
        let runtime =
            tokio::runtime::Handle::try_current().unwrap_or_else(|_| RUNTIME.handle().clone());
        let _guard = runtime.enter();
        op = op
            .layer(core::layers::BlockingLayer::create().expect("blocking layer must be created"));
    }
    Ok(op)
}

/// \brief Construct an operator based on `scheme` and `options`
///
/// Uses an array of key-value pairs to initialize the operator based on provided `scheme`
/// and `options`. For each scheme, i.e. Backend, different options could be set, you may
/// reference the [documentation](https://opendal.apache.org/docs/category/services/) for
/// each service, especially for the **Configuration Part**.
///
/// @param scheme the service scheme you want to specify, e.g. "fs", "s3", "supabase"
/// @param options the pointer to the options for this operator, it could be NULL, which means no
/// option is set
/// @see opendal_operator_options
/// @return A valid opendal_result_operator_new setup with the `scheme` and `options` is the construction
/// succeeds. On success the operator field is a valid pointer to a newly allocated opendal_operator,
/// and the error field is NULL. Otherwise, the operator field is a NULL pointer and the error field.
///
/// # Example
///
/// Following is an example.
/// ```C
/// // Allocate a new options
/// opendal_operator_options *options = opendal_operator_options_new();
/// // Set the options you need
/// opendal_operator_options_set(options, "root", "/myroot");
///
/// // Construct the operator based on the options and scheme
/// opendal_result_operator_new result = opendal_operator_new("memory", options);
/// opendal_operator* op = result.op;
///
/// // you could free the options right away since the options is not used afterwards
/// opendal_operator_options_free(options);
///
/// // ... your operations
/// ```
///
/// # Safety
///
/// The only unsafe case is passing an invalid c string pointer to the `scheme` argument.
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_new(
    scheme: *const c_char,
    options: *const opendal_operator_options,
) -> opendal_result_operator_new {
    assert!(!scheme.is_null());
    let scheme = std::ffi::CStr::from_ptr(scheme)
        .to_str()
        .expect("malformed scheme");
    let scheme = match core::Scheme::from_str(scheme) {
        Ok(s) => s,
        Err(e) => {
            return opendal_result_operator_new {
                op: std::ptr::null_mut(),
                error: opendal_error::new(e),
            };
        }
    };

    let mut map = HashMap::<String, String>::default();
    if !options.is_null() {
        for (k, v) in (*options).deref() {
            map.insert(k.to_string(), v.to_string());
        }
    }

    match build_operator(scheme, map) {
        Ok(op) => opendal_result_operator_new {
            op: Box::into_raw(Box::new(opendal_operator {
                inner: Box::into_raw(Box::new(op.blocking())) as _,
            })),
            error: std::ptr::null_mut(),
        },
        Err(e) => opendal_result_operator_new {
            op: std::ptr::null_mut(),
            error: opendal_error::new(e),
        },
    }
}

/// \brief Blocking write raw bytes to `path`.
///
/// Write the `bytes` into the `path` blocking by `op_ptr`.
/// Error is NULL if successful, otherwise it contains the error code and error message.
///
/// \note It is important to notice that the `bytes` that is passes in will be consumed by this
///       function. Therefore, you should not use the `bytes` after this function returns.
///
/// @param op The opendal_operator created previously
/// @param path The designated path you want to write your bytes in
/// @param bytes The opendal_byte typed bytes to be written
/// @see opendal_operator
/// @see opendal_bytes
/// @see opendal_error
/// @return NULL if succeeds, otherwise it contains the error code and error message.
///
/// # Example
///
/// Following is an example
/// ```C
/// //...prepare your opendal_operator, named op for example
///
/// // prepare your data
/// char* data = "Hello, World!";
/// opendal_bytes bytes = opendal_bytes { .data = (uint8_t*)data, .len = 13 };
///
/// // now you can write!
/// opendal_error *err = opendal_operator_write(op, "/testpath", bytes);
///
/// // Assert that this succeeds
/// assert(err == NULL);
/// ```
///
/// # Safety
///
/// It is **safe** under the cases below
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
/// * The `bytes` provided has valid byte in the `data` field and the `len` field is set
///   correctly.
///
/// # Panic
///
/// * If the `path` points to NULL, this function panics, i.e. exits with information
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_write(
    op: &opendal_operator,
    path: *const c_char,
    bytes: &opendal_bytes,
) -> *mut opendal_error {
    assert!(!path.is_null());
    let path = std::ffi::CStr::from_ptr(path)
        .to_str()
        .expect("malformed path");
    match op.deref().write(path, bytes) {
        Ok(_) => std::ptr::null_mut(),
        Err(e) => opendal_error::new(e),
    }
}

/// \brief Blocking read the data from `path`.
///
/// Read the data out from `path` blocking by operator.
///
/// @param op The opendal_operator created previously
/// @param path The path you want to read the data out
/// @see opendal_operator
/// @see opendal_result_read
/// @see opendal_error
/// @return Returns opendal_result_read, the `data` field is a pointer to a newly allocated
/// opendal_bytes, the `error` field contains the error. If the `error` is not NULL, then
/// the operation failed and the `data` field is a nullptr.
///
/// \note If the read operation succeeds, the returned opendal_bytes is newly allocated on heap.
/// After your usage of that, please call opendal_bytes_free() to free the space.
///
/// # Example
///
/// Following is an example
/// ```C
/// // ... you have write "Hello, World!" to path "/testpath"
///
/// opendal_result_read r = opendal_operator_read(op, "testpath");
/// assert(r.error == NULL);
///
/// opendal_bytes bytes = r.data;
/// assert(bytes.len == 13);
/// opendal_bytes_free(bytes);
/// ```
///
/// # Safety
///
/// It is **safe** under the cases below
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
///
/// # Panic
///
/// * If the `path` points to NULL, this function panics, i.e. exits with information
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_read(
    op: &opendal_operator,
    path: *const c_char,
) -> opendal_result_read {
    assert!(!path.is_null());
    let path = std::ffi::CStr::from_ptr(path)
        .to_str()
        .expect("malformed path");
    match op.deref().read(path) {
        Ok(b) => opendal_result_read {
            data: opendal_bytes::new(b),
            error: std::ptr::null_mut(),
        },
        Err(e) => opendal_result_read {
            data: opendal_bytes::empty(),
            error: opendal_error::new(e),
        },
    }
}

/// \brief Blocking read the data from `path`.
///
/// Read the data out from `path` blocking by operator, returns
/// an opendal_result_read with error code.
///
/// @param op The opendal_operator created previously
/// @param path The path you want to read the data out
/// @see opendal_operator
/// @see opendal_result_read
/// @see opendal_code
/// @return Returns opendal_code
///
/// \note If the read operation succeeds, the returned opendal_bytes is newly allocated on heap.
/// After your usage of that, please call opendal_bytes_free() to free the space.
///
/// # Example
///
/// Following is an example
/// ```C
/// // ... you have created an operator named op
///
/// opendal_result_operator_reader result = opendal_operator_reader(op, "/testpath");
/// assert(result.error == NULL);
/// // The reader is in result.reader
/// opendal_reader *reader = result.reader;
/// ```
///
/// # Safety
///
/// It is **safe** under the cases below
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
///
/// # Panic
///
/// * If the `path` points to NULL, this function panics, i.e. exits with information
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_reader(
    op: &opendal_operator,
    path: *const c_char,
) -> opendal_result_operator_reader {
    assert!(!path.is_null());
    let path = std::ffi::CStr::from_ptr(path)
        .to_str()
        .expect("malformed path");
    let reader = match op.deref().reader(path) {
        Ok(reader) => reader,
        Err(err) => {
            return opendal_result_operator_reader {
                reader: std::ptr::null_mut(),
                error: opendal_error::new(err),
            }
        }
    };

    match reader.into_std_read(..) {
        Ok(reader) => opendal_result_operator_reader {
            reader: Box::into_raw(Box::new(opendal_reader::new(reader))),
            error: std::ptr::null_mut(),
        },
        Err(e) => opendal_result_operator_reader {
            reader: std::ptr::null_mut(),
            error: opendal_error::new(e),
        },
    }
}

/// \brief Blocking create a writer for the specified path.
///
/// This function prepares a writer that can be used to write data to the specified path
/// using the provided operator. If successful, it returns a valid writer; otherwise, it
/// returns an error.
///
/// @param op The opendal_operator created previously
/// @param path The designated path where the writer will be used
/// @see opendal_operator
/// @see opendal_result_operator_writer
/// @see opendal_error
/// @return Returns opendal_result_operator_writer, containing a writer and an opendal_error.
/// If the operation succeeds, the `writer` field holds a valid writer and the `error` field
/// is null. Otherwise, the `writer` will be null and the `error` will be set correspondingly.
///
/// # Example
///
/// Following is an example
/// ```C
/// //...prepare your opendal_operator, named op for example
///
/// opendal_result_operator_writer result = opendal_operator_writer(op, "/testpath");
/// assert(result.error == NULL);
/// opendal_writer *writer = result.writer;
/// // Use the writer to write data...
/// ```
///
/// # Safety
///
/// It is **safe** under the cases below
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
///
/// # Panic
///
/// * If the `path` points to NULL, this function panics, i.e. exits with information
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_writer(
    op: &opendal_operator,
    path: *const c_char,
) -> opendal_result_operator_writer {
    assert!(!path.is_null());
    let path = std::ffi::CStr::from_ptr(path)
        .to_str()
        .expect("malformed path");
    let writer = match op.deref().writer(path) {
        Ok(writer) => writer,
        Err(err) => {
            return opendal_result_operator_writer {
                writer: std::ptr::null_mut(),
                error: opendal_error::new(err),
            }
        }
    };

    opendal_result_operator_writer {
        writer: Box::into_raw(Box::new(opendal_writer::new(writer))),
        error: std::ptr::null_mut(),
    }
}

/// \brief Blocking delete the object in `path`.
///
/// Delete the object in `path` blocking by `op_ptr`.
/// Error is NULL if successful, otherwise it contains the error code and error message.
///
/// @param op The opendal_operator created previously
/// @param path The designated path you want to delete
/// @see opendal_operator
/// @see opendal_error
/// @return NULL if succeeds, otherwise it contains the error code and error message.
///
/// # Example
///
/// Following is an example
/// ```C
/// //...prepare your opendal_operator, named op for example
///
/// // prepare your data
/// char* data = "Hello, World!";
/// opendal_bytes bytes = opendal_bytes { .data = (uint8_t*)data, .len = 13 };
/// opendal_error *error = opendal_operator_write(op, "/testpath", bytes);
///
/// assert(error == NULL);
///
/// // now you can delete!
/// opendal_error *error = opendal_operator_delete(op, "/testpath");
///
/// // Assert that this succeeds
/// assert(error == NULL);
/// ```
///
/// # Safety
///
/// It is **safe** under the cases below
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
///
/// # Panic
///
/// * If the `path` points to NULL, this function panics, i.e. exits with information
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_delete(
    op: &opendal_operator,
    path: *const c_char,
) -> *mut opendal_error {
    assert!(!path.is_null());
    let path = std::ffi::CStr::from_ptr(path)
        .to_str()
        .expect("malformed path");
    match op.deref().delete(path) {
        Ok(_) => std::ptr::null_mut(),
        Err(e) => opendal_error::new(e),
    }
}

/// \brief Check whether the path exists.
///
/// If the operation succeeds, no matter the path exists or not,
/// the error should be a nullptr. Otherwise, the field `is_exist`
/// is filled with false, and the error is set
///
/// @param op The opendal_operator created previously
/// @param path The path you want to check existence
/// @see opendal_operator
/// @see opendal_result_is_exist
/// @see opendal_error
/// @return Returns opendal_result_is_exist, the `is_exist` field contains whether the path exists.
/// However, it the operation fails, the `is_exist` will contain false and the error will be set.
///
/// # Example
///
/// ```C
/// // .. you previously wrote some data to path "/mytest/obj"
/// opendal_result_is_exist e = opendal_operator_is_exist(op, "/mytest/obj");
/// assert(e.error == NULL);
/// assert(e.is_exist);
///
/// // but you previously did **not** write any data to path "/yourtest/obj"
/// opendal_result_is_exist e = opendal_operator_is_exist(op, "/yourtest/obj");
/// assert(e.error == NULL);
/// assert(!e.is_exist);
/// ```
///
/// # Safety
///
/// It is **safe** under the cases below
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
///
/// # Panic
///
/// * If the `path` points to NULL, this function panics, i.e. exits with information
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_is_exist(
    op: &opendal_operator,
    path: *const c_char,
) -> opendal_result_is_exist {
    assert!(!path.is_null());
    let path = std::ffi::CStr::from_ptr(path)
        .to_str()
        .expect("malformed path");
    match op.deref().is_exist(path) {
        Ok(e) => opendal_result_is_exist {
            is_exist: e,
            error: std::ptr::null_mut(),
        },
        Err(e) => opendal_result_is_exist {
            is_exist: false,
            error: opendal_error::new(e),
        },
    }
}

/// \brief Stat the path, return its metadata.
///
/// Error is NULL if successful, otherwise it contains the error code and error message.
///
/// @param op The opendal_operator created previously
/// @param path The path you want to stat
/// @see opendal_operator
/// @see opendal_result_stat
/// @see opendal_metadata
/// @return Returns opendal_result_stat, containing a metadata and an opendal_error.
/// If the operation succeeds, the `meta` field would hold a valid metadata and
/// the `error` field should hold nullptr. Otherwise, the metadata will contain a
/// NULL pointer, i.e. invalid, and the `error` will be set correspondingly.
///
/// # Example
///
/// ```C
/// // ... previously you wrote "Hello, World!" to path "/testpath"
/// opendal_result_stat s = opendal_operator_stat(op, "/testpath");
/// assert(s.error == NULL);
///
/// const opendal_metadata *meta = s.meta;
///
/// // ... you could now use your metadata, notice that please only access metadata
/// // using the APIs provided by OpenDAL
/// ```
///
/// # Safety
///
/// It is **safe** under the cases below
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
///
/// # Panic
///
/// * If the `path` points to NULL, this function panics, i.e. exits with information
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_stat(
    op: &opendal_operator,
    path: *const c_char,
) -> opendal_result_stat {
    assert!(!path.is_null());
    let path = std::ffi::CStr::from_ptr(path)
        .to_str()
        .expect("malformed path");
    match op.deref().stat(path) {
        Ok(m) => opendal_result_stat {
            meta: Box::into_raw(Box::new(opendal_metadata::new(m))),
            error: std::ptr::null_mut(),
        },
        Err(e) => opendal_result_stat {
            meta: std::ptr::null_mut(),
            error: opendal_error::new(e),
        },
    }
}

/// \brief Blocking list the objects in `path`.
///
/// List the object in `path` blocking by `op_ptr`, return a result with an
/// opendal_lister. Users should call opendal_lister_next() on the
/// lister.
///
/// @param op The opendal_operator created previously
/// @param path The designated path you want to list
/// @see opendal_lister
/// @return Returns opendal_result_list, containing a lister and an opendal_error.
/// If the operation succeeds, the `lister` field would hold a valid lister and
/// the `error` field should hold nullptr. Otherwise, the `lister`` will contain a
/// NULL pointer, i.e. invalid, and the `error` will be set correspondingly.
///
/// # Example
///
/// Following is an example
/// ```C
/// // You have written some data into some files path "root/dir1"
/// // Your opendal_operator was called op
/// opendal_result_list l = opendal_operator_list(op, "root/dir1");
/// assert(l.error == ERROR);
///
/// opendal_lister *lister = l.lister;
/// opendal_list_entry *entry;
///
/// while ((entry = opendal_lister_next(lister)) != NULL) {
///     const char* de_path = opendal_list_entry_path(entry);
///     const char* de_name = opendal_list_entry_name(entry);
///     // ...... your operations
///
///     // remember to free the entry after you are done using it
///     opendal_list_entry_free(entry);
/// }
///
/// // and remember to free the lister
/// opendal_lister_free(lister);
/// ```
///
/// # Safety
///
/// It is **safe** under the cases below
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
///
/// # Panic
///
/// * If the `path` points to NULL, this function panics, i.e. exits with information
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_list(
    op: &opendal_operator,
    path: *const c_char,
) -> opendal_result_list {
    assert!(!path.is_null());
    let path = std::ffi::CStr::from_ptr(path)
        .to_str()
        .expect("malformed path");
    match op.deref().lister(path) {
        Ok(lister) => opendal_result_list {
            lister: Box::into_raw(Box::new(opendal_lister::new(lister))),
            error: std::ptr::null_mut(),
        },
        Err(e) => opendal_result_list {
            lister: std::ptr::null_mut(),
            error: opendal_error::new(e),
        },
    }
}

/// \brief Blocking create the directory in `path`.
///
/// Create the directory in `path` blocking by `op_ptr`.
/// Error is NULL if successful, otherwise it contains the error code and error message.
///
/// @param op The opendal_operator created previously
/// @param path The designated directory you want to create
/// @see opendal_operator
/// @see opendal_error
/// @return NULL if succeeds, otherwise it contains the error code and error message.
///
/// # Example
///
/// Following is an example
/// ```C
/// //...prepare your opendal_operator, named op for example
///
/// // create your directory
/// opendal_error *error = opendal_operator_create_dir(op, "/testdir/");
///
/// // Assert that this succeeds
/// assert(error == NULL);
/// ```
///
/// # Safety
///
/// It is **safe** under the cases below
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
///
/// # Panic
///
/// * If the `path` points to NULL, this function panics, i.e. exits with information
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_create_dir(
    op: &opendal_operator,
    path: *const c_char,
) -> *mut opendal_error {
    assert!(!path.is_null());
    let path = std::ffi::CStr::from_ptr(path)
        .to_str()
        .expect("malformed path");
    if let Err(err) = op.deref().create_dir(path) {
        opendal_error::new(err)
    } else {
        std::ptr::null_mut()
    }
}

/// \brief Blocking rename the object in `path`.
///
/// Rename the object in `src` to `dest` blocking by `op`.
/// Error is NULL if successful, otherwise it contains the error code and error message.
///
/// @param op The opendal_operator created previously
/// @param src The designated source path you want to rename
/// @param dest The designated destination path you want to rename
/// @see opendal_operator
/// @see opendal_error
/// @return NULL if succeeds, otherwise it contains the error code and error message.
///
/// # Example
///
/// Following is an example
/// ```C
/// //...prepare your opendal_operator, named op for example
///
/// // prepare your data
/// char* data = "Hello, World!";
/// opendal_bytes bytes = opendal_bytes { .data = (uint8_t*)data, .len = 13 };
/// opendal_error *error = opendal_operator_write(op, "/testpath", bytes);
///
/// assert(error == NULL);
///
/// // now you can rename!
/// opendal_error *error = opendal_operator_rename(op, "/testpath", "/testpath2");
///
/// // Assert that this succeeds
/// assert(error == NULL);
/// ```
///
/// # Safety
///
/// It is **safe** under the cases below
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
///
/// # Panic
///
/// * If the `src` or `dest` points to NULL, this function panics, i.e. exits with information
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_rename(
    op: &opendal_operator,
    src: *const c_char,
    dest: *const c_char,
) -> *mut opendal_error {
    assert!(!src.is_null());
    assert!(!dest.is_null());
    let src = std::ffi::CStr::from_ptr(src)
        .to_str()
        .expect("malformed src");
    let dest = std::ffi::CStr::from_ptr(dest)
        .to_str()
        .expect("malformed dest");
    if let Err(err) = op.deref().rename(src, dest) {
        opendal_error::new(err)
    } else {
        std::ptr::null_mut()
    }
}

/// \brief Blocking copy the object in `path`.
///
/// Copy the object in `src` to `dest` blocking by `op`.
/// Error is NULL if successful, otherwise it contains the error code and error message.
///
/// @param op The opendal_operator created previously
/// @param src The designated source path you want to copy
/// @param dest The designated destination path you want to copy
/// @see opendal_operator
/// @see opendal_error
/// @return NULL if succeeds, otherwise it contains the error code and error message.
///
/// # Example
///
/// Following is an example
/// ```C
/// //...prepare your opendal_operator, named op for example
///
/// // prepare your data
/// char* data = "Hello, World!";
/// opendal_bytes bytes = opendal_bytes { .data = (uint8_t*)data, .len = 13 };
/// opendal_error *error = opendal_operator_write(op, "/testpath", bytes);
///
/// assert(error == NULL);
///
/// // now you can rename!
/// opendal_error *error = opendal_operator_copy(op, "/testpath", "/testpath2");
///
/// // Assert that this succeeds
/// assert(error == NULL);
/// ```
///
/// # Safety
///
/// It is **safe** under the cases below
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
///
/// # Panic
///
/// * If the `src` or `dest` points to NULL, this function panics, i.e. exits with information
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_copy(
    op: &opendal_operator,
    src: *const c_char,
    dest: *const c_char,
) -> *mut opendal_error {
    assert!(!src.is_null());
    assert!(!dest.is_null());
    let src = std::ffi::CStr::from_ptr(src)
        .to_str()
        .expect("malformed src");
    let dest = std::ffi::CStr::from_ptr(dest)
        .to_str()
        .expect("malformed dest");
    if let Err(err) = op.deref().copy(src, dest) {
        opendal_error::new(err)
    } else {
        std::ptr::null_mut()
    }
}
