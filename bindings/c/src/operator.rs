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
use std::sync::LazyLock;

use ::opendal as core;

use super::*;
use crate::cancel::block_on_cancellable;

static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
});

pub(crate) struct OperatorInner {
    handle: tokio::runtime::Handle,
    op: Option<core::Operator>,
}

impl OperatorInner {
    fn new(handle: tokio::runtime::Handle, op: core::Operator) -> Self {
        Self {
            handle,
            op: Some(op),
        }
    }

    fn handle(&self) -> &tokio::runtime::Handle {
        &self.handle
    }

    fn op(&self) -> &core::Operator {
        self.op.as_ref().expect("operator has been dropped")
    }
}

impl Drop for OperatorInner {
    fn drop(&mut self) {
        if let Some(op) = self.op.take() {
            let _guard = self.handle.enter();
            drop(op);
        }
    }
}

/// \brief Used to access almost all OpenDAL APIs. It represents an
/// operator that provides the unified interfaces provided by OpenDAL.
///
/// @see opendal_operator_new This function construct the operator
/// @see opendal_operator_free This function frees the heap memory of the operator
///
/// \note The opendal_operator owns an async `opendal::Operator` together with
/// the Tokio runtime handle used to drive its operations.
///
/// \remark You may use the field `ptr` to check whether this is a NULL
/// operator.
#[repr(C)]
pub struct opendal_operator {
    /// The pointer to the internal Rust operator state.
    /// Only touch this on judging whether it is NULL.
    inner: *mut c_void,
}

impl opendal_operator {
    fn inner(&self) -> &OperatorInner {
        // Safety: the inner should never be null once constructed
        // The use-after-free is undefined behavior
        unsafe { &*(self.inner as *const OperatorInner) }
    }

    pub(crate) fn deref(&self) -> &core::Operator {
        self.inner().op()
    }

    pub(crate) fn handle(&self) -> &tokio::runtime::Handle {
        self.inner().handle()
    }

    #[cfg(test)]
    pub(crate) fn from_operator(handle: tokio::runtime::Handle, op: core::Operator) -> *mut Self {
        let inner = OperatorInner::new(handle, op);
        Box::into_raw(Box::new(Self {
            inner: Box::into_raw(Box::new(inner)) as _,
        }))
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
    /// opendal_operator *op = NULL;
    /// opendal_error *err = opendal_operator_new("fs", NULL, &op);
    /// // ... use this op, maybe some reads and writes
    ///
    /// // free this operator
    /// opendal_operator_free(op);
    /// ```
    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn opendal_operator_free(ptr: *const opendal_operator) {
        unsafe {
            if !ptr.is_null() {
                drop(Box::from_raw((*ptr).inner as *mut OperatorInner));
                drop(Box::from_raw(ptr as *mut opendal_operator));
            }
        }
    }
}

fn current_or_global_handle() -> tokio::runtime::Handle {
    tokio::runtime::Handle::try_current().unwrap_or_else(|_| RUNTIME.handle().clone())
}

fn build_operator(schema: &str, map: HashMap<String, String>) -> core::Result<OperatorInner> {
    core::install_default();

    let handle = current_or_global_handle();
    let _guard = handle.enter();
    let op = core::Operator::via_iter(schema, map)?
        .layer(core::layers::RetryLayer::new())
        .layer(core::layers::LoggingLayer::default());

    Ok(OperatorInner::new(handle, op))
}

fn build_operator_with_layers(
    schema: &str,
    map: HashMap<String, String>,
    layers: *const opendal_operator_layers,
) -> core::Result<OperatorInner> {
    core::install_default();

    let handle = current_or_global_handle();
    let _guard = handle.enter();
    let mut op = core::Operator::via_iter(schema, map)?;
    if !layers.is_null() {
        op = unsafe { (*layers).apply(op) };
    }

    Ok(OperatorInner::new(handle, op))
}

fn parse_operator_options(options: *const opendal_operator_options) -> HashMap<String, String> {
    let mut map = HashMap::<String, String>::default();
    if !options.is_null() {
        unsafe {
            for (k, v) in (*options).deref() {
                map.insert(k.to_string(), v.to_string());
            }
        }
    }
    map
}

unsafe fn new_operator_result(
    res: core::Result<OperatorInner>,
    op_out: *mut *mut opendal_operator,
) -> *mut opendal_error {
    assert!(!op_out.is_null());
    match res {
        Ok(op) => {
            let boxed = Box::into_raw(Box::new(opendal_operator {
                inner: Box::into_raw(Box::new(op)) as _,
            }));
            std::ptr::write(op_out, boxed);
            std::ptr::null_mut()
        }
        Err(e) => {
            std::ptr::write(op_out, std::ptr::null_mut());
            opendal_error::new(e)
        }
    }
}

/// \brief Construct an operator based on `scheme` and `options`
///
/// Pass service configuration and any `context.<name>.*`, `layers`, and
/// `layer.<name>.*` options as key-value pairs from trusted sources. The linked
/// library must include the selected service and configured providers.
///
/// The `layers` option lists layer names from inner to outer, separated by
/// commas. This constructor appends the C binding's default retry and logging
/// layers after the configured layers. In particular, `layers=retry` adds an
/// inner retry layer beneath the default retry layer. Use
/// opendal_operator_new_with_layers() with `layers` set to NULL to use only
/// the configured layers in addition to OpenDAL's core layers.
///
/// @param scheme the service scheme you want to specify, e.g. "fs", "s3"
/// @param options the pointer to the options for this operator, it could be NULL, which means no
/// option is set
/// @param op output pointer receiving the newly allocated `opendal_operator *` on success, or NULL on failure
/// @see opendal_operator_options
/// @return NULL if construction succeeds, or an `opendal_error *` on failure.
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
/// opendal_operator *op = NULL;
/// opendal_error *err = opendal_operator_new("memory", options, &op);
///
/// // you could free the options right away since the options is not used afterwards
/// opendal_operator_options_free(options);
///
/// // ... your operations
/// ```
///
/// # Safety
///
/// `scheme` must be a valid nul-terminated C string and `op` must be a valid, aligned,
/// writable pointer.
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_new(
    scheme: *const c_char,
    options: *const opendal_operator_options,
    op: *mut *mut opendal_operator,
) -> *mut opendal_error {
    assert!(!scheme.is_null());
    assert!(!op.is_null());
    let scheme = std::ffi::CStr::from_ptr(scheme)
        .to_str()
        .expect("malformed scheme");

    let map = parse_operator_options(options);

    new_operator_result(build_operator(scheme, map), op)
}

/// \brief Construct an operator based on scheme, options, and explicit layers.
///
/// This constructor uses the same configuration options as
/// opendal_operator_new(). It appends the layers in the C handle in insertion
/// order, after any layers configured in `options`. Pass NULL for the C handle
/// to keep only the configured layers and OpenDAL's core layers.
///
/// This constructor omits the C binding's default retry and logging layers.
///
/// @param scheme The service scheme.
/// @param options The configuration options, or NULL to use defaults.
/// @param layers The C layer handle, or NULL to append no explicit layers.
/// @param op Output pointer receiving the newly allocated `opendal_operator *` on success, or NULL on failure.
/// @return NULL if construction succeeds, or an `opendal_error *` on failure.
///
/// # Safety
///
/// `scheme` must be a valid nul-terminated C string and `op` must be a valid, aligned,
/// writable pointer.
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_new_with_layers(
    scheme: *const c_char,
    options: *const opendal_operator_options,
    layers: *const opendal_operator_layers,
    op: *mut *mut opendal_operator,
) -> *mut opendal_error {
    assert!(!scheme.is_null());
    assert!(!op.is_null());
    let scheme = std::ffi::CStr::from_ptr(scheme)
        .to_str()
        .expect("malformed scheme");

    let map = parse_operator_options(options);

    new_operator_result(build_operator_with_layers(scheme, map, layers), op)
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
/// * If `bytes.len` is greater than 0, `bytes.data` must point to at least
///   `bytes.len` valid bytes. If `bytes.len` is 0, `bytes.data` must be NULL.
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
    opendal_operator_write_with_cancel(
        op,
        path,
        bytes,
        std::ptr::null(),
        std::ptr::null(),
        std::ptr::null_mut(),
    )
}

/// \brief Blocking write raw bytes to `path` with options.
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_write_with(
    op: &opendal_operator,
    path: *const c_char,
    bytes: &opendal_bytes,
    opts: *const opendal_write_options,
) -> *mut opendal_error {
    opendal_operator_write_with_cancel(
        op,
        path,
        bytes,
        opts,
        std::ptr::null(),
        std::ptr::null_mut(),
    )
}

/// \brief Blocking write raw bytes to `path`, returning the written object's metadata.
///
/// Like `opendal_operator_write_with`, but on success writes the metadata of the
/// just-written object (e.g. etag, version, last modified) to `*meta` instead of discarding it.
/// A NULL `opts` is treated as the default options, behaving like a plain write.
///
/// @param op The opendal_operator created previously
/// @param path The designated path where the data will be written
/// @param bytes The data to write
/// @param opts The write options, or NULL to use the defaults
/// @param meta Output pointer receiving the allocated `opendal_metadata *` on success, or NULL on failure
/// @see opendal_operator
/// @see opendal_write_options
/// @return NULL if the operation succeeds, or an `opendal_error *` on failure.
///
/// \note The returned metadata must be freed with opendal_metadata_free().
///
/// # Safety
///
/// It is **safe** under the cases below
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
/// * The `bytes` provided has valid byte in the `data` field and the `len` field is set
///   correctly.
/// * `meta` must be a valid, aligned, writable pointer.
///
/// # Panic
///
/// * If `path` or `meta` points to NULL, this function panics, i.e. exits with information
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_write_with_metadata(
    op: &opendal_operator,
    path: *const c_char,
    bytes: &opendal_bytes,
    opts: *const opendal_write_options,
    meta: *mut *mut opendal_metadata,
) -> *mut opendal_error {
    assert!(!meta.is_null());
    opendal_operator_write_with_cancel(op, path, bytes, opts, std::ptr::null(), meta)
}

/// \brief Blocking write raw bytes to `path` with options, optional cancellation, and optional metadata output.
///
/// Pass NULL for `opts` to use defaults, NULL for `cancel` to run without cancellation,
/// and NULL for `meta` to discard the written object's metadata. When `meta` is non-null,
/// `*meta` receives the allocated `opendal_metadata *` on success, or NULL on cancellation
/// or failure.
///
/// @param op The opendal_operator created previously
/// @param path The designated path where the data will be written
/// @param bytes The data to write
/// @param opts The write options, or NULL to use the defaults
/// @param cancel Optional cancel token, or NULL
/// @param meta Optional output pointer receiving the allocated `opendal_metadata *`, or NULL
/// @return NULL if the operation succeeds, or an `opendal_error *` on cancellation or failure.
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_write_with_cancel(
    op: &opendal_operator,
    path: *const c_char,
    bytes: &opendal_bytes,
    opts: *const opendal_write_options,
    cancel: *const opendal_cancel_token,
    meta: *mut *mut opendal_metadata,
) -> *mut opendal_error {
    assert!(!path.is_null());
    if !meta.is_null() {
        std::ptr::write(meta, std::ptr::null_mut());
    }
    let path = std::ffi::CStr::from_ptr(path)
        .to_str()
        .expect("malformed path");
    let opts = if opts.is_null() {
        core::options::WriteOptions::default()
    } else {
        (&*opts).into()
    };
    let bytes = match bytes.to_buffer() {
        Ok(bytes) => bytes,
        Err(e) => return opendal_error::new(e),
    };
    let cancel_token = opendal_cancel_token::as_inner(cancel);
    match block_on_cancellable(
        op.handle(),
        cancel_token,
        op.deref().write_options(path, bytes, opts),
    ) {
        Ok(m) => {
            if !meta.is_null() {
                std::ptr::write(meta, Box::into_raw(Box::new(opendal_metadata::new(m))));
            }
            std::ptr::null_mut()
        }
        Err(e) => opendal_error::from_cancellable(e),
    }
}

/// \brief Blocking read the data from `path`.
///
/// Read the data out from `path` blocking by operator.
///
/// @param op The opendal_operator created previously
/// @param path The path you want to read the data out
/// @param bytes Output pointer receiving the newly allocated `opendal_bytes` on success, or an empty `opendal_bytes` on failure
/// @see opendal_operator
/// @see opendal_bytes
/// @see opendal_error
/// @return NULL if the read succeeds, or an `opendal_error *` on failure.
///
/// \note If the read operation succeeds, the buffer inside `*bytes` is newly allocated on heap.
/// After your usage of that, please call opendal_bytes_free() to free the space. If you reuse
/// the same `opendal_bytes` variable for another read call, free any previously owned buffer first.
///
/// # Example
///
/// Following is an example
/// ```C
/// // ... you have write "Hello, World!" to path "/testpath"
///
/// opendal_bytes bytes;
/// opendal_error *err = opendal_operator_read(op, "testpath", &bytes);
/// assert(err == NULL);
///
/// assert(bytes.len == 13);
/// opendal_bytes_free(&bytes);
/// ```
///
/// # Safety
///
/// It is **safe** under the cases below
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
/// * `bytes` must point to valid, aligned, writable storage.
///
/// # Panic
///
/// * If `path` or `bytes` points to NULL, this function panics, i.e. exits with information
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_read(
    op: &opendal_operator,
    path: *const c_char,
    bytes: *mut opendal_bytes,
) -> *mut opendal_error {
    opendal_operator_read_with_cancel(op, path, std::ptr::null(), std::ptr::null(), bytes)
}

/// \brief Blocking read the data from `path` with options.
///
/// Read the data out from `path` blocking by operator, using the provided
/// `opendal_read_options` to control the behavior, e.g. range, version, or
/// conditional headers.
///
/// @param op The opendal_operator created previously
/// @param path The path you want to read the data out
/// @param opts The options for the read operation; pass NULL to use defaults
/// @param bytes Output pointer receiving the newly allocated `opendal_bytes` on success, or an empty `opendal_bytes` on failure
/// @see opendal_operator
/// @see opendal_read_options
/// @see opendal_error
/// @return NULL if the read succeeds, or an `opendal_error *` on failure.
///
/// \note If the read operation succeeds, the buffer inside `*bytes` is newly allocated on heap.
/// After your usage of that, please call opendal_bytes_free() to free the space. If you reuse
/// the same `opendal_bytes` variable for another read call, free any previously owned buffer first.
///
/// # Safety
///
/// It is **safe** under the cases below
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
/// * `bytes` must point to valid, aligned, writable storage.
///
/// # Panic
///
/// * If `path` or `bytes` points to NULL, this function panics, i.e. exits with information
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_read_with(
    op: &opendal_operator,
    path: *const c_char,
    opts: *const opendal_read_options,
    bytes: *mut opendal_bytes,
) -> *mut opendal_error {
    opendal_operator_read_with_cancel(op, path, opts, std::ptr::null(), bytes)
}

/// \brief Blocking read the data from `path` with options and optional cancellation.
///
/// Pass NULL for `opts` to use defaults, and NULL for `cancel` to run without cancellation.
/// On cancellation or failure, writes an empty `opendal_bytes` to `*bytes` and returns an
/// `opendal_error *` (`OPENDAL_CANCELLED` on cancellation).
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_read_with_cancel(
    op: &opendal_operator,
    path: *const c_char,
    opts: *const opendal_read_options,
    cancel: *const opendal_cancel_token,
    bytes: *mut opendal_bytes,
) -> *mut opendal_error {
    assert!(!path.is_null());
    assert!(!bytes.is_null());
    std::ptr::write(bytes, opendal_bytes::empty());
    let path = std::ffi::CStr::from_ptr(path)
        .to_str()
        .expect("malformed path");
    let opts = if opts.is_null() {
        core::options::ReadOptions::default()
    } else {
        match core::options::ReadOptions::try_from(&*opts) {
            Ok(opts) => opts,
            Err(e) => return opendal_error::new(e),
        }
    };
    let cancel_token = opendal_cancel_token::as_inner(cancel);
    match block_on_cancellable(
        op.handle(),
        cancel_token,
        op.deref().read_options(path, opts),
    ) {
        Ok(b) => {
            std::ptr::write(bytes, opendal_bytes::new(b));
            std::ptr::null_mut()
        }
        Err(e) => opendal_error::from_cancellable(e),
    }
}

/// \brief Blocking create a reader for the specified path.
///
/// @param op The opendal_operator created previously
/// @param path The path you want to read the data out
/// @param reader Output pointer receiving the allocated `opendal_reader *` on success, or NULL on failure
/// @see opendal_operator
/// @see opendal_reader
/// @see opendal_error
/// @return NULL if the operation succeeds, or an `opendal_error *` on failure.
///
/// # Example
///
/// Following is an example
/// ```C
/// // ... you have created an operator named op
///
/// opendal_reader *reader = NULL;
/// opendal_error *err = opendal_operator_reader(op, "/testpath", &reader);
/// assert(err == NULL);
/// ```
///
/// # Safety
///
/// It is **safe** under the cases below
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
/// * `reader` must be a valid, aligned, writable pointer.
///
/// # Panic
///
/// * If `path` or `reader` points to NULL, this function panics, i.e. exits with information
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_reader(
    op: &opendal_operator,
    path: *const c_char,
    reader: *mut *mut opendal_reader,
) -> *mut opendal_error {
    opendal_operator_reader_with_cancel(op, path, std::ptr::null(), std::ptr::null(), reader)
}

/// \brief Blocking create a reader for the specified path with options.
///
/// This function prepares a reader, applying the conditional, version and
/// concurrency options carried by `opts`. A NULL `opts` is treated as the
/// default options, behaving like `opendal_operator_reader`.
///
/// @param op The opendal_operator created previously
/// @param path The designated path where the reader will be used
/// @param opts The reader options, or NULL to use the defaults
/// @param reader Output pointer receiving the allocated `opendal_reader *` on success, or NULL on failure
/// @see opendal_operator
/// @see opendal_reader_options
/// @return NULL if the operation succeeds, or an `opendal_error *` on failure.
///
/// # Safety
///
/// It is **safe** under the cases below
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
/// * `reader` must be a valid, aligned, writable pointer.
///
/// # Panic
///
/// * If `path` or `reader` points to NULL, this function panics, i.e. exits with information
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_reader_with(
    op: &opendal_operator,
    path: *const c_char,
    opts: *const opendal_reader_options,
    reader: *mut *mut opendal_reader,
) -> *mut opendal_error {
    opendal_operator_reader_with_cancel(op, path, opts, std::ptr::null(), reader)
}

/// \brief Blocking create a reader for the specified path with options and optional cancellation.
///
/// Pass NULL for `opts` to use defaults, and NULL for `cancel` to run without cancellation.
/// On cancellation or failure, writes NULL to `*reader` and returns an `opendal_error *`
/// (`OPENDAL_CANCELLED` on cancellation).
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_reader_with_cancel(
    op: &opendal_operator,
    path: *const c_char,
    opts: *const opendal_reader_options,
    cancel: *const opendal_cancel_token,
    reader: *mut *mut opendal_reader,
) -> *mut opendal_error {
    assert!(!path.is_null());
    assert!(!reader.is_null());
    std::ptr::write(reader, std::ptr::null_mut());
    let path = std::ffi::CStr::from_ptr(path)
        .to_str()
        .expect("malformed path");
    let opts = if opts.is_null() {
        core::options::ReaderOptions::default()
    } else {
        match core::options::ReaderOptions::try_from(&*opts) {
            Ok(opts) => opts,
            Err(e) => return opendal_error::new(e),
        }
    };
    let cancel_token = opendal_cancel_token::as_inner(cancel);
    match block_on_cancellable(op.handle(), cancel_token, async {
        let r = op.deref().reader_options(path, opts).await?;
        r.into_futures_async_read(..).await
    }) {
        Ok(r) => {
            std::ptr::write(
                reader,
                Box::into_raw(Box::new(opendal_reader::new(op.handle().clone(), r))),
            );
            std::ptr::null_mut()
        }
        Err(e) => opendal_error::from_cancellable(e),
    }
}

/// \brief Blocking create a writer for the specified path.
///
/// This function prepares a writer that can be used to write data to the specified path
/// using the provided operator.
///
/// @param op The opendal_operator created previously
/// @param path The designated path where the writer will be used
/// @param writer Output pointer receiving the allocated `opendal_writer *` on success, or NULL on failure
/// @see opendal_operator
/// @see opendal_writer
/// @see opendal_error
/// @return NULL if the operation succeeds, or an `opendal_error *` on failure.
///
/// # Example
///
/// Following is an example
/// ```C
/// //...prepare your opendal_operator, named op for example
///
/// opendal_writer *writer = NULL;
/// opendal_error *err = opendal_operator_writer(op, "/testpath", &writer);
/// assert(err == NULL);
/// // Use the writer to write data...
/// ```
///
/// # Safety
///
/// It is **safe** under the cases below
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
/// * `writer` must be a valid, aligned, writable pointer.
///
/// # Panic
///
/// * If `path` or `writer` points to NULL, this function panics, i.e. exits with information
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_writer(
    op: &opendal_operator,
    path: *const c_char,
    writer: *mut *mut opendal_writer,
) -> *mut opendal_error {
    opendal_operator_writer_with_cancel(op, path, std::ptr::null(), std::ptr::null(), writer)
}

/// \brief Blocking create a writer for the specified path with options.
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_writer_with(
    op: &opendal_operator,
    path: *const c_char,
    opts: *const opendal_write_options,
    writer: *mut *mut opendal_writer,
) -> *mut opendal_error {
    opendal_operator_writer_with_cancel(op, path, opts, std::ptr::null(), writer)
}

/// \brief Blocking create a writer for the specified path with options and optional cancellation.
///
/// Pass NULL for `opts` to use defaults, and NULL for `cancel` to run without cancellation.
/// On cancellation or failure, writes NULL to `*writer` and returns an `opendal_error *`
/// (`OPENDAL_CANCELLED` on cancellation).
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_writer_with_cancel(
    op: &opendal_operator,
    path: *const c_char,
    opts: *const opendal_write_options,
    cancel: *const opendal_cancel_token,
    writer: *mut *mut opendal_writer,
) -> *mut opendal_error {
    assert!(!path.is_null());
    assert!(!writer.is_null());
    std::ptr::write(writer, std::ptr::null_mut());
    let path = std::ffi::CStr::from_ptr(path)
        .to_str()
        .expect("malformed path");
    let opts = if opts.is_null() {
        core::options::WriteOptions::default()
    } else {
        (&*opts).into()
    };
    let cancel_token = opendal_cancel_token::as_inner(cancel);
    match block_on_cancellable(
        op.handle(),
        cancel_token,
        op.deref().writer_options(path, opts),
    ) {
        Ok(w) => {
            std::ptr::write(
                writer,
                Box::into_raw(Box::new(opendal_writer::new(op.handle().clone(), w))),
            );
            std::ptr::null_mut()
        }
        Err(e) => opendal_error::from_cancellable(e),
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
    opendal_operator_delete_with_cancel(op, path, std::ptr::null(), std::ptr::null())
}

/// \brief Blocking delete the object in `path` with options.
///
/// Delete the object in `path` blocking by `op`, using the provided `opendal_delete_options`.
/// This is similar to `opendal_operator_delete` but allows specifying a version or
/// requesting a recursive delete.
///
/// @param op The opendal_operator created previously
/// @param path The designated path you want to delete
/// @param opts The options for the delete operation; pass NULL to use defaults
/// @see opendal_delete_options
/// @return NULL if succeeds, otherwise it contains the error code and error message.
///
/// # Safety
///
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
///
/// # Panic
///
/// * If the `path` points to NULL, this function panics, i.e. exits with information
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_delete_with(
    op: &opendal_operator,
    path: *const c_char,
    opts: *const opendal_delete_options,
) -> *mut opendal_error {
    opendal_operator_delete_with_cancel(op, path, opts, std::ptr::null())
}

/// \brief Blocking delete the object in `path` with options and optional cancellation.
///
/// Pass NULL for `opts` to use defaults, and NULL for `cancel` to run without cancellation.
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_delete_with_cancel(
    op: &opendal_operator,
    path: *const c_char,
    opts: *const opendal_delete_options,
    cancel: *const opendal_cancel_token,
) -> *mut opendal_error {
    assert!(!path.is_null());
    let path = std::ffi::CStr::from_ptr(path)
        .to_str()
        .expect("malformed path");
    let delete_opts = if opts.is_null() {
        core::options::DeleteOptions::default()
    } else {
        (&*opts).into()
    };
    let cancel_token = opendal_cancel_token::as_inner(cancel);
    match block_on_cancellable(
        op.handle(),
        cancel_token,
        op.deref().delete_options(path, delete_opts),
    ) {
        Ok(_) => std::ptr::null_mut(),
        Err(e) => opendal_error::from_cancellable(e),
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
/// @param is_exist Output pointer receiving whether the path exists, or false on failure
/// @see opendal_operator
/// @see opendal_error
/// @return NULL if the operation succeeds, or an `opendal_error *` on failure.
///
/// # Example
///
/// ```C
/// // .. you previously wrote some data to path "/mytest/obj"
/// bool exists = false;
/// opendal_error *err = opendal_operator_is_exist(op, "/mytest/obj", &exists);
/// assert(err == NULL);
/// assert(exists);
///
/// // but you previously did **not** write any data to path "/yourtest/obj"
/// err = opendal_operator_is_exist(op, "/yourtest/obj", &exists);
/// assert(err == NULL);
/// assert(!exists);
/// ```
///
/// # Safety
///
/// It is **safe** under the cases below
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
/// * `is_exist` must be a valid, aligned, writable pointer.
///
/// # Panic
///
/// * If `path` or `is_exist` points to NULL, this function panics, i.e. exits with information
#[no_mangle]
#[cfg_attr(cbindgen, cbindgen::ignore)]
pub unsafe extern "C" fn opendal_operator_is_exist(
    op: &opendal_operator,
    path: *const c_char,
    is_exist: *mut bool,
) -> *mut opendal_error {
    opendal_operator_exists_with_cancel(op, path, std::ptr::null(), is_exist)
}

/// \brief Check whether the path exists.
///
/// If the operation succeeds, no matter the path exists or not,
/// the error should be a nullptr. Otherwise, the field `exists`
/// is filled with false, and the error is set
///
/// @param op The opendal_operator created previously
/// @param path The path you want to check existence
/// @param exists Output pointer receiving whether the path exists, or false on failure
/// @see opendal_operator
/// @see opendal_error
/// @return NULL if the operation succeeds, or an `opendal_error *` on failure.
///
/// # Example
///
/// ```C
/// // .. you previously wrote some data to path "/mytest/obj"
/// bool exists = false;
/// opendal_error *err = opendal_operator_exists(op, "/mytest/obj", &exists);
/// assert(err == NULL);
/// assert(exists);
///
/// // but you previously did **not** write any data to path "/yourtest/obj"
/// err = opendal_operator_exists(op, "/yourtest/obj", &exists);
/// assert(err == NULL);
/// assert(!exists);
/// ```
///
/// # Safety
///
/// It is **safe** under the cases below
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
/// * `exists` must be a valid, aligned, writable pointer.
///
/// # Panic
///
/// * If `path` or `exists` points to NULL, this function panics, i.e. exits with information
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_exists(
    op: &opendal_operator,
    path: *const c_char,
    exists: *mut bool,
) -> *mut opendal_error {
    opendal_operator_exists_with_cancel(op, path, std::ptr::null(), exists)
}

/// \brief Check whether the path exists, supporting optional cancellation via `cancel`.
///
/// Pass NULL for `cancel` to run without cancellation. On cancellation or failure,
/// writes `false` to `*exists` and returns an `opendal_error *` (`OPENDAL_CANCELLED`
/// on cancellation).
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_exists_with_cancel(
    op: &opendal_operator,
    path: *const c_char,
    cancel: *const opendal_cancel_token,
    exists: *mut bool,
) -> *mut opendal_error {
    assert!(!path.is_null());
    assert!(!exists.is_null());
    std::ptr::write(exists, false);
    let path = std::ffi::CStr::from_ptr(path)
        .to_str()
        .expect("malformed path");
    let cancel_token = opendal_cancel_token::as_inner(cancel);
    match block_on_cancellable(op.handle(), cancel_token, op.deref().exists(path)) {
        Ok(e) => {
            std::ptr::write(exists, e);
            std::ptr::null_mut()
        }
        Err(e) => opendal_error::from_cancellable(e),
    }
}

/// \brief Stat the path, return its metadata.
///
/// Error is NULL if successful, otherwise it contains the error code and error message.
///
/// @param op The opendal_operator created previously
/// @param path The path you want to stat
/// @param meta Output pointer receiving the allocated `opendal_metadata *` on success, or NULL on failure
/// @see opendal_operator
/// @see opendal_metadata
/// @return NULL if the operation succeeds, or an `opendal_error *` on failure.
///
/// # Example
///
/// ```C
/// // ... previously you wrote "Hello, World!" to path "/testpath"
/// opendal_metadata *meta = NULL;
/// opendal_error *err = opendal_operator_stat(op, "/testpath", &meta);
/// assert(err == NULL);
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
/// * `meta` must be a valid, aligned, writable pointer.
///
/// # Panic
///
/// * If `path` or `meta` points to NULL, this function panics, i.e. exits with information
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_stat(
    op: &opendal_operator,
    path: *const c_char,
    meta: *mut *mut opendal_metadata,
) -> *mut opendal_error {
    opendal_operator_stat_with_cancel(op, path, std::ptr::null(), std::ptr::null(), meta)
}

/// \brief Blocking stat the object in `path` with options.
///
/// Stat the object in `path` with the provided `opendal_stat_options`. This is
/// similar to `opendal_operator_stat` but allows passing options such as
/// `version`, `if_match`, `if_none_match`, or response header overrides.
///
/// @param op The opendal_operator created previously
/// @param path The path you want to stat
/// @param opts The options for the stat operation; pass NULL to use defaults
/// @param meta Output pointer receiving the allocated `opendal_metadata *` on success, or NULL on failure
/// @see opendal_operator
/// @see opendal_stat_options
/// @return NULL if the operation succeeds, or an `opendal_error *` on failure.
///
/// # Safety
///
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
/// * `meta` must be a valid, aligned, writable pointer.
///
/// # Panic
///
/// * If `path` or `meta` points to NULL, this function panics, i.e. exits with information
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_stat_with(
    op: &opendal_operator,
    path: *const c_char,
    opts: *const opendal_stat_options,
    meta: *mut *mut opendal_metadata,
) -> *mut opendal_error {
    opendal_operator_stat_with_cancel(op, path, opts, std::ptr::null(), meta)
}

/// \brief Blocking stat the object in `path` with options and optional cancellation.
///
/// Pass NULL for `opts` to use defaults, and NULL for `cancel` to run without cancellation.
/// On cancellation or failure, writes NULL to `*meta` and returns an `opendal_error *`
/// (`OPENDAL_CANCELLED` on cancellation).
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_stat_with_cancel(
    op: &opendal_operator,
    path: *const c_char,
    opts: *const opendal_stat_options,
    cancel: *const opendal_cancel_token,
    meta: *mut *mut opendal_metadata,
) -> *mut opendal_error {
    assert!(!path.is_null());
    assert!(!meta.is_null());
    std::ptr::write(meta, std::ptr::null_mut());
    let path = std::ffi::CStr::from_ptr(path)
        .to_str()
        .expect("malformed path");
    let opts = if opts.is_null() {
        core::options::StatOptions::default()
    } else {
        match core::options::StatOptions::try_from(&*opts) {
            Ok(opts) => opts,
            Err(e) => return opendal_error::new(e),
        }
    };
    let cancel_token = opendal_cancel_token::as_inner(cancel);
    match block_on_cancellable(
        op.handle(),
        cancel_token,
        op.deref().stat_options(path, opts),
    ) {
        Ok(m) => {
            std::ptr::write(meta, Box::into_raw(Box::new(opendal_metadata::new(m))));
            std::ptr::null_mut()
        }
        Err(e) => opendal_error::from_cancellable(e),
    }
}

/// \brief Blocking list the objects in `path`.
///
/// List the object in `path` blocking by `op_ptr`, writing the constructed
/// `opendal_lister *` to `*lister`. Users should call opendal_lister_next() on the
/// lister.
///
/// @param op The opendal_operator created previously
/// @param path The designated path you want to list
/// @param lister Output pointer receiving the allocated `opendal_lister *` on success, or NULL on failure
/// @see opendal_lister
/// @return NULL if the operation succeeds, or an `opendal_error *` on failure.
///
/// # Example
///
/// Following is an example
/// ```C
/// // You have written some data into some files path "root/dir1"
/// // Your opendal_operator was called op
/// opendal_lister *lister = NULL;
/// opendal_error *err = opendal_operator_list(op, "root/dir1", &lister);
/// assert(err == NULL);
///
/// opendal_entry *entry = NULL;
/// while ((err = opendal_lister_next(lister, &entry)) == NULL && entry != NULL) {
///     char* de_path = opendal_entry_path(entry);
///     char* de_name = opendal_entry_name(entry);
///     // ...... your operations
///     opendal_string_free(de_path);
///     opendal_string_free(de_name);
///
///     // remember to free the entry after you are done using it
///     opendal_entry_free(entry);
/// }
/// assert(err == NULL);
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
/// * `lister` must be a valid, aligned, writable pointer.
///
/// # Panic
///
/// * If `path` or `lister` points to NULL, this function panics, i.e. exits with information
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_list(
    op: &opendal_operator,
    path: *const c_char,
    lister: *mut *mut opendal_lister,
) -> *mut opendal_error {
    opendal_operator_list_with_cancel(op, path, std::ptr::null(), std::ptr::null(), lister)
}

/// \brief Blocking list the objects in `path` with options.
///
/// List the objects in `path` with the provided `opendal_list_options`. This is
/// similar to `opendal_operator_list` but allows passing options such as
/// `recursive` to control the listing behavior.
///
/// @param op The opendal_operator created previously
/// @param path The designated path you want to list
/// @param opts The options for the list operation; pass NULL to use defaults
/// @param lister Output pointer receiving the allocated `opendal_lister *` on success, or NULL on failure
/// @see opendal_lister
/// @see opendal_list_options
/// @return NULL if the operation succeeds, or an `opendal_error *` on failure.
///
/// # Safety
///
/// * The memory pointed to by `path` must contain a valid null terminator at the end of
///   the string.
/// * `lister` must be a valid, aligned, writable pointer.
///
/// # Panic
///
/// * If `path` or `lister` points to NULL, this function panics, i.e. exits with information
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_list_with(
    op: &opendal_operator,
    path: *const c_char,
    opts: *const opendal_list_options,
    lister: *mut *mut opendal_lister,
) -> *mut opendal_error {
    opendal_operator_list_with_cancel(op, path, opts, std::ptr::null(), lister)
}

/// \brief Blocking list the objects in `path` with options and optional cancellation.
///
/// Pass NULL for `opts` to use defaults, and NULL for `cancel` to run without cancellation.
/// On cancellation or failure, writes NULL to `*lister` and returns an `opendal_error *`
/// (`OPENDAL_CANCELLED` on cancellation).
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_list_with_cancel(
    op: &opendal_operator,
    path: *const c_char,
    opts: *const opendal_list_options,
    cancel: *const opendal_cancel_token,
    lister: *mut *mut opendal_lister,
) -> *mut opendal_error {
    assert!(!path.is_null());
    assert!(!lister.is_null());
    std::ptr::write(lister, std::ptr::null_mut());
    let path = std::ffi::CStr::from_ptr(path)
        .to_str()
        .expect("malformed path");
    let list_opts = if opts.is_null() {
        core::options::ListOptions::default()
    } else {
        (&*opts).into()
    };
    let cancel_token = opendal_cancel_token::as_inner(cancel);
    match block_on_cancellable(
        op.handle(),
        cancel_token,
        op.deref().lister_options(path, list_opts),
    ) {
        Ok(l) => {
            std::ptr::write(
                lister,
                Box::into_raw(Box::new(opendal_lister::new(op.handle().clone(), l))),
            );
            std::ptr::null_mut()
        }
        Err(e) => opendal_error::from_cancellable(e),
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
    opendal_operator_create_dir_with_cancel(op, path, std::ptr::null())
}

/// \brief Blocking create the directory in `path` with optional cancellation.
///
/// Pass NULL for `cancel` to run without cancellation.
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_create_dir_with_cancel(
    op: &opendal_operator,
    path: *const c_char,
    cancel: *const opendal_cancel_token,
) -> *mut opendal_error {
    assert!(!path.is_null());
    let path = std::ffi::CStr::from_ptr(path)
        .to_str()
        .expect("malformed path");
    let cancel_token = opendal_cancel_token::as_inner(cancel);
    match block_on_cancellable(op.handle(), cancel_token, op.deref().create_dir(path)) {
        Ok(()) => std::ptr::null_mut(),
        Err(e) => opendal_error::from_cancellable(e),
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
    opendal_operator_rename_with_cancel(op, src, dest, std::ptr::null())
}

/// \brief Blocking rename the object from `src` to `dest` with optional cancellation.
///
/// Pass NULL for `cancel` to run without cancellation.
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_rename_with_cancel(
    op: &opendal_operator,
    src: *const c_char,
    dest: *const c_char,
    cancel: *const opendal_cancel_token,
) -> *mut opendal_error {
    assert!(!src.is_null());
    assert!(!dest.is_null());
    let src = std::ffi::CStr::from_ptr(src)
        .to_str()
        .expect("malformed src");
    let dest = std::ffi::CStr::from_ptr(dest)
        .to_str()
        .expect("malformed dest");
    let cancel_token = opendal_cancel_token::as_inner(cancel);
    match block_on_cancellable(op.handle(), cancel_token, op.deref().rename(src, dest)) {
        Ok(()) => std::ptr::null_mut(),
        Err(e) => opendal_error::from_cancellable(e),
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
    opendal_operator_copy_with_cancel(op, src, dest, std::ptr::null(), std::ptr::null())
}

/// \brief Blocking copy the object in `path` with options.
///
/// Copy the object from `src` to `dest` blocking by `op`, using the provided
/// `opendal_copy_options` to control the behavior, e.g. `if_not_exists` or
/// `if_match` conditions.
///
/// @param op The opendal_operator created previously
/// @param src The designated source path you want to copy
/// @param dest The designated destination path you want to copy
/// @param opts The options for the copy operation; pass NULL to use defaults
/// @see opendal_operator
/// @see opendal_copy_options
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
/// // prepare options
/// opendal_copy_options *opts = opendal_copy_options_new();
/// opendal_copy_options_set_if_not_exists(opts, true);
///
/// // now you can copy with options!
/// opendal_error *error = opendal_operator_copy_with(op, "/testpath", "/testpath2", opts);
///
/// // Assert that this succeeds
/// assert(error == NULL);
///
/// // remember to free the options
/// opendal_copy_options_free(opts);
/// ```
///
/// # Safety
///
/// It is **safe** under the cases below
/// * The memory pointed to by `src` and `dest` must contain a valid nul terminator at the end of
///   the string.
///
/// # Panic
///
/// * If the `src` or `dest` points to NULL, this function panics, i.e. exits with information
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_copy_with(
    op: &opendal_operator,
    src: *const c_char,
    dest: *const c_char,
    opts: *const opendal_copy_options,
) -> *mut opendal_error {
    opendal_operator_copy_with_cancel(op, src, dest, opts, std::ptr::null())
}

/// \brief Blocking copy the object from `src` to `dest` with options and optional cancellation.
///
/// Pass NULL for `opts` to use defaults, and NULL for `cancel` to run without cancellation.
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_copy_with_cancel(
    op: &opendal_operator,
    src: *const c_char,
    dest: *const c_char,
    opts: *const opendal_copy_options,
    cancel: *const opendal_cancel_token,
) -> *mut opendal_error {
    assert!(!src.is_null());
    assert!(!dest.is_null());
    let src = std::ffi::CStr::from_ptr(src)
        .to_str()
        .expect("malformed src");
    let dest = std::ffi::CStr::from_ptr(dest)
        .to_str()
        .expect("malformed dest");
    let copy_opts = if opts.is_null() {
        core::options::CopyOptions::default()
    } else {
        core::options::CopyOptions::from(&*opts)
    };
    let cancel_token = opendal_cancel_token::as_inner(cancel);
    match block_on_cancellable(
        op.handle(),
        cancel_token,
        op.deref().copy_options(src, dest, copy_opts),
    ) {
        Ok(_) => std::ptr::null_mut(),
        Err(e) => opendal_error::from_cancellable(e),
    }
}

/// \brief Check whether the operator works properly.
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_check(op: &opendal_operator) -> *mut opendal_error {
    opendal_operator_check_with_cancel(op, std::ptr::null())
}

/// \brief Check whether the operator works properly, supporting optional cancellation.
///
/// Pass NULL for `cancel` to run without cancellation.
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_check_with_cancel(
    op: &opendal_operator,
    cancel: *const opendal_cancel_token,
) -> *mut opendal_error {
    let cancel_token = opendal_cancel_token::as_inner(cancel);
    match block_on_cancellable(op.handle(), cancel_token, op.deref().check()) {
        Ok(()) => std::ptr::null_mut(),
        Err(e) => opendal_error::from_cancellable(e),
    }
}

/// \brief Blocking create a copier to copy a file from `src` to `dest`.
///
/// The returned copier is used to complete a long-running copy operation. Call
/// `opendal_copier_next` repeatedly to make progress, and `opendal_copier_free`
/// to release it once finished.
///
/// @param op The opendal_operator created previously
/// @param src The designated source path you want to copy
/// @param dest The designated destination path you want to copy
/// @param copier Output pointer receiving the allocated `opendal_copier *` on success, or NULL on failure
/// @see opendal_operator
/// @see opendal_copier
/// @return NULL if the operation succeeds, or an `opendal_error *` on failure.
///
/// # Safety
///
/// * The memory pointed to by `src` and `dest` must contain a valid nul terminator at the
///   end of the string.
/// * `copier` must be a valid, aligned, writable pointer.
///
/// # Panic
///
/// * If `src`, `dest`, or `copier` points to NULL, this function panics
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_copier(
    op: &opendal_operator,
    src: *const c_char,
    dest: *const c_char,
    copier: *mut *mut opendal_copier,
) -> *mut opendal_error {
    opendal_operator_copier_with_cancel(op, src, dest, std::ptr::null(), std::ptr::null(), copier)
}

/// \brief Blocking create a copier to copy a file from `src` to `dest` with options.
///
/// This is the same as `opendal_operator_copier` but accepts an `opendal_copy_options`
/// to control the behavior, e.g. `concurrent` or `chunk`. Pass NULL to use defaults.
///
/// @param op The opendal_operator created previously
/// @param src The designated source path you want to copy
/// @param dest The designated destination path you want to copy
/// @param opts The options for the copy operation; pass NULL to use defaults
/// @param copier Output pointer receiving the allocated `opendal_copier *` on success, or NULL on failure
/// @see opendal_operator_copier
/// @see opendal_copy_options
/// @return NULL if the operation succeeds, or an `opendal_error *` on failure.
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_copier_with(
    op: &opendal_operator,
    src: *const c_char,
    dest: *const c_char,
    opts: *const opendal_copy_options,
    copier: *mut *mut opendal_copier,
) -> *mut opendal_error {
    opendal_operator_copier_with_cancel(op, src, dest, opts, std::ptr::null(), copier)
}

/// \brief Blocking create a copier to copy a file from `src` to `dest` with options and optional cancellation.
///
/// Pass NULL for `opts` to use defaults, and NULL for `cancel` to run without cancellation.
/// On cancellation or failure, writes NULL to `*copier` and returns an `opendal_error *`
/// (`OPENDAL_CANCELLED` on cancellation).
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_copier_with_cancel(
    op: &opendal_operator,
    src: *const c_char,
    dest: *const c_char,
    opts: *const opendal_copy_options,
    cancel: *const opendal_cancel_token,
    copier: *mut *mut opendal_copier,
) -> *mut opendal_error {
    assert!(!src.is_null());
    assert!(!dest.is_null());
    assert!(!copier.is_null());
    std::ptr::write(copier, std::ptr::null_mut());
    let src = std::ffi::CStr::from_ptr(src)
        .to_str()
        .expect("malformed src");
    let dest = std::ffi::CStr::from_ptr(dest)
        .to_str()
        .expect("malformed dest");
    let copy_opts = if opts.is_null() {
        core::options::CopyOptions::default()
    } else {
        core::options::CopyOptions::from(&*opts)
    };
    let cancel_token = opendal_cancel_token::as_inner(cancel);
    match block_on_cancellable(
        op.handle(),
        cancel_token,
        op.deref().copier_options(src, dest, copy_opts),
    ) {
        Ok(c) => {
            std::ptr::write(
                copier,
                Box::into_raw(Box::new(opendal_copier::new(op.handle().clone(), c))),
            );
            std::ptr::null_mut()
        }
        Err(e) => opendal_error::from_cancellable(e),
    }
}

/// \brief Create a blocking deleter to remove multiple paths.
///
/// Submit paths with `opendal_deleter_delete` or `opendal_deleter_delete_many`.
/// Call `opendal_deleter_flush` to complete pending deletions and
/// `opendal_deleter_free` to release the handle.
///
/// @param op The opendal_operator created previously
/// @param deleter_out Output pointer receiving the allocated deleter on success, or NULL on failure
/// @return NULL if the operation succeeds, or an `opendal_error *` on failure.
///
/// # Safety
///
/// * `deleter_out` must point to writable storage for an `opendal_deleter *`.
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_deleter(
    op: &opendal_operator,
    deleter_out: *mut *mut opendal_deleter,
) -> *mut opendal_error {
    opendal_operator_deleter_with_cancel(op, std::ptr::null(), deleter_out)
}

/// \brief Create a blocking deleter to remove multiple paths, supporting optional cancellation.
///
/// Pass NULL for `cancel` to run without cancellation. On cancellation or failure,
/// writes NULL to `*deleter_out` and returns an `opendal_error *` (`OPENDAL_CANCELLED`
/// on cancellation).
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_deleter_with_cancel(
    op: &opendal_operator,
    cancel: *const opendal_cancel_token,
    deleter_out: *mut *mut opendal_deleter,
) -> *mut opendal_error {
    assert!(!deleter_out.is_null());
    std::ptr::write(deleter_out, std::ptr::null_mut());
    let cancel_token = opendal_cancel_token::as_inner(cancel);
    match block_on_cancellable(op.handle(), cancel_token, op.deref().deleter()) {
        Ok(deleter) => {
            std::ptr::write(
                deleter_out,
                Box::into_raw(Box::new(opendal_deleter::new(op.handle().clone(), deleter))),
            );
            std::ptr::null_mut()
        }
        Err(e) => opendal_error::from_cancellable(e),
    }
}
