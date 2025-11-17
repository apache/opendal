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
use std::os::raw::c_char;
use std::str::FromStr;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::task;

use ::opendal as core;

use super::*;
use crate::error::opendal_error;
use crate::metadata::opendal_metadata; // Keep this
use crate::result::opendal_result_read;
use crate::result::opendal_result_stat; // Keep this
use crate::types::opendal_bytes;
use crate::types::opendal_operator_options; // Keep this

/// Status returned by non-blocking future polling.
#[repr(C)]
pub enum opendal_future_status {
    /// Future is still pending.
    OPENDAL_FUTURE_PENDING = 0,
    /// Future is ready and output has been written to the provided out param.
    OPENDAL_FUTURE_READY = 1,
    /// Future completed with an error state (e.g., channel closed).
    OPENDAL_FUTURE_ERROR = 2,
    /// Future was cancelled.
    OPENDAL_FUTURE_CANCELED = 3,
}

#[no_mangle]
pub static OPENDAL_FUTURE_PENDING: opendal_future_status = opendal_future_status::OPENDAL_FUTURE_PENDING;
#[no_mangle]
pub static OPENDAL_FUTURE_READY: opendal_future_status = opendal_future_status::OPENDAL_FUTURE_READY;
#[no_mangle]
pub static OPENDAL_FUTURE_ERROR: opendal_future_status = opendal_future_status::OPENDAL_FUTURE_ERROR;
#[no_mangle]
pub static OPENDAL_FUTURE_CANCELED: opendal_future_status = opendal_future_status::OPENDAL_FUTURE_CANCELED;

macro_rules! impl_poll_result {
    ($fn_name:ident, $future_ty:ty, $out_ty:ty, $ok_ctor:expr, $err_ctor:expr) => {
        #[no_mangle]
        pub unsafe extern "C" fn $fn_name(
            future: *mut $future_ty,
            out: *mut $out_ty,
        ) -> opendal_future_status {
            if future.is_null() || out.is_null() {
                return opendal_future_status::OPENDAL_FUTURE_ERROR;
            }

            let future = &mut *future;
            if future.rx.is_null() {
                return opendal_future_status::OPENDAL_FUTURE_READY;
            }

            let rx_opt = unsafe { &mut *future.rx };
            let Some(rx) = rx_opt.as_mut() else {
                return opendal_future_status::OPENDAL_FUTURE_READY;
            };

            match rx.try_recv() {
                Ok(Ok(value)) => {
                    rx_opt.take();
                    if !future.handle.is_null() {
                        let mut handle_box = Box::from_raw(future.handle);
                        future.handle = std::ptr::null_mut();
                        if let Some(handle) = (*handle_box).take() {
                            let _ = handle;
                        }
                    }

                    let out_ref = &mut *out;
                    *out_ref = $ok_ctor(value);
                    opendal_future_status::OPENDAL_FUTURE_READY
                }
                Ok(Err(err)) => {
                    rx_opt.take();
                    if !future.handle.is_null() {
                        let mut handle_box = Box::from_raw(future.handle);
                        future.handle = std::ptr::null_mut();
                        if let Some(handle) = (*handle_box).take() {
                            let _ = handle;
                        }
                    }
                    let out_ref = &mut *out;
                    *out_ref = $err_ctor(err);
                    opendal_future_status::OPENDAL_FUTURE_READY
                }
                Err(TryRecvError::Empty) => opendal_future_status::OPENDAL_FUTURE_PENDING,
                Err(TryRecvError::Closed) => {
                    rx_opt.take();
                    opendal_future_status::OPENDAL_FUTURE_CANCELED
                }
            }
        }
    };
}

macro_rules! impl_poll_error_only {
    ($fn_name:ident, $future_ty:ty) => {
        #[no_mangle]
        pub unsafe extern "C" fn $fn_name(
            future: *mut $future_ty,
            error_out: *mut *mut opendal_error,
        ) -> opendal_future_status {
            if future.is_null() || error_out.is_null() {
                return opendal_future_status::OPENDAL_FUTURE_ERROR;
            }

            let future = &mut *future;
            if future.rx.is_null() {
                return opendal_future_status::OPENDAL_FUTURE_READY;
            }

            let rx_opt = unsafe { &mut *future.rx };
            let Some(rx) = rx_opt.as_mut() else {
                return opendal_future_status::OPENDAL_FUTURE_READY;
            };

            match rx.try_recv() {
                Ok(Ok(_)) => {
                    rx_opt.take();
                    if !future.handle.is_null() {
                        let mut handle_box = Box::from_raw(future.handle);
                        future.handle = std::ptr::null_mut();
                        if let Some(handle) = (*handle_box).take() {
                            let _ = handle;
                        }
                    }
                    *error_out = std::ptr::null_mut();
                    opendal_future_status::OPENDAL_FUTURE_READY
                }
                Ok(Err(err)) => {
                    rx_opt.take();
                    if !future.handle.is_null() {
                        let mut handle_box = Box::from_raw(future.handle);
                        future.handle = std::ptr::null_mut();
                        if let Some(handle) = (*handle_box).take() {
                            let _ = handle;
                        }
                    }
                    *error_out = opendal_error::new(err);
                    opendal_future_status::OPENDAL_FUTURE_READY
                }
                Err(TryRecvError::Empty) => opendal_future_status::OPENDAL_FUTURE_PENDING,
                Err(TryRecvError::Closed) => {
                    rx_opt.take();
                    opendal_future_status::OPENDAL_FUTURE_CANCELED
                }
            }
        }
    };
}

macro_rules! impl_is_ready_fn {
    ($fn_name:ident, $future_ty:ty) => {
        #[no_mangle]
        pub unsafe extern "C" fn $fn_name(future: *const $future_ty) -> bool {
            if future.is_null() {
                return false;
            }

            let future = &*future;
            if future.handle.is_null() {
                return false;
            }

            let handle_slot = &*future.handle;
            match handle_slot.as_ref() {
                Some(handle) => handle.is_finished(),
                None => true,
            }
        }
    };
}

macro_rules! impl_await_result {
    ($fn_name:ident, $future_ty:ty, $out_ty:ty, $ok_ctor:expr, $err_ctor:expr) => {
        #[no_mangle]
        pub unsafe extern "C" fn $fn_name(future: *mut $future_ty) -> $out_ty {
            if future.is_null() {
                return $err_ctor(core::Error::new(
                    core::ErrorKind::Unexpected,
                    stringify!($fn_name).to_string() + " future is null",
                ));
            }

            let mut future = Box::from_raw(future);
            if future.rx.is_null() {
                return $err_ctor(core::Error::new(
                    core::ErrorKind::Unexpected,
                    stringify!($fn_name).to_string() + " receiver is null",
                ));
            }

            let mut rx_box = Box::from_raw(future.rx);
            future.rx = std::ptr::null_mut();
            let rx = match rx_box.take() {
                Some(rx) => rx,
                None => {
                    return $err_ctor(core::Error::new(
                        core::ErrorKind::Unexpected,
                        stringify!($fn_name).to_string() + " already awaited",
                    ));
                }
            };

            let recv_result = crate::operator::RUNTIME.block_on(async { rx.await });

            if !future.handle.is_null() {
                let mut handle_box = Box::from_raw(future.handle);
                future.handle = std::ptr::null_mut();
                if let Some(handle) = (*handle_box).take() {
                    let _ = handle;
                }
            }

            match recv_result {
                Ok(Ok(v)) => $ok_ctor(v),
                Ok(Err(e)) => $err_ctor(e),
                Err(recv_err) => $err_ctor(core::Error::new(
                    core::ErrorKind::Unexpected,
                    format!("join error: {}", recv_err),
                )),
            }
        }
    };
}

macro_rules! impl_await_error_only {
    ($fn_name:ident, $future_ty:ty) => {
        #[no_mangle]
        pub unsafe extern "C" fn $fn_name(future: *mut $future_ty) -> *mut opendal_error {
            if future.is_null() {
                return opendal_error::new(core::Error::new(
                    core::ErrorKind::Unexpected,
                    stringify!($fn_name).to_string() + " future is null",
                ));
            }

            let mut future = Box::from_raw(future);
            if future.rx.is_null() {
                return opendal_error::new(core::Error::new(
                    core::ErrorKind::Unexpected,
                    stringify!($fn_name).to_string() + " receiver is null",
                ));
            }

            let mut rx_box = Box::from_raw(future.rx);
            future.rx = std::ptr::null_mut();
            let rx = match rx_box.take() {
                Some(rx) => rx,
                None => {
                    return opendal_error::new(core::Error::new(
                        core::ErrorKind::Unexpected,
                        stringify!($fn_name).to_string() + " already awaited",
                    ));
                }
            };

            let recv_result = crate::operator::RUNTIME.block_on(async { rx.await });

            if !future.handle.is_null() {
                let mut handle_box = Box::from_raw(future.handle);
                future.handle = std::ptr::null_mut();
                if let Some(handle) = (*handle_box).take() {
                    let _ = handle;
                }
            }

            match recv_result {
                Ok(Ok(_)) => std::ptr::null_mut(),
                Ok(Err(e)) => opendal_error::new(e),
                Err(recv_err) => opendal_error::new(core::Error::new(
                    core::ErrorKind::Unexpected,
                    format!("join error: {}", recv_err),
                )),
            }
        }
    };
}

/// Future handle for asynchronous stat operations.
#[repr(C)]
pub struct opendal_future_stat {
    /// Pointer to an owned JoinHandle wrapped in Option for safe extraction.
    handle: *mut Option<task::JoinHandle<()>>,
    /// Receiver for the stat result.
    rx: *mut Option<oneshot::Receiver<core::Result<core::Metadata>>>,
}

unsafe impl Send for opendal_future_stat {}

/// Result type for creating an asynchronous stat future.
#[repr(C)]
pub struct opendal_result_future_stat {
    /// The future handle. Null when creation fails.
    pub future: *mut opendal_future_stat,
    /// The error information. Null on success.
    pub error: *mut opendal_error,
}

unsafe impl Send for opendal_result_future_stat {}

/// Future handle for asynchronous read operations.
#[repr(C)]
pub struct opendal_future_read {
    handle: *mut Option<task::JoinHandle<()>>,
    rx: *mut Option<oneshot::Receiver<core::Result<core::Buffer>>>,
}

unsafe impl Send for opendal_future_read {}

/// Result type for creating an asynchronous read future.
#[repr(C)]
pub struct opendal_result_future_read {
    /// The future handle. Null when creation fails.
    pub future: *mut opendal_future_read,
    /// The error information. Null on success.
    pub error: *mut opendal_error,
}

unsafe impl Send for opendal_result_future_read {}

/// Future handle for asynchronous write operations.
#[repr(C)]
pub struct opendal_future_write {
    handle: *mut Option<task::JoinHandle<()>>,
    rx: *mut Option<oneshot::Receiver<core::Result<core::Metadata>>>,
}

unsafe impl Send for opendal_future_write {}

/// Result type for creating an asynchronous write future.
#[repr(C)]
pub struct opendal_result_future_write {
    /// The future handle. Null when creation fails.
    pub future: *mut opendal_future_write,
    /// The error information. Null on success.
    pub error: *mut opendal_error,
}

unsafe impl Send for opendal_result_future_write {}

/// Future handle for asynchronous delete operations.
#[repr(C)]
pub struct opendal_future_delete {
    handle: *mut Option<task::JoinHandle<()>>,
    rx: *mut Option<oneshot::Receiver<core::Result<()>>>,
}

unsafe impl Send for opendal_future_delete {}

/// Result type for creating an asynchronous delete future.
#[repr(C)]
pub struct opendal_result_future_delete {
    /// The future handle. Null when creation fails.
    pub future: *mut opendal_future_delete,
    /// The error information. Null on success.
    pub error: *mut opendal_error,
}

unsafe impl Send for opendal_result_future_delete {}

/// \brief Represents an asynchronous OpenDAL Operator.
///
/// This operator interacts with storage services using non-blocking APIs.
/// Use `opendal_async_operator_new` to construct and `opendal_async_operator_free` to release.
#[repr(C)]
pub struct opendal_async_operator {
    /// Internal pointer to the Rust async Operator.
    inner: *mut core::Operator,
}

impl opendal_async_operator {
    /// Returns a reference to the inner asynchronous `core::Operator`.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the `opendal_async_operator` pointer is valid
    /// and that the lifetime of the returned reference does not exceed the lifetime
    /// of the `opendal_async_operator`.
    pub(crate) unsafe fn as_ref(&self) -> &core::Operator {
        &*self.inner
    }
}

/// \brief Constructs a new asynchronous OpenDAL Operator.
///
/// @param scheme The storage service scheme (e.g., "s3", "fs").
/// @param options Configuration options for the service. Can be NULL.
/// @return Result containing the new operator or an error.
///
/// \see opendal_operator_options
/// \see opendal_result_operator_new (reused for simplicity, but contains async op)
///
/// # Safety
///
/// `scheme` must be a valid, null-terminated C string.
/// `options` must be a valid pointer or NULL.
#[no_mangle]
pub unsafe extern "C" fn opendal_async_operator_new(
    scheme: *const c_char,
    options: *const opendal_operator_options,
) -> opendal_result_operator_new {
    assert!(!scheme.is_null());
    let scheme_str = match std::ffi::CStr::from_ptr(scheme).to_str() {
        Ok(s) => s,
        Err(e) => {
            let err = core::Error::new(core::ErrorKind::Unexpected, "invalid scheme string")
                .set_source(e);
            return opendal_result_operator_new {
                op: std::ptr::null_mut(), // Represents async operator here
                error: opendal_error::new(err),
            };
        }
    };

    let scheme = match core::Scheme::from_str(scheme_str) {
        Ok(s) => s,
        Err(e) => {
            return opendal_result_operator_new {
                op: std::ptr::null_mut(),
                error: opendal_error::new(e),
            };
        }
    };

    let map: HashMap<_, _> = if options.is_null() {
        HashMap::default()
    } else {
        (*options)
            .deref()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    };

    match core::Operator::via_iter(scheme, map) {
        Ok(mut op) => {
            // Apply common layers like retry
            op = op.layer(core::layers::RetryLayer::new());

            let async_op = Box::into_raw(Box::new(opendal_async_operator {
                inner: Box::into_raw(Box::new(op)),
            }));

            // We reuse opendal_result_operator_new, but the `op` field now points
            // to an opendal_async_operator. The C code needs to cast appropriately.
            opendal_result_operator_new {
                op: async_op as *mut opendal_operator, // Cast needed for return type
                error: std::ptr::null_mut(),
            }
        }
        Err(e) => opendal_result_operator_new {
            op: std::ptr::null_mut(),
            error: opendal_error::new(e),
        },
    }
}

/// \brief Frees an asynchronous OpenDAL Operator.
///
/// # Safety
///
/// `op` must be a valid pointer previously returned by `opendal_async_operator_new`.
/// Calling with NULL does nothing.
#[no_mangle]
pub unsafe extern "C" fn opendal_async_operator_free(op: *const opendal_async_operator) {
    if !op.is_null() {
        // Drop the inner Operator
        drop(Box::from_raw((*op).inner));
        // Drop the container struct itself
        drop(Box::from_raw(op as *mut opendal_async_operator));
    }
}

// --- Async Stat Operation ---

/// \brief Asynchronously gets metadata of a path using a callback.
///
/// @param op A valid pointer to `opendal_async_operator`.
/// @param path The path to the object or directory.
///
/// # Safety
/// `op` must be a valid `opendal_async_operator`.
/// `path` must be a valid, null-terminated C string.
/// \brief Asynchronously gets metadata of a path, returning a future handle.
///
/// The returned future can be awaited via `opendal_future_stat_await` to obtain the
/// resulting metadata or error, mirroring Rust's `async/await` ergonomics.
#[no_mangle]
pub unsafe extern "C" fn opendal_async_operator_stat(
    op: *const opendal_async_operator,
    path: *const c_char,
) -> opendal_result_future_stat {
    if op.is_null() {
        return opendal_result_future_stat {
            future: std::ptr::null_mut(),
            error: opendal_error::new(core::Error::new(
                core::ErrorKind::Unexpected,
                "opendal_async_operator is null",
            )),
        };
    }
    if path.is_null() {
        return opendal_result_future_stat {
            future: std::ptr::null_mut(),
            error: opendal_error::new(core::Error::new(
                core::ErrorKind::Unexpected,
                "path is null",
            )),
        };
    }

    let operator = (*op).as_ref();
    let path_str = match std::ffi::CStr::from_ptr(path).to_str() {
        Ok(s) => s.to_string(),
        Err(e) => {
            return opendal_result_future_stat {
                future: std::ptr::null_mut(),
                error: opendal_error::new(
                    core::Error::new(core::ErrorKind::Unexpected, "invalid path string")
                        .set_source(e),
                ),
            };
        }
    };

    let operator_clone = operator.clone();
    let (tx, rx) = oneshot::channel();
    let handle = crate::operator::RUNTIME.spawn(async move {
        let res = operator_clone.stat(&path_str).await;
        let _ = tx.send(res);
    });
    let future = Box::into_raw(Box::new(opendal_future_stat {
        handle: Box::into_raw(Box::new(Some(handle))),
        rx: Box::into_raw(Box::new(Some(rx))),
    }));

    opendal_result_future_stat {
        future,
        error: std::ptr::null_mut(),
    }
}

impl_await_result!(
    opendal_future_stat_await_impl,
    opendal_future_stat,
    opendal_result_stat,
    |metadata| opendal_result_stat {
        meta: Box::into_raw(Box::new(opendal_metadata::new(metadata))),
        error: std::ptr::null_mut(),
    },
    |e| opendal_result_stat {
        meta: std::ptr::null_mut(),
        error: opendal_error::new(e),
    }
);

#[no_mangle]
pub unsafe extern "C" fn opendal_future_stat_await(
    future: *mut opendal_future_stat,
) -> opendal_result_stat {
    opendal_future_stat_await_impl(future)
}

impl_poll_result!(
    opendal_future_stat_poll,
    opendal_future_stat,
    opendal_result_stat,
    |metadata| opendal_result_stat {
        meta: Box::into_raw(Box::new(opendal_metadata::new(metadata))),
        error: std::ptr::null_mut(),
    },
    |e| opendal_result_stat {
        meta: std::ptr::null_mut(),
        error: opendal_error::new(e),
    }
);

impl_is_ready_fn!(opendal_future_stat_is_ready, opendal_future_stat);

/// \brief Cancel and free a stat future without awaiting it.
#[no_mangle]
pub unsafe extern "C" fn opendal_future_stat_free(future: *mut opendal_future_stat) {
    if future.is_null() {
        return;
    }

    let mut future = Box::from_raw(future);
    if !future.handle.is_null() {
        let mut handle_box = Box::from_raw(future.handle);
        future.handle = std::ptr::null_mut();
        if let Some(handle) = (*handle_box).take() {
            handle.abort();
        }
    }
    if !future.rx.is_null() {
        drop(Box::from_raw(future.rx));
        future.rx = std::ptr::null_mut();
    }
}

// --- Async Read Operation ---

/// \brief Asynchronously reads data from a path.
///
/// The returned future can be awaited via `opendal_future_read_await` to obtain
/// the resulting bytes or error.
#[no_mangle]
pub unsafe extern "C" fn opendal_async_operator_read(
    op: *const opendal_async_operator,
    path: *const c_char,
) -> opendal_result_future_read {
    if op.is_null() {
        return opendal_result_future_read {
            future: std::ptr::null_mut(),
            error: opendal_error::new(core::Error::new(
                core::ErrorKind::Unexpected,
                "opendal_async_operator is null",
            )),
        };
    }
    if path.is_null() {
        return opendal_result_future_read {
            future: std::ptr::null_mut(),
            error: opendal_error::new(core::Error::new(
                core::ErrorKind::Unexpected,
                "path is null",
            )),
        };
    }

    let operator = (*op).as_ref();
    let path_str = match std::ffi::CStr::from_ptr(path).to_str() {
        Ok(s) => s.to_string(),
        Err(e) => {
            return opendal_result_future_read {
                future: std::ptr::null_mut(),
                error: opendal_error::new(
                    core::Error::new(core::ErrorKind::Unexpected, "invalid path string")
                        .set_source(e),
                ),
            };
        }
    };

    let operator_clone = operator.clone();
    let (tx, rx) = oneshot::channel();
    let handle: task::JoinHandle<()> = crate::operator::RUNTIME.spawn(async move {
        let res = operator_clone.read(&path_str).await;
        let _ = tx.send(res);
    });
    let future = Box::into_raw(Box::new(opendal_future_read {
        handle: Box::into_raw(Box::new(Some(handle))),
        rx: Box::into_raw(Box::new(Some(rx))),
    }));

    opendal_result_future_read {
        future,
        error: std::ptr::null_mut(),
    }
}

impl_await_result!(
    opendal_future_read_await_impl,
    opendal_future_read,
    opendal_result_read,
    |buffer| opendal_result_read {
        data: opendal_bytes::new(buffer),
        error: std::ptr::null_mut(),
    },
    |e| opendal_result_read {
        data: opendal_bytes::empty(),
        error: opendal_error::new(e),
    }
);

#[no_mangle]
pub unsafe extern "C" fn opendal_future_read_await(
    future: *mut opendal_future_read,
) -> opendal_result_read {
    opendal_future_read_await_impl(future)
}

impl_poll_result!(
    opendal_future_read_poll,
    opendal_future_read,
    opendal_result_read,
    |buffer| opendal_result_read {
        data: opendal_bytes::new(buffer),
        error: std::ptr::null_mut(),
    },
    |e| opendal_result_read {
        data: opendal_bytes::empty(),
        error: opendal_error::new(e),
    }
);

impl_is_ready_fn!(opendal_future_read_is_ready, opendal_future_read);

/// \brief Cancel and free a read future without awaiting it.
#[no_mangle]
pub unsafe extern "C" fn opendal_future_read_free(future: *mut opendal_future_read) {
    if future.is_null() {
        return;
    }

    let mut future = Box::from_raw(future);
    if !future.handle.is_null() {
        let mut handle_box = Box::from_raw(future.handle);
        future.handle = std::ptr::null_mut();
        if let Some(handle) = (*handle_box).take() {
            handle.abort();
        }
    }
    if !future.rx.is_null() {
        drop(Box::from_raw(future.rx));
        future.rx = std::ptr::null_mut();
    }
}

// --- Async Write Operation ---

/// \brief Asynchronously writes data to a path.
#[no_mangle]
pub unsafe extern "C" fn opendal_async_operator_write(
    op: *const opendal_async_operator,
    path: *const c_char,
    bytes: *const opendal_bytes,
) -> opendal_result_future_write {
    if op.is_null() {
        return opendal_result_future_write {
            future: std::ptr::null_mut(),
            error: opendal_error::new(core::Error::new(
                core::ErrorKind::Unexpected,
                "opendal_async_operator is null",
            )),
        };
    }
    if path.is_null() {
        return opendal_result_future_write {
            future: std::ptr::null_mut(),
            error: opendal_error::new(core::Error::new(
                core::ErrorKind::Unexpected,
                "path is null",
            )),
        };
    }
    if bytes.is_null() {
        return opendal_result_future_write {
            future: std::ptr::null_mut(),
            error: opendal_error::new(core::Error::new(
                core::ErrorKind::Unexpected,
                "bytes is null",
            )),
        };
    }

    let operator = (*op).as_ref();
    let path_str = match std::ffi::CStr::from_ptr(path).to_str() {
        Ok(s) => s.to_string(),
        Err(e) => {
            return opendal_result_future_write {
                future: std::ptr::null_mut(),
                error: opendal_error::new(
                    core::Error::new(core::ErrorKind::Unexpected, "invalid path string")
                        .set_source(e),
                ),
            };
        }
    };

    let buffer: core::Buffer = core::Buffer::from(&*bytes);
    let operator_clone = operator.clone();
    let (tx, rx) = oneshot::channel();
    let handle: task::JoinHandle<()> = crate::operator::RUNTIME.spawn(async move {
        let res = operator_clone.write(&path_str, buffer).await;
        let _ = tx.send(res);
    });
    let future = Box::into_raw(Box::new(opendal_future_write {
        handle: Box::into_raw(Box::new(Some(handle))),
        rx: Box::into_raw(Box::new(Some(rx))),
    }));

    opendal_result_future_write {
        future,
        error: std::ptr::null_mut(),
    }
}

impl_await_error_only!(opendal_future_write_await_impl, opendal_future_write);

#[no_mangle]
pub unsafe extern "C" fn opendal_future_write_await(
    future: *mut opendal_future_write,
) -> *mut opendal_error {
    opendal_future_write_await_impl(future)
}

impl_poll_error_only!(opendal_future_write_poll, opendal_future_write);

impl_is_ready_fn!(opendal_future_write_is_ready, opendal_future_write);

/// \brief Cancel and free a write future without awaiting it.
#[no_mangle]
pub unsafe extern "C" fn opendal_future_write_free(future: *mut opendal_future_write) {
    if future.is_null() {
        return;
    }

    let mut future = Box::from_raw(future);
    if !future.handle.is_null() {
        let mut handle_box = Box::from_raw(future.handle);
        future.handle = std::ptr::null_mut();
        if let Some(handle) = (*handle_box).take() {
            handle.abort();
        }
    }
    if !future.rx.is_null() {
        drop(Box::from_raw(future.rx));
        future.rx = std::ptr::null_mut();
    }
}

// --- Async Delete Operation ---

/// \brief Asynchronously deletes the specified path.
#[no_mangle]
pub unsafe extern "C" fn opendal_async_operator_delete(
    op: *const opendal_async_operator,
    path: *const c_char,
) -> opendal_result_future_delete {
    if op.is_null() {
        return opendal_result_future_delete {
            future: std::ptr::null_mut(),
            error: opendal_error::new(core::Error::new(
                core::ErrorKind::Unexpected,
                "opendal_async_operator is null",
            )),
        };
    }
    if path.is_null() {
        return opendal_result_future_delete {
            future: std::ptr::null_mut(),
            error: opendal_error::new(core::Error::new(
                core::ErrorKind::Unexpected,
                "path is null",
            )),
        };
    }

    let operator = (*op).as_ref();
    let path_str = match std::ffi::CStr::from_ptr(path).to_str() {
        Ok(s) => s.to_string(),
        Err(e) => {
            return opendal_result_future_delete {
                future: std::ptr::null_mut(),
                error: opendal_error::new(
                    core::Error::new(core::ErrorKind::Unexpected, "invalid path string")
                        .set_source(e),
                ),
            };
        }
    };

    let operator_clone = operator.clone();
    let (tx, rx) = oneshot::channel();
    let handle: task::JoinHandle<()> = crate::operator::RUNTIME.spawn(async move {
        let res = operator_clone.delete(&path_str).await;
        let _ = tx.send(res);
    });
    let future = Box::into_raw(Box::new(opendal_future_delete {
        handle: Box::into_raw(Box::new(Some(handle))),
        rx: Box::into_raw(Box::new(Some(rx))),
    }));

    opendal_result_future_delete {
        future,
        error: std::ptr::null_mut(),
    }
}

impl_await_error_only!(opendal_future_delete_await_impl, opendal_future_delete);

#[no_mangle]
pub unsafe extern "C" fn opendal_future_delete_await(
    future: *mut opendal_future_delete,
) -> *mut opendal_error {
    opendal_future_delete_await_impl(future)
}

impl_poll_error_only!(opendal_future_delete_poll, opendal_future_delete);

impl_is_ready_fn!(opendal_future_delete_is_ready, opendal_future_delete);

/// \brief Cancel and free a delete future without awaiting it.
#[no_mangle]
pub unsafe extern "C" fn opendal_future_delete_free(future: *mut opendal_future_delete) {
    if future.is_null() {
        return;
    }

    let mut future = Box::from_raw(future);
    if !future.handle.is_null() {
        let mut handle_box = Box::from_raw(future.handle);
        future.handle = std::ptr::null_mut();
        if let Some(handle) = (*handle_box).take() {
            handle.abort();
        }
    }
    if !future.rx.is_null() {
        drop(Box::from_raw(future.rx));
        future.rx = std::ptr::null_mut();
    }
}
