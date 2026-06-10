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

use std::ffi::c_void;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use ::opendal as core;

use crate::runtime::RUNTIME;

struct CancelState {
    cancelled: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl CancelState {
    fn new() -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            waker: Mutex::new(None),
        }
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        if let Some(waker) = self.waker.lock().expect("cancel waker poisoned").take() {
            waker.wake();
        }
    }
}

#[derive(Clone)]
pub(crate) struct CancelToken {
    state: Arc<CancelState>,
}

impl CancelToken {
    fn new() -> Self {
        Self {
            state: Arc::new(CancelState::new()),
        }
    }

    fn cancel(&self) {
        self.state.cancel();
    }

    fn cancelled(&self) -> Cancelled {
        Cancelled {
            state: self.state.clone(),
        }
    }
}

pub(crate) struct Cancelled {
    state: Arc<CancelState>,
}

impl Future for Cancelled {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.state.cancelled.load(Ordering::SeqCst) {
            return Poll::Ready(());
        }

        *self.state.waker.lock().expect("cancel waker poisoned") = Some(cx.waker().clone());

        if self.state.cancelled.load(Ordering::SeqCst) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

/// \brief A cancellation token for cancellable OpenDAL operations.
#[repr(C)]
pub struct opendal_cancel_token {
    /// The pointer to the Rust cancellation token.
    /// Only touch this on judging whether it is NULL.
    inner: *mut c_void,
}

impl opendal_cancel_token {
    fn deref(&self) -> &CancelToken {
        // Safety: inner is initialized by opendal_cancel_token_new.
        unsafe { &*(self.inner as *mut CancelToken) }
    }

    /// \brief Construct a cancellation token.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_cancel_token_new() -> *mut Self {
        Box::into_raw(Box::new(Self {
            inner: Box::into_raw(Box::new(CancelToken::new())) as _,
        }))
    }

    /// \brief Cancel operations using this token.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_cancel_token_cancel(ptr: *const Self) {
        if !ptr.is_null() {
            unsafe { (*ptr).deref().cancel() };
        }
    }

    /// \brief Free a cancellation token.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_cancel_token_free(ptr: *mut Self) {
        if !ptr.is_null() {
            unsafe {
                drop(Box::from_raw((*ptr).inner as *mut CancelToken));
                drop(Box::from_raw(ptr));
            }
        }
    }
}

pub(crate) unsafe fn clone_token(ptr: *const opendal_cancel_token) -> Option<CancelToken> {
    if ptr.is_null() {
        None
    } else {
        Some(unsafe { (*ptr).deref().clone() })
    }
}

pub(crate) async fn run<T, F>(token: Option<CancelToken>, fut: F) -> core::Result<T>
where
    F: Future<Output = core::Result<T>>,
{
    match token {
        Some(token) => {
            tokio::select! {
                biased;
                _ = token.cancelled() => Err(cancelled_error()),
                result = fut => result,
            }
        }
        None => fut.await,
    }
}

pub(crate) fn cancelled_error() -> core::Error {
    core::Error::new(core::ErrorKind::Unexpected, "operation cancelled")
}

/// Block on a cancellable future on the current runtime thread.
///
/// Use this for futures that capture non-`Send` borrows, such as reader/writer/lister
/// handles accessed through `&mut self`.
pub(crate) fn block_on_cancelable<T, F>(
    token: *const opendal_cancel_token,
    fut: F,
) -> core::Result<T>
where
    F: Future<Output = core::Result<T>>,
{
    let token = unsafe { clone_token(token) };
    RUNTIME.block_on(run(token, fut))
}

/// Block on a cancellable `Send` future by spawning it on the runtime.
///
/// Use this for operator-level calls where the future owns cloned `Operator` state.
pub(crate) fn block_on_cancelable_spawn<T, F>(
    token: *const opendal_cancel_token,
    fut: F,
) -> core::Result<T>
where
    T: Send + 'static,
    F: Future<Output = core::Result<T>> + Send + 'static,
{
    let token = unsafe { clone_token(token) };
    RUNTIME
        .block_on(RUNTIME.spawn(run(token, fut)))
        .map_err(|err| {
            core::Error::new(core::ErrorKind::Unexpected, "cancellable task failed").set_source(err)
        })?
}
