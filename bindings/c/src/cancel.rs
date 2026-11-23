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
use std::sync::atomic::{AtomicBool, Ordering};

/// Internal state behind an `opendal_cancel_token`.
pub(crate) struct CancelTokenInner {
    cancelled: AtomicBool,
    tx: tokio::sync::watch::Sender<bool>,
}

impl CancelTokenInner {
    fn new() -> Self {
        let (tx, _rx) = tokio::sync::watch::channel(false);
        Self {
            cancelled: AtomicBool::new(false),
            tx,
        }
    }

    /// Signal cancellation. Thread-safe, idempotent, and lock-free on repeat calls.
    pub(crate) fn cancel(&self) {
        if !self.cancelled.swap(true, Ordering::AcqRel) {
            self.tx.send_replace(true);
        }
    }

    /// Return whether cancellation has been signalled.
    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Resolve once cancellation has been signalled.
    pub(crate) async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        let mut rx = self.tx.subscribe();
        let _ = rx.wait_for(|&c| c).await;
    }
}

/// Error returned by [`block_on_cancellable`].
pub(crate) enum CancellableError<E> {
    /// The operation was cancelled via an [`opendal_cancel_token`].
    Cancelled,
    /// The underlying future returned an error.
    Inner(E),
}

/// Drive `fut` to completion on `handle`, racing against `cancel` if provided.
///
/// The future is polled in-place on the calling thread and never spawned, so on
/// cancellation `fut` is synchronously dropped before this function returns.
/// Callers can safely release buffers and handles immediately after return.
pub(crate) fn block_on_cancellable<F, T, E>(
    handle: &tokio::runtime::Handle,
    cancel: Option<&CancelTokenInner>,
    fut: F,
) -> Result<T, CancellableError<E>>
where
    F: Future<Output = Result<T, E>>,
{
    if let Some(tok) = cancel {
        if tok.is_cancelled() {
            return Err(CancellableError::Cancelled);
        }
        handle.block_on(async move {
            tokio::select! {
                biased;
                _ = tok.cancelled() => Err(CancellableError::Cancelled),
                res = fut => res.map_err(CancellableError::Inner),
            }
        })
    } else {
        handle.block_on(fut).map_err(CancellableError::Inner)
    }
}

/// \brief An opaque, thread-safe token used to cancel in-flight OpenDAL operations.
///
/// Pass a token created with `opendal_cancel_token_new()` to any `*_with_cancel`
/// function. Calling `opendal_cancel_token_cancel()` from another thread wakes
/// waiting operations and causes them to drop their in-flight work and return
/// `OPENDAL_CANCELLED`.
///
/// A single token may be shared across multiple concurrent operations, and
/// cancelling it is idempotent. Free the token with `opendal_cancel_token_free()`
/// only after all operations using it have returned.
///
/// @see opendal_cancel_token_new()
/// @see opendal_cancel_token_cancel()
/// @see opendal_cancel_token_is_cancelled()
/// @see opendal_cancel_token_free()
#[repr(C)]
pub struct opendal_cancel_token {
    /// The pointer to the internal Rust cancel token state.
    /// Only touch this on judging whether it is NULL.
    inner: *mut c_void,
}

impl opendal_cancel_token {
    /// Borrow the underlying [`CancelTokenInner`] if `token` is non-null.
    pub(crate) unsafe fn as_inner<'a>(
        token: *const opendal_cancel_token,
    ) -> Option<&'a CancelTokenInner> {
        if token.is_null() {
            return None;
        }
        let inner = (*token).inner as *const CancelTokenInner;
        if inner.is_null() {
            None
        } else {
            Some(&*inner)
        }
    }

    /// \brief Allocate a new uncancelled `opendal_cancel_token`.
    ///
    /// Free the returned token with `opendal_cancel_token_free()` once no
    /// operations are using it.
    #[no_mangle]
    pub extern "C" fn opendal_cancel_token_new() -> *mut opendal_cancel_token {
        let inner = Box::into_raw(Box::new(CancelTokenInner::new())) as *mut c_void;
        Box::into_raw(Box::new(opendal_cancel_token { inner }))
    }

    /// \brief Signal cancellation on `token`.
    ///
    /// Safe to call from any thread while operations are in flight, safe to call
    /// multiple times, and a no-op when `token` is NULL.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_cancel_token_cancel(token: *const opendal_cancel_token) {
        if let Some(inner) = Self::as_inner(token) {
            inner.cancel();
        }
    }

    /// \brief Return whether `token` has been cancelled.
    ///
    /// Returns `false` when `token` is NULL.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_cancel_token_is_cancelled(
        token: *const opendal_cancel_token,
    ) -> bool {
        Self::as_inner(token).is_some_and(CancelTokenInner::is_cancelled)
    }

    /// \brief Free the heap memory used by `opendal_cancel_token`.
    ///
    /// Safe to call on NULL. Must only be called after every operation that
    /// received `token` has returned.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_cancel_token_free(token: *mut opendal_cancel_token) {
        if !token.is_null() {
            let inner = (*token).inner as *mut CancelTokenInner;
            if !inner.is_null() {
                drop(Box::from_raw(inner));
            }
            (*token).inner = std::ptr::null_mut();
            drop(Box::from_raw(token));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Condvar, Mutex};
    use std::task::{Context, Poll};

    use ::opendal as core;
    use core::raw::{self, oio};

    use super::*;
    use crate::copier::opendal_copier;
    use crate::error::{opendal_code, opendal_error};
    use crate::operator::{
        opendal_operator, opendal_operator_copier_with_cancel, opendal_operator_read_with_cancel,
        opendal_operator_reader_with_cancel, opendal_operator_stat_with_cancel,
    };
    use crate::reader::opendal_reader;
    use crate::types::opendal_bytes;

    #[derive(Debug)]
    struct Gate {
        entered: Mutex<bool>,
        cv: Condvar,
    }

    impl Gate {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                entered: Mutex::new(false),
                cv: Condvar::new(),
            })
        }

        fn signal(&self) {
            let mut guard = self.entered.lock().unwrap();
            *guard = true;
            self.cv.notify_all();
        }

        fn wait(&self) {
            let mut guard = self.entered.lock().unwrap();
            while !*guard {
                guard = self.cv.wait(guard).unwrap();
            }
        }

        fn reset(&self) {
            let mut guard = self.entered.lock().unwrap();
            *guard = false;
        }
    }

    /// Wrapper so a raw pointer can be sent to a test helper thread that cancels it.
    struct SendCancelPtr(*const opendal_cancel_token);
    unsafe impl Send for SendCancelPtr {}

    #[test]
    fn test_cancel_token_lifecycle_and_null_safety() {
        unsafe {
            opendal_cancel_token::opendal_cancel_token_cancel(std::ptr::null());
            assert!(!opendal_cancel_token::opendal_cancel_token_is_cancelled(
                std::ptr::null()
            ));
            opendal_cancel_token::opendal_cancel_token_free(std::ptr::null_mut());

            let tok = opendal_cancel_token::opendal_cancel_token_new();
            assert!(!tok.is_null());
            assert!(!opendal_cancel_token::opendal_cancel_token_is_cancelled(
                tok
            ));

            opendal_cancel_token::opendal_cancel_token_cancel(tok);
            assert!(opendal_cancel_token::opendal_cancel_token_is_cancelled(tok));

            // Cancelling a second time is idempotent.
            opendal_cancel_token::opendal_cancel_token_cancel(tok);
            assert!(opendal_cancel_token::opendal_cancel_token_is_cancelled(tok));

            opendal_cancel_token::opendal_cancel_token_free(tok);
        }
    }

    #[test]
    fn test_block_on_cancellable_pre_cancelled_and_null_token() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        // NULL token runs the future to completion.
        let res: Result<usize, CancellableError<()>> =
            block_on_cancellable(rt.handle(), None, async { Ok(42) });
        assert!(matches!(res, Ok(42)));

        // Pre-cancelled token returns Cancelled without ever polling the future.
        struct TrackedFuture {
            polled: Arc<AtomicBool>,
            dropped: Arc<AtomicBool>,
        }
        impl Future for TrackedFuture {
            type Output = Result<(), ()>;
            fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
                self.polled.store(true, Ordering::SeqCst);
                Poll::Ready(Ok(()))
            }
        }
        impl Drop for TrackedFuture {
            fn drop(&mut self) {
                self.dropped.store(true, Ordering::SeqCst);
            }
        }

        let inner = CancelTokenInner::new();
        inner.cancel();
        let polled = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicBool::new(false));

        let res = block_on_cancellable(
            rt.handle(),
            Some(&inner),
            TrackedFuture {
                polled: polled.clone(),
                dropped: dropped.clone(),
            },
        );
        assert!(matches!(res, Err(CancellableError::Cancelled)));
        assert!(!polled.load(Ordering::SeqCst));
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn test_block_on_cancellable_drops_future_synchronously_before_return() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        struct BorrowedPendingFuture<'a> {
            gate: Arc<Gate>,
            dropped: Arc<AtomicBool>,
            buf: &'a mut [u8],
        }

        impl Future for BorrowedPendingFuture<'_> {
            type Output = Result<(), ()>;

            fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
                self.buf[0] = 0xAB;
                self.gate.signal();
                Poll::Pending
            }
        }

        impl Drop for BorrowedPendingFuture<'_> {
            fn drop(&mut self) {
                self.dropped.store(true, Ordering::SeqCst);
            }
        }

        let gate = Gate::new();
        let dropped = Arc::new(AtomicBool::new(false));
        let tok = opendal_cancel_token::opendal_cancel_token_new();
        let send_tok = SendCancelPtr(tok);
        let gate_bg = gate.clone();

        let cancel_thread = std::thread::spawn(move || {
            let send_tok = send_tok;
            gate_bg.wait();
            unsafe { opendal_cancel_token::opendal_cancel_token_cancel(send_tok.0) };
        });

        let mut caller_buf = vec![0u8; 64];
        let res = block_on_cancellable(
            rt.handle(),
            unsafe { opendal_cancel_token::as_inner(tok) },
            BorrowedPendingFuture {
                gate,
                dropped: dropped.clone(),
                buf: &mut caller_buf,
            },
        );

        // Immediately upon return, the future MUST already be dropped so freeing
        // caller_buf cannot race with any background task.
        assert!(matches!(res, Err(CancellableError::Cancelled)));
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(caller_buf[0], 0xAB);
        drop(caller_buf);

        cancel_thread.join().unwrap();
        unsafe { opendal_cancel_token::opendal_cancel_token_free(tok) };
    }

    #[derive(Debug, Clone)]
    struct BlockingLayer {
        gate: Arc<Gate>,
        block_stat: Arc<AtomicBool>,
        block_read: Arc<AtomicBool>,
    }

    impl raw::Layer for BlockingLayer {
        fn apply_service(&self, inner: raw::Servicer) -> raw::Servicer {
            Arc::new(BlockingService {
                inner,
                gate: self.gate.clone(),
                block_stat: self.block_stat.clone(),
                block_read: self.block_read.clone(),
            })
        }
    }

    #[derive(Debug)]
    struct BlockingService {
        inner: raw::Servicer,
        gate: Arc<Gate>,
        block_stat: Arc<AtomicBool>,
        block_read: Arc<AtomicBool>,
    }

    struct BlockingReader {
        inner: oio::Reader,
        gate: Arc<Gate>,
        block_read: Arc<AtomicBool>,
    }

    impl oio::Read for BlockingReader {
        async fn open(
            &self,
            range: core::BytesRange,
        ) -> core::Result<(raw::RpRead, Box<dyn oio::ReadStreamDyn>)> {
            if self.block_read.load(Ordering::SeqCst) {
                self.gate.signal();
                std::future::pending::<()>().await;
            }
            self.inner.open_dyn(range).await
        }

        async fn read(&self, range: core::BytesRange) -> core::Result<(raw::RpRead, core::Buffer)> {
            if self.block_read.load(Ordering::SeqCst) {
                self.gate.signal();
                std::future::pending::<()>().await;
            }
            self.inner.read_dyn(range).await
        }
    }

    struct TestCopier {
        inner: raw::Servicer,
        ctx: core::OperationContext,
        from: String,
        to: String,
        copied: bool,
    }

    impl oio::Copy for TestCopier {
        async fn next(&mut self) -> core::Result<Option<usize>> {
            if self.copied {
                return Ok(None);
            }
            let r = self
                .inner
                .read_dyn(&self.ctx, &self.from, raw::OpRead::new())?;
            let (_, mut stream) = r.open_dyn(core::BytesRange::default()).await?;
            let buf = stream.read_all_dyn().await?;
            let len = buf.len();
            let mut w = self
                .inner
                .write_dyn(&self.ctx, &self.to, raw::OpWrite::new())?;
            w.write_dyn(buf).await?;
            let _ = w.close_dyn().await?;
            self.copied = true;
            Ok(Some(len))
        }

        async fn close(&mut self) -> core::Result<core::Metadata> {
            Ok(core::MetadataBuilder::unknown().build())
        }

        async fn abort(&mut self) -> core::Result<()> {
            self.copied = true;
            Ok(())
        }
    }

    impl raw::Service for BlockingService {
        type Reader = BlockingReader;
        type Writer = oio::Writer;
        type Lister = oio::Lister;
        type Deleter = oio::Deleter;
        type Copier = TestCopier;
        type Composer = oio::Composer;

        fn info(&self) -> raw::ServiceInfo {
            self.inner.info()
        }

        fn capability(&self) -> core::Capability {
            let mut cap = self.inner.capability();
            cap.copy = true;
            cap
        }

        async fn create_dir(
            &self,
            ctx: &core::OperationContext,
            path: &str,
            args: raw::OpCreateDir,
        ) -> core::Result<raw::RpCreateDir> {
            self.inner.create_dir(ctx, path, args).await
        }

        async fn stat(
            &self,
            ctx: &core::OperationContext,
            path: &str,
            args: raw::OpStat,
        ) -> core::Result<raw::RpStat> {
            if self.block_stat.load(Ordering::SeqCst) {
                self.gate.signal();
                std::future::pending::<()>().await;
            }
            self.inner.stat(ctx, path, args).await
        }

        fn read(
            &self,
            ctx: &core::OperationContext,
            path: &str,
            args: raw::OpRead,
        ) -> core::Result<Self::Reader> {
            let inner = self.inner.read(ctx, path, args)?;
            Ok(BlockingReader {
                inner,
                gate: self.gate.clone(),
                block_read: self.block_read.clone(),
            })
        }

        fn write(
            &self,
            ctx: &core::OperationContext,
            path: &str,
            args: raw::OpWrite,
        ) -> core::Result<Self::Writer> {
            self.inner.write(ctx, path, args)
        }

        fn delete(&self, ctx: &core::OperationContext) -> core::Result<Self::Deleter> {
            self.inner.delete(ctx)
        }

        fn list(
            &self,
            ctx: &core::OperationContext,
            path: &str,
            args: raw::OpList,
        ) -> core::Result<Self::Lister> {
            self.inner.list(ctx, path, args)
        }

        fn copy(
            &self,
            ctx: &core::OperationContext,
            from: &str,
            to: &str,
            _args: raw::OpCopy,
        ) -> core::Result<Self::Copier> {
            Ok(TestCopier {
                inner: self.inner.clone(),
                ctx: ctx.clone(),
                from: from.to_string(),
                to: to.to_string(),
                copied: false,
            })
        }

        fn compose(
            &self,
            ctx: &core::OperationContext,
            to: &str,
            args: raw::OpCompose,
        ) -> core::Result<Self::Composer> {
            self.inner.compose(ctx, to, args)
        }

        async fn rename(
            &self,
            ctx: &core::OperationContext,
            from: &str,
            to: &str,
            args: raw::OpRename,
        ) -> core::Result<raw::RpRename> {
            self.inner.rename(ctx, from, to, args).await
        }

        async fn restore(
            &self,
            ctx: &core::OperationContext,
            path: &str,
            args: raw::OpRestore,
        ) -> core::Result<raw::RpRestore> {
            self.inner.restore(ctx, path, args).await
        }

        async fn presign(
            &self,
            ctx: &core::OperationContext,
            path: &str,
            args: raw::OpPresign,
        ) -> core::Result<raw::RpPresign> {
            self.inner.presign(ctx, path, args).await
        }
    }

    #[test]
    fn test_ffi_in_flight_cancellation_with_blocking_layer() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        let gate = Gate::new();
        let block_stat = Arc::new(AtomicBool::new(false));
        let block_read = Arc::new(AtomicBool::new(false));

        let op = core::Operator::new(core::services::Memory::default())
            .unwrap()
            .layer(BlockingLayer {
                gate: gate.clone(),
                block_stat: block_stat.clone(),
                block_read: block_read.clone(),
            });

        rt.block_on(async {
            op.write("file.txt", "hello cancellation").await.unwrap();
        });

        let c_op = opendal_operator::from_operator(rt.handle().clone(), op);
        let path = CString::new("file.txt").unwrap();

        // 1. Cancel in-flight opendal_operator_read_with_cancel
        gate.reset();
        block_read.store(true, Ordering::SeqCst);
        let tok = opendal_cancel_token::opendal_cancel_token_new();
        let send_tok = SendCancelPtr(tok);
        let gate_bg = gate.clone();
        let t = std::thread::spawn(move || {
            let send_tok = send_tok;
            gate_bg.wait();
            unsafe { opendal_cancel_token::opendal_cancel_token_cancel(send_tok.0) };
        });

        let mut bytes = opendal_bytes::empty();
        let err = unsafe {
            opendal_operator_read_with_cancel(
                &*c_op,
                path.as_ptr(),
                std::ptr::null(),
                tok,
                &mut bytes,
            )
        };
        t.join().unwrap();
        assert!(!err.is_null());
        assert_eq!(unsafe { (*err).code() }, opendal_code::OPENDAL_CANCELLED);
        assert!(bytes.data.is_null());
        assert_eq!(bytes.len, 0);
        unsafe {
            opendal_error::opendal_error_free(err);
            opendal_cancel_token::opendal_cancel_token_free(tok);
        }

        // 2. Cancel in-flight opendal_operator_stat_with_cancel
        gate.reset();
        block_read.store(false, Ordering::SeqCst);
        block_stat.store(true, Ordering::SeqCst);
        let tok = opendal_cancel_token::opendal_cancel_token_new();
        let send_tok = SendCancelPtr(tok);
        let gate_bg = gate.clone();
        let t = std::thread::spawn(move || {
            let send_tok = send_tok;
            gate_bg.wait();
            unsafe { opendal_cancel_token::opendal_cancel_token_cancel(send_tok.0) };
        });

        let mut meta = std::ptr::null_mut();
        let err = unsafe {
            opendal_operator_stat_with_cancel(
                &*c_op,
                path.as_ptr(),
                std::ptr::null(),
                tok,
                &mut meta,
            )
        };
        t.join().unwrap();
        assert!(!err.is_null());
        assert_eq!(unsafe { (*err).code() }, opendal_code::OPENDAL_CANCELLED);
        assert!(meta.is_null());
        unsafe {
            opendal_error::opendal_error_free(err);
            opendal_cancel_token::opendal_cancel_token_free(tok);
        }

        // 2b. Cancel in-flight opendal_operator_reader_with_cancel during initial stat/open
        gate.reset();
        block_read.store(false, Ordering::SeqCst);
        block_stat.store(true, Ordering::SeqCst);
        let tok = opendal_cancel_token::opendal_cancel_token_new();
        let send_tok = SendCancelPtr(tok);
        let gate_bg = gate.clone();
        let t = std::thread::spawn(move || {
            let send_tok = send_tok;
            gate_bg.wait();
            unsafe { opendal_cancel_token::opendal_cancel_token_cancel(send_tok.0) };
        });

        let mut reader = std::ptr::null_mut();
        let reader_opts = crate::types::opendal_reader_options {
            has_chunk: true,
            chunk: 4,
            ..Default::default()
        };
        let err = unsafe {
            opendal_operator_reader_with_cancel(
                &*c_op,
                path.as_ptr(),
                &reader_opts,
                tok,
                &mut reader,
            )
        };
        t.join().unwrap();
        assert!(!err.is_null());
        assert_eq!(unsafe { (*err).code() }, opendal_code::OPENDAL_CANCELLED);
        assert!(reader.is_null());
        unsafe {
            opendal_error::opendal_error_free(err);
            opendal_cancel_token::opendal_cancel_token_free(tok);
        }

        // 3. Cancel in-flight opendal_reader_read_with_cancel
        block_stat.store(false, Ordering::SeqCst);
        block_read.store(false, Ordering::SeqCst);
        let mut reader = std::ptr::null_mut();
        let err = unsafe {
            opendal_operator_reader_with_cancel(
                &*c_op,
                path.as_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                &mut reader,
            )
        };
        assert!(err.is_null());
        assert!(!reader.is_null());

        gate.reset();
        block_read.store(true, Ordering::SeqCst);
        let tok = opendal_cancel_token::opendal_cancel_token_new();
        let send_tok = SendCancelPtr(tok);
        let gate_bg = gate.clone();
        let t = std::thread::spawn(move || {
            let send_tok = send_tok;
            gate_bg.wait();
            unsafe { opendal_cancel_token::opendal_cancel_token_cancel(send_tok.0) };
        });

        let mut buf = vec![0u8; 32];
        let mut read_size = 999usize;
        let err = unsafe {
            opendal_reader::opendal_reader_read_with_cancel(
                &mut *reader,
                buf.as_mut_ptr(),
                buf.len(),
                tok,
                &mut read_size,
            )
        };
        t.join().unwrap();
        assert!(!err.is_null());
        assert_eq!(unsafe { (*err).code() }, opendal_code::OPENDAL_CANCELLED);
        assert_eq!(read_size, 0);
        drop(buf);
        unsafe {
            opendal_error::opendal_error_free(err);
            opendal_cancel_token::opendal_cancel_token_free(tok);
            opendal_reader::opendal_reader_free(reader);
        }

        // 4. Copier preserves state on cancellation so caller can resume and complete
        block_stat.store(false, Ordering::SeqCst);
        block_read.store(false, Ordering::SeqCst);
        let dest = CString::new("file_copy.txt").unwrap();
        let mut copier = std::ptr::null_mut();
        let err = unsafe {
            opendal_operator_copier_with_cancel(
                &*c_op,
                path.as_ptr(),
                dest.as_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                &mut copier,
            )
        };
        assert!(err.is_null());
        assert!(!copier.is_null());

        let pre_cancelled = opendal_cancel_token::opendal_cancel_token_new();
        unsafe { opendal_cancel_token::opendal_cancel_token_cancel(pre_cancelled) };
        let mut size = 77usize;
        let mut has_next = true;
        let err = unsafe {
            opendal_copier::opendal_copier_next_with_cancel(
                &mut *copier,
                pre_cancelled,
                &mut size,
                &mut has_next,
            )
        };
        assert!(!err.is_null());
        assert_eq!(unsafe { (*err).code() }, opendal_code::OPENDAL_CANCELLED);
        assert_eq!(size, 0);
        assert!(!has_next);
        unsafe {
            opendal_error::opendal_error_free(err);
            opendal_cancel_token::opendal_cancel_token_free(pre_cancelled);
        }

        // Resume with NULL cancel token and drive to completion.
        loop {
            let err = unsafe {
                opendal_copier::opendal_copier_next_with_cancel(
                    &mut *copier,
                    std::ptr::null(),
                    &mut size,
                    &mut has_next,
                )
            };
            assert!(err.is_null());
            if !has_next {
                break;
            }
        }

        let mut copied = opendal_bytes::empty();
        let err = unsafe {
            opendal_operator_read_with_cancel(
                &*c_op,
                dest.as_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                &mut copied,
            )
        };
        assert!(err.is_null());
        let copied_slice = unsafe { std::slice::from_raw_parts(copied.data, copied.len) };
        assert_eq!(copied_slice, b"hello cancellation");

        unsafe {
            opendal_bytes::opendal_bytes_free(&mut copied);
            opendal_copier::opendal_copier_free(copier);
            opendal_operator::opendal_operator_free(c_op);
        }
    }
}
