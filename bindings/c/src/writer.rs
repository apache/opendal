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

use ::opendal as core;

use super::*;
use crate::cancel::block_on_cancellable;

struct WriterInner {
    handle: tokio::runtime::Handle,
    writer: Option<core::Writer>,
}

impl Drop for WriterInner {
    fn drop(&mut self) {
        if let Some(v) = self.writer.take() {
            self.handle.block_on(async move { drop(v) });
        }
    }
}

/// \brief The result type returned by opendal's writer operation.
/// \note The opendal_writer owns an async `opendal::Writer` driven on the
/// operator's Tokio runtime handle.
#[repr(C)]
pub struct opendal_writer {
    /// The pointer to the internal Rust writer state.
    /// Only touch this on judging whether it is NULL.
    inner: *mut c_void,
}

impl opendal_writer {
    fn deref_mut(&mut self) -> &mut WriterInner {
        // Safety: the inner should never be null once constructed
        // The use-after-free is undefined behavior
        unsafe { &mut *(self.inner as *mut WriterInner) }
    }
}

impl opendal_writer {
    pub(crate) fn new(handle: tokio::runtime::Handle, writer: core::Writer) -> Self {
        Self {
            inner: Box::into_raw(Box::new(WriterInner {
                handle,
                writer: Some(writer),
            })) as _,
        }
    }

    /// \brief Write data to the writer and write the number of bytes written to `*size`.
    ///
    /// On failure, writes `0` to `*size` and returns an `opendal_error *`.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_writer_write(
        &mut self,
        bytes: &opendal_bytes,
        size: *mut usize,
    ) -> *mut opendal_error {
        self.opendal_writer_write_with_cancel(bytes, std::ptr::null(), size)
    }

    /// \brief Write data to the writer, supporting cancellation via `cancel`.
    ///
    /// Pass NULL for `cancel` to run without cancellation. On cancellation or failure,
    /// writes `0` to `*size` and returns an `opendal_error *` (`OPENDAL_CANCELLED` on
    /// cancellation).
    ///
    /// If cancelled mid-flight, the backend may already have accepted part or all of
    /// the write; release the handle with `opendal_writer_free` and start a new writer
    /// instead of continuing to write or close, which can duplicate or corrupt data on
    /// streaming or append backends.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_writer_write_with_cancel(
        &mut self,
        bytes: &opendal_bytes,
        cancel: *const opendal_cancel_token,
        size: *mut usize,
    ) -> *mut opendal_error {
        assert!(!size.is_null());
        std::ptr::write(size, 0);
        let len = bytes.len;
        let bytes = match bytes.to_buffer() {
            Ok(bytes) => bytes,
            Err(e) => return opendal_error::new(e),
        };
        let cancel_token = opendal_cancel_token::as_inner(cancel);
        let inner = self.deref_mut();
        let writer = inner.writer.as_mut().expect("writer has been dropped");
        match block_on_cancellable(&inner.handle, cancel_token, writer.write(bytes)) {
            Ok(()) => {
                std::ptr::write(size, len);
                std::ptr::null_mut()
            }
            Err(e) => opendal_error::from_cancellable(e),
        }
    }

    /// \brief Close the writer and make sure all data have been stored.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_writer_close(ptr: *mut opendal_writer) -> *mut opendal_error {
        Self::opendal_writer_close_with_cancel(ptr, std::ptr::null(), std::ptr::null_mut())
    }

    /// \brief Close the writer and write the written object's metadata to `*meta`.
    ///
    /// Like `opendal_writer_close`, but on success writes the metadata of the
    /// written object (e.g. etag, version, last modified) to `*meta` instead of discarding it.
    ///
    /// @param ptr The opendal_writer to close
    /// @param meta Output pointer receiving the allocated `opendal_metadata *` on success, or NULL on failure
    /// @return NULL on success, or an `opendal_error *` on failure.
    ///
    /// \note The returned metadata must be freed with opendal_metadata_free().
    #[no_mangle]
    pub unsafe extern "C" fn opendal_writer_close_with_metadata(
        ptr: *mut opendal_writer,
        meta: *mut *mut opendal_metadata,
    ) -> *mut opendal_error {
        assert!(!meta.is_null());
        Self::opendal_writer_close_with_cancel(ptr, std::ptr::null(), meta)
    }

    /// \brief Close the writer, supporting cancellation via `cancel` and optional metadata output.
    ///
    /// Pass NULL for `cancel` to run without cancellation. Pass NULL for `meta` to
    /// discard the written object's metadata; when non-null, `*meta` receives the
    /// allocated `opendal_metadata *` on success or NULL on cancellation or failure.
    ///
    /// If cancelled mid-flight, the finalization state on the backend is unknown;
    /// release the handle with `opendal_writer_free` and start a new writer instead of
    /// continuing to write or close, which can duplicate or corrupt data on streaming
    /// or append backends.
    ///
    /// @param ptr The opendal_writer to close (NULL is a no-op returning NULL)
    /// @param cancel Optional cancel token, or NULL
    /// @param meta Optional output pointer for the written object's metadata, or NULL
    /// @return NULL on success, or an `opendal_error *` on cancellation or failure.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_writer_close_with_cancel(
        ptr: *mut opendal_writer,
        cancel: *const opendal_cancel_token,
        meta: *mut *mut opendal_metadata,
    ) -> *mut opendal_error {
        if !meta.is_null() {
            std::ptr::write(meta, std::ptr::null_mut());
        }
        if ptr.is_null() {
            return std::ptr::null_mut();
        }
        let cancel_token = opendal_cancel_token::as_inner(cancel);
        let inner = (*ptr).deref_mut();
        let writer = inner.writer.as_mut().expect("writer has been dropped");
        match block_on_cancellable(&inner.handle, cancel_token, writer.close()) {
            Ok(m) => {
                if !meta.is_null() {
                    std::ptr::write(meta, Box::into_raw(Box::new(opendal_metadata::new(m))));
                }
                std::ptr::null_mut()
            }
            Err(e) => opendal_error::from_cancellable(e),
        }
    }

    /// \brief Frees the heap memory used by the opendal_writer.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_writer_free(ptr: *mut opendal_writer) {
        unsafe {
            if !ptr.is_null() {
                drop(Box::from_raw((*ptr).inner as *mut WriterInner));
                drop(Box::from_raw(ptr));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::opendal::raw::{self, oio};
    use std::sync::Arc;

    #[derive(Debug)]
    struct FailingWriteService {
        inner: raw::Servicer,
    }

    #[derive(Debug)]
    struct FailingWriteLayer;

    impl raw::Layer for FailingWriteLayer {
        fn apply_service(&self, inner: raw::Servicer) -> raw::Servicer {
            Arc::new(FailingWriteService { inner })
        }
    }

    struct FailingWriter;

    impl oio::Write for FailingWriter {
        async fn write(&mut self, _bs: core::Buffer) -> core::Result<()> {
            Err(core::Error::new(
                core::ErrorKind::ConditionNotMatch,
                "precondition failed on write",
            ))
        }

        async fn close(&mut self) -> core::Result<core::Metadata> {
            Err(core::Error::new(
                core::ErrorKind::PermissionDenied,
                "permission denied on close",
            ))
        }

        async fn abort(&mut self) -> core::Result<()> {
            Ok(())
        }
    }

    impl raw::Service for FailingWriteService {
        type Reader = oio::Reader;
        type Writer = FailingWriter;
        type Lister = oio::Lister;
        type Deleter = oio::Deleter;
        type Copier = oio::Copier;
        type Composer = oio::Composer;

        fn info(&self) -> raw::ServiceInfo {
            self.inner.info()
        }

        fn capability(&self) -> core::Capability {
            let mut cap = self.inner.capability();
            cap.write = true;
            cap.write_can_multi = true;
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
            self.inner.stat(ctx, path, args).await
        }

        fn read(
            &self,
            ctx: &core::OperationContext,
            path: &str,
            args: raw::OpRead,
        ) -> core::Result<Self::Reader> {
            self.inner.read(ctx, path, args)
        }

        fn write(
            &self,
            _ctx: &core::OperationContext,
            _path: &str,
            _args: raw::OpWrite,
        ) -> core::Result<Self::Writer> {
            Ok(FailingWriter)
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
            args: raw::OpCopy,
        ) -> core::Result<Self::Copier> {
            self.inner.copy(ctx, from, to, args)
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

        fn compose(
            &self,
            ctx: &core::OperationContext,
            path: &str,
            args: raw::OpCompose,
        ) -> core::Result<Self::Composer> {
            self.inner.compose(ctx, path, args)
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
    fn test_writer_preserves_backend_error_kinds() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let op = core::Operator::new(core::services::Memory::default())
            .unwrap()
            .layer(FailingWriteLayer);
        let writer = runtime.block_on(op.writer("test")).unwrap();
        let writer = Box::into_raw(Box::new(opendal_writer::new(
            runtime.handle().clone(),
            writer,
        )));

        let payload = b"hello";
        let bytes = std::mem::ManuallyDrop::new(opendal_bytes {
            data: payload.as_ptr() as *mut u8,
            len: payload.len(),
            capacity: payload.len(),
        });
        let mut written = 99;
        let err =
            unsafe { opendal_writer::opendal_writer_write(&mut *writer, &bytes, &mut written) };
        assert!(!err.is_null());
        assert_eq!(
            unsafe { (*err).code() },
            opendal_code::OPENDAL_CONDITION_NOT_MATCH
        );
        assert_eq!(written, 0);
        unsafe { opendal_error::opendal_error_free(err) };

        let mut meta = std::ptr::dangling_mut();
        let err = unsafe { opendal_writer::opendal_writer_close_with_metadata(writer, &mut meta) };
        assert!(!err.is_null());
        assert_eq!(
            unsafe { (*err).code() },
            opendal_code::OPENDAL_PERMISSION_DENIED
        );
        assert!(meta.is_null());
        unsafe {
            opendal_error::opendal_error_free(err);
            opendal_writer::opendal_writer_free(writer);
        }
    }
}
