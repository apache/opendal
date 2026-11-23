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
use std::io::SeekFrom;

use ::opendal as core;
use futures::{AsyncReadExt, AsyncSeekExt};

use super::*;
use crate::cancel::{block_on_cancellable, CancellableError};

/// Seek from the start of the stream.
pub const OPENDAL_SEEK_SET: i32 = 0;
/// Seek relative to the current stream position.
pub const OPENDAL_SEEK_CUR: i32 = 1;
/// Seek relative to the end of the stream.
pub const OPENDAL_SEEK_END: i32 = 2;

struct ReaderInner {
    handle: tokio::runtime::Handle,
    reader: Option<core::FuturesAsyncReader>,
}

impl Drop for ReaderInner {
    fn drop(&mut self) {
        if let Some(v) = self.reader.take() {
            self.handle.block_on(async move { drop(v) });
        }
    }
}

/// \brief The result type returned by opendal's reader operation.
///
/// \note The opendal_reader owns an async OpenDAL reader driven on the
/// operator's Tokio runtime handle.
#[repr(C)]
pub struct opendal_reader {
    /// The pointer to the internal Rust reader state.
    /// Only touch this on judging whether it is NULL.
    inner: *mut c_void,
}

fn reader_io_error(e: std::io::Error, context: &str) -> *mut opendal_error {
    if e.get_ref().is_some_and(|s| s.is::<core::Error>()) {
        let err = e
            .into_inner()
            .expect("io error inner was checked to be present")
            .downcast::<core::Error>()
            .expect("io error inner was checked to be opendal::Error");
        return opendal_error::new(*err);
    }

    let kind = match e.kind() {
        std::io::ErrorKind::NotFound => core::ErrorKind::NotFound,
        std::io::ErrorKind::PermissionDenied => core::ErrorKind::PermissionDenied,
        _ => core::ErrorKind::Unexpected,
    };
    opendal_error::new(core::Error::new(kind, context).set_source(e))
}

impl opendal_reader {
    fn deref_mut(&mut self) -> &mut ReaderInner {
        // Safety: the inner should never be null once constructed
        // The use-after-free is undefined behavior
        unsafe { &mut *(self.inner as *mut ReaderInner) }
    }
}

impl opendal_reader {
    pub(crate) fn new(handle: tokio::runtime::Handle, reader: core::FuturesAsyncReader) -> Self {
        Self {
            inner: Box::into_raw(Box::new(ReaderInner {
                handle,
                reader: Some(reader),
            })) as _,
        }
    }

    /// \brief Read data from the reader into `buf` and write the number of bytes read to `*size`.
    ///
    /// At EOF, writes `0` to `*size` and returns NULL. On failure, writes `0` to `*size`
    /// and returns an `opendal_error *`.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_reader_read(
        &mut self,
        buf: *mut u8,
        len: usize,
        size: *mut usize,
    ) -> *mut opendal_error {
        self.opendal_reader_read_with_cancel(buf, len, std::ptr::null(), size)
    }

    /// \brief Read data from the reader into `buf`, supporting cancellation via `cancel`.
    ///
    /// Pass NULL for `cancel` to run without cancellation. At EOF, writes `0` to
    /// `*size` and returns NULL. On cancellation or failure, writes `0` to `*size`
    /// and returns an `opendal_error *` (`OPENDAL_CANCELLED` on cancellation).
    #[no_mangle]
    pub unsafe extern "C" fn opendal_reader_read_with_cancel(
        &mut self,
        buf: *mut u8,
        len: usize,
        cancel: *const opendal_cancel_token,
        size: *mut usize,
    ) -> *mut opendal_error {
        assert!(!buf.is_null());
        assert!(!size.is_null());
        std::ptr::write(size, 0);
        let buf = std::slice::from_raw_parts_mut(buf, len);
        let cancel_token = opendal_cancel_token::as_inner(cancel);
        let inner = self.deref_mut();
        let reader = inner.reader.as_mut().expect("reader has been dropped");
        match block_on_cancellable(&inner.handle, cancel_token, reader.read(buf)) {
            Ok(n) => {
                std::ptr::write(size, n);
                std::ptr::null_mut()
            }
            Err(CancellableError::Cancelled) => opendal_error::cancelled(),
            Err(CancellableError::Inner(e)) => reader_io_error(e, "read failed from reader"),
        }
    }

    /// \brief Seek to an offset, in bytes, in a stream, and write the new position to `*pos`.
    ///
    /// On failure, writes `0` to `*pos` and returns an `opendal_error *`.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_reader_seek(
        &mut self,
        offset: i64,
        whence: i32,
        pos: *mut u64,
    ) -> *mut opendal_error {
        self.opendal_reader_seek_with_cancel(offset, whence, std::ptr::null(), pos)
    }

    /// \brief Seek to an offset, in bytes, in a stream, supporting cancellation via `cancel`.
    ///
    /// Pass NULL for `cancel` to run without cancellation. On cancellation or failure,
    /// writes `0` to `*pos` and returns an `opendal_error *` (`OPENDAL_CANCELLED` on
    /// cancellation).
    #[no_mangle]
    pub unsafe extern "C" fn opendal_reader_seek_with_cancel(
        &mut self,
        offset: i64,
        whence: i32,
        cancel: *const opendal_cancel_token,
        pos: *mut u64,
    ) -> *mut opendal_error {
        assert!(!pos.is_null());
        std::ptr::write(pos, 0);
        let seek_from = match whence {
            OPENDAL_SEEK_SET => match u64::try_from(offset) {
                Ok(offset) => SeekFrom::Start(offset),
                Err(_) => {
                    return opendal_error::new(core::Error::new(
                        core::ErrorKind::Unexpected,
                        "negative seek offset is invalid with OPENDAL_SEEK_SET",
                    ));
                }
            },
            OPENDAL_SEEK_CUR => SeekFrom::Current(offset),
            OPENDAL_SEEK_END => SeekFrom::End(offset),
            _ => {
                return opendal_error::new(core::Error::new(
                    core::ErrorKind::Unexpected,
                    "undefined whence",
                ));
            }
        };

        let cancel_token = opendal_cancel_token::as_inner(cancel);
        let inner = self.deref_mut();
        let reader = inner.reader.as_mut().expect("reader has been dropped");
        match block_on_cancellable(&inner.handle, cancel_token, reader.seek(seek_from)) {
            Ok(p) => {
                std::ptr::write(pos, p);
                std::ptr::null_mut()
            }
            Err(CancellableError::Cancelled) => opendal_error::cancelled(),
            Err(CancellableError::Inner(e)) => reader_io_error(e, "seek failed from reader"),
        }
    }

    /// \brief Frees the heap memory used by the opendal_reader.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_reader_free(ptr: *mut opendal_reader) {
        unsafe {
            if !ptr.is_null() {
                drop(Box::from_raw((*ptr).inner as *mut ReaderInner));
                drop(Box::from_raw(ptr));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_seek_set_rejects_negative_offset() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let op = core::Operator::new(core::services::Memory::default()).unwrap();
        let reader = runtime.block_on(async {
            op.write("test", "hello").await.unwrap();
            op.reader("test")
                .await
                .unwrap()
                .into_futures_async_read(core::BytesRange::default())
                .await
                .unwrap()
        });
        let reader = Box::into_raw(Box::new(opendal_reader::new(
            runtime.handle().clone(),
            reader,
        )));

        let mut pos = 42;
        let err = unsafe {
            opendal_reader::opendal_reader_seek(&mut *reader, -1, OPENDAL_SEEK_SET, &mut pos)
        };
        assert!(!err.is_null());
        assert_eq!(pos, 0);
        unsafe {
            opendal_error::opendal_error_free(err);
            opendal_reader::opendal_reader_free(reader);
        }
    }

    #[test]
    fn test_reader_preserves_not_found_on_read_and_seek() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let op = core::Operator::new(core::services::Memory::default()).unwrap();
        let reader = runtime.block_on(async {
            op.reader("does_not_exist")
                .await
                .unwrap()
                .into_futures_async_read(core::BytesRange::default())
                .await
                .unwrap()
        });
        let reader = Box::into_raw(Box::new(opendal_reader::new(
            runtime.handle().clone(),
            reader,
        )));

        let mut buf = [0u8; 16];
        let mut size = 99;
        let err = unsafe {
            opendal_reader::opendal_reader_read(
                &mut *reader,
                buf.as_mut_ptr(),
                buf.len(),
                &mut size,
            )
        };
        assert!(!err.is_null());
        assert_eq!(unsafe { (*err).code() }, opendal_code::OPENDAL_NOT_FOUND);
        assert_eq!(size, 0);
        unsafe { opendal_error::opendal_error_free(err) };

        let mut pos = 99;
        let err = unsafe {
            opendal_reader::opendal_reader_seek(&mut *reader, 0, OPENDAL_SEEK_END, &mut pos)
        };
        assert!(!err.is_null());
        assert_eq!(unsafe { (*err).code() }, opendal_code::OPENDAL_NOT_FOUND);
        assert_eq!(pos, 0);
        unsafe {
            opendal_error::opendal_error_free(err);
            opendal_reader::opendal_reader_free(reader);
        }
    }

    #[test]
    fn test_reader_preserves_backend_error_kind() {
        use ::opendal::raw::{self, oio};
        use std::sync::Arc;

        #[derive(Debug)]
        struct FailingReadService {
            inner: raw::Servicer,
        }

        #[derive(Debug)]
        struct FailingReadLayer;

        impl raw::Layer for FailingReadLayer {
            fn apply_service(&self, inner: raw::Servicer) -> raw::Servicer {
                Arc::new(FailingReadService { inner })
            }
        }

        struct FailingReader;

        impl oio::Read for FailingReader {
            async fn open(
                &self,
                _range: core::BytesRange,
            ) -> core::Result<(raw::RpRead, Box<dyn oio::ReadStreamDyn>)> {
                Err(core::Error::new(
                    core::ErrorKind::ConditionNotMatch,
                    "etag mismatch on read",
                ))
            }

            async fn read(
                &self,
                _range: core::BytesRange,
            ) -> core::Result<(raw::RpRead, core::Buffer)> {
                Err(core::Error::new(
                    core::ErrorKind::ConditionNotMatch,
                    "etag mismatch on read",
                ))
            }
        }

        impl raw::Service for FailingReadService {
            type Reader = FailingReader;
            type Writer = oio::Writer;
            type Lister = oio::Lister;
            type Deleter = oio::Deleter;
            type Copier = oio::Copier;
            type Composer = oio::Composer;

            fn info(&self) -> raw::ServiceInfo {
                self.inner.info()
            }

            fn capability(&self) -> core::Capability {
                self.inner.capability()
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
                _ctx: &core::OperationContext,
                _path: &str,
                _args: raw::OpStat,
            ) -> core::Result<raw::RpStat> {
                Err(core::Error::new(
                    core::ErrorKind::PermissionDenied,
                    "permission denied on stat",
                ))
            }

            fn read(
                &self,
                _ctx: &core::OperationContext,
                _path: &str,
                _args: raw::OpRead,
            ) -> core::Result<Self::Reader> {
                Ok(FailingReader)
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

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let op = core::Operator::new(core::services::Memory::default())
            .unwrap()
            .layer(FailingReadLayer);
        let reader = runtime.block_on(async {
            op.reader("test")
                .await
                .unwrap()
                .into_futures_async_read(core::BytesRange::default())
                .await
                .unwrap()
        });
        let reader = Box::into_raw(Box::new(opendal_reader::new(
            runtime.handle().clone(),
            reader,
        )));

        let mut buf = [0u8; 8];
        let mut size = 99;
        let err = unsafe {
            opendal_reader::opendal_reader_read(
                &mut *reader,
                buf.as_mut_ptr(),
                buf.len(),
                &mut size,
            )
        };
        assert!(!err.is_null());
        assert_eq!(
            unsafe { (*err).code() },
            opendal_code::OPENDAL_CONDITION_NOT_MATCH
        );
        assert_eq!(size, 0);
        unsafe { opendal_error::opendal_error_free(err) };

        let mut pos = 99;
        let err = unsafe {
            opendal_reader::opendal_reader_seek(&mut *reader, 0, OPENDAL_SEEK_END, &mut pos)
        };
        assert!(!err.is_null());
        assert_eq!(
            unsafe { (*err).code() },
            opendal_code::OPENDAL_PERMISSION_DENIED
        );
        assert_eq!(pos, 0);
        unsafe {
            opendal_error::opendal_error_free(err);
            opendal_reader::opendal_reader_free(reader);
        }
    }

    #[test]
    fn test_reader_io_error_fallback() {
        unsafe {
            let err = reader_io_error(
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "bad seek"),
                "seek failed",
            );
            assert_eq!((*err).code(), opendal_code::OPENDAL_UNEXPECTED);
            opendal_error::opendal_error_free(err);

            let err = reader_io_error(
                std::io::Error::new(std::io::ErrorKind::NotFound, "missing"),
                "read failed",
            );
            assert_eq!((*err).code(), opendal_code::OPENDAL_NOT_FOUND);
            opendal_error::opendal_error_free(err);

            let err = reader_io_error(
                std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied"),
                "read failed",
            );
            assert_eq!((*err).code(), opendal_code::OPENDAL_PERMISSION_DENIED);
            opendal_error::opendal_error_free(err);
        }
    }
}
