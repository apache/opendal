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

use crate::raw::Accessor;
use crate::raw::*;
use crate::*;
use async_trait::async_trait;
use bytes::Bytes;
use probe::probe_lazy;
use std::ffi::CString;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::io;
use std::task::Context;
use std::task::Poll;

/// Support User Statically-Defined Tracing(aka USDT) on Linux
///
/// This layer is an experimental feature, it will be enabled by `features = ["layers-dtrace"]` in Cargo.toml.
///
/// Example:
/// ```
///
/// use anyhow::Result;
/// use opendal::services::Fs;
/// use opendal::Operator;
/// use opendal::layers::DTraceLayer;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let mut builder = Fs::default();
///
///     builder.root("/tmp");
///
///     // `Accessor` provides the low level APIs, we will use `Operator` normally.
///     let op: Operator = Operator::new(builder)?.layer(DtraceLayer::default()).finish();
///     
///     let path="/tmp/test.txt";
///     for _ in 1..100000{
///         let bs = vec![0; 64 * 1024 * 1024];
///         op.write(path, bs).await?;
///         op.read(path).await?;
///     }
///     Ok(())
/// }
/// ```
///
/// Then you can use `readelf -n target/debug/examples/dtrace` to see the probes:
///
/// ```text
/// Displaying notes found in: .note.stapsdt
///   Owner                Data size        Description
///   stapsdt              0x00000039       NT_STAPSDT (SystemTap probe descriptors)
///     Provider: opendal
///     Name: create_dir_start
///     Location: 0x00000000000f8f05, Base: 0x0000000000000000, Semaphore: 0x00000000003649f8
///     Arguments: -8@%rax
///   stapsdt              0x00000037       NT_STAPSDT (SystemTap probe descriptors)
///     Provider: opendal
///     Name: create_dir_end
///     Location: 0x00000000000f9284, Base: 0x0000000000000000, Semaphore: 0x00000000003649fa
///     Arguments: -8@%rax
///   stapsdt              0x0000003c       NT_STAPSDT (SystemTap probe descriptors)
///     Provider: opendal
///     Name: blocking_list_start
///     Location: 0x00000000000f9487, Base: 0x0000000000000000, Semaphore: 0x0000000000364a28
///     Arguments: -8@%rax
///   stapsdt              0x0000003a       NT_STAPSDT (SystemTap probe descriptors)
///     Provider: opendal
///     Name: blocking_list_end
///     Location: 0x00000000000f9546, Base: 0x0000000000000000, Semaphore: 0x0000000000364a2a
///     Arguments: -8@%rax
///   stapsdt              0x0000003c       NT_STAPSDT (SystemTap probe descriptors)
/// ```
#[derive(Default, Debug, Clone)]
pub struct DtraceLayer {}

impl<A: Accessor> Layer<A> for DtraceLayer {
    type LayeredAccessor = DTraceAccessor<A>;
    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        DTraceAccessor { inner }
    }
}

#[derive(Clone)]
pub struct DTraceAccessor<A: Accessor> {
    inner: A,
}

impl<A: Accessor> Debug for DTraceAccessor<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DTraceAccessor")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<A: Accessor> LayeredAccessor for DTraceAccessor<A> {
    type Inner = A;
    type Reader = DtraceLayerWarpper<A::Reader>;
    type BlockingReader = DtraceLayerWarpper<A::BlockingReader>;
    type Writer = DtraceLayerWarpper<A::Writer>;
    type BlockingWriter = DtraceLayerWarpper<A::BlockingWriter>;
    type Lister = A::Lister;
    type BlockingLister = A::BlockingLister;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        let c_path = CString::new(path).unwrap();
        probe_lazy!(opendal, create_dir_start, c_path.as_ptr());
        let result = self.inner.create_dir(path, args).await;
        probe_lazy!(opendal, create_dir_end, c_path.as_ptr());
        result
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let c_path = CString::new(path).unwrap();
        probe_lazy!(opendal, fetch_reader_start, c_path.as_ptr());
        let result = self
            .inner
            .read(path, args)
            .await
            .map(|(rp, r)| (rp, DtraceLayerWarpper::new(r, &path.to_string())));
        probe_lazy!(opendal, fetch_reader_end, c_path.as_ptr());
        result
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let c_path = CString::new(path).unwrap();
        probe_lazy!(opendal, fetch_writer_start, c_path.as_ptr());
        let result = self
            .inner
            .write(path, args)
            .await
            .map(|(rp, r)| (rp, DtraceLayerWarpper::new(r, &path.to_string())));

        probe_lazy!(opendal, fetch_writer_end, c_path.as_ptr());
        result
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let c_path = CString::new(path).unwrap();
        probe_lazy!(opendal, stat_start, c_path.as_ptr());
        let result = self.inner.stat(path, args).await;
        probe_lazy!(opendal, stat_end, c_path.as_ptr());
        result
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        let c_path = CString::new(path).unwrap();
        probe_lazy!(opendal, delete_start, c_path.as_ptr());
        let result = self.inner.delete(path, args).await;
        probe_lazy!(opendal, delete_end, c_path.as_ptr());
        result
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let c_path = CString::new(path).unwrap();
        probe_lazy!(opendal, list_start, c_path.as_ptr());
        let result = self.inner.list(path, args).await;
        probe_lazy!(opendal, list_end, c_path.as_ptr());
        result
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        self.inner.batch(args).await
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        let c_path = CString::new(path).unwrap();
        probe_lazy!(opendal, presign_start, c_path.as_ptr());
        let result = self.inner.presign(path, args).await;
        probe_lazy!(opendal, presign_end, c_path.as_ptr());
        result
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        let c_path = CString::new(path).unwrap();
        probe_lazy!(opendal, blocking_create_dir_start, c_path.as_ptr());
        let result = self.inner.blocking_create_dir(path, args);
        probe_lazy!(opendal, blocking_create_dir_end, c_path.as_ptr());
        result
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        let c_path = CString::new(path).unwrap();
        probe_lazy!(opendal, fetch_blocking_reader_start, c_path.as_ptr());
        let result = self
            .inner
            .blocking_read(path, args)
            .map(|(rp, r)| (rp, DtraceLayerWarpper::new(r, &path.to_string())));
        probe_lazy!(opendal, fetch_blocking_reader_end, c_path.as_ptr());
        result
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        let c_path = CString::new(path).unwrap();
        probe_lazy!(opendal, fetch_blocking_writer_start, c_path.as_ptr());
        let result = self
            .inner
            .blocking_write(path, args)
            .map(|(rp, r)| (rp, DtraceLayerWarpper::new(r, &path.to_string())));
        probe_lazy!(opendal, fetch_blocking_writer_end, c_path.as_ptr());
        result
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let c_path = CString::new(path).unwrap();
        probe_lazy!(opendal, blocking_stat_start, c_path.as_ptr());
        let result = self.inner.blocking_stat(path, args);
        probe_lazy!(opendal, blocking_stat_end, c_path.as_ptr());
        result
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        let c_path = CString::new(path).unwrap();
        probe_lazy!(opendal, blocking_delete_start, c_path.as_ptr());
        let result = self.inner.blocking_delete(path, args);
        probe_lazy!(opendal, blocking_delete_end, c_path.as_ptr());
        result
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        let c_path = CString::new(path).unwrap();
        probe_lazy!(opendal, blocking_list_start, c_path.as_ptr());
        let result = self.inner.blocking_list(path, args);
        probe_lazy!(opendal, blocking_list_end, c_path.as_ptr());
        result
    }
}

pub struct DtraceLayerWarpper<R> {
    inner: R,
    path: String,
}

impl<R> DtraceLayerWarpper<R> {
    pub fn new(inner: R, path: &String) -> Self {
        Self {
            inner,
            path: path.to_string(),
        }
    }
}

impl<R: oio::Read> oio::Read for DtraceLayerWarpper<R> {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        let c_path = CString::new(self.path.clone()).unwrap();
        probe_lazy!(opendal, poll_read_start, c_path.as_ptr());
        self.inner.poll_read(cx, buf).map(|res| match res {
            Ok(bytes) => {
                probe_lazy!(opendal, poll_read_complete_success, c_path.as_ptr(), bytes);
                Ok(bytes)
            }
            Err(e) => {
                probe_lazy!(opendal, poll_read_complete_failed, c_path.as_ptr());
                Err(e)
            }
        })
    }
    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: io::SeekFrom) -> Poll<Result<u64>> {
        let c_path = CString::new(self.path.clone()).unwrap();
        probe_lazy!(opendal, poll_seek_start, c_path.as_ptr());
        self.inner.poll_seek(cx, pos).map(|res| match res {
            Ok(n) => {
                probe_lazy!(opendal, poll_seek_complete_success, c_path.as_ptr(), n);
                Ok(n)
            }
            Err(e) => {
                probe_lazy!(opendal, poll_seek_complete_failed, c_path.as_ptr());
                Err(e)
            }
        })
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        let c_path = CString::new(self.path.clone()).unwrap();
        probe_lazy!(opendal, poll_next_start, c_path.as_ptr());
        self.inner.poll_next(cx).map(|res| match res {
            Some(Ok(bytes)) => {
                probe_lazy!(
                    opendal,
                    poll_next_complete_success,
                    c_path.as_ptr(),
                    bytes.len()
                );
                Some(Ok(bytes))
            }
            Some(Err(e)) => {
                probe_lazy!(opendal, poll_next_complete_failed, c_path.as_ptr());
                Some(Err(e))
            }
            None => {
                probe_lazy!(opendal, poll_next_complete, c_path.as_ptr());
                None
            }
        })
    }
}

impl<R: oio::BlockingRead> oio::BlockingRead for DtraceLayerWarpper<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let c_path = CString::new(self.path.clone()).unwrap();
        probe_lazy!(opendal, read_start, c_path.as_ptr());
        self.inner
            .read(buf)
            .map(|n| {
                probe_lazy!(opendal, read_complete_success, c_path.as_ptr(), n);
                n
            })
            .map_err(|e| {
                probe_lazy!(opendal, read_complete_failed, c_path.as_ptr());
                e
            })
    }

    fn seek(&mut self, pos: io::SeekFrom) -> Result<u64> {
        let c_path = CString::new(self.path.clone()).unwrap();
        probe_lazy!(opendal, seek_start, c_path.as_ptr());
        self.inner
            .seek(pos)
            .map(|res| {
                probe_lazy!(opendal, seek_complete_success, c_path.as_ptr(), res);
                res
            })
            .map_err(|e| {
                probe_lazy!(opendal, seek_complete_failed, c_path.as_ptr());
                e
            })
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        let c_path = CString::new(self.path.clone()).unwrap();
        probe_lazy!(opendal, next_start, c_path.as_ptr());
        self.inner.next().map(|res| match res {
            Ok(bytes) => {
                probe_lazy!(opendal, next_complete_success, c_path.as_ptr(), bytes.len());
                Ok(bytes)
            }
            Err(e) => {
                probe_lazy!(opendal, next_complete_failed, c_path.as_ptr());
                Err(e)
            }
        })
    }
}

impl<R: oio::Write> oio::Write for DtraceLayerWarpper<R> {
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: &dyn oio::WriteBuf) -> Poll<Result<usize>> {
        let c_path = CString::new(self.path.clone()).unwrap();
        probe_lazy!(opendal, poll_write_start, c_path.as_ptr());
        self.inner
            .poll_write(cx, bs)
            .map_ok(|n| {
                probe_lazy!(opendal, poll_write_complete_success, c_path.as_ptr(), n);
                n
            })
            .map_err(|err| {
                probe_lazy!(opendal, poll_write_complete_failed, c_path.as_ptr());
                err
            })
    }

    fn poll_abort(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let c_path = CString::new(self.path.clone()).unwrap();
        probe_lazy!(opendal, poll_abort_start, c_path.as_ptr());
        self.inner
            .poll_abort(cx)
            .map_ok(|_| {
                probe_lazy!(opendal, poll_abort_complete_success, c_path.as_ptr());
            })
            .map_err(|err| {
                probe_lazy!(opendal, poll_abort_complete_failed, c_path.as_ptr());
                err
            })
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let c_path = CString::new(self.path.clone()).unwrap();
        probe_lazy!(opendal, poll_close_start, c_path.as_ptr());
        self.inner
            .poll_close(cx)
            .map_ok(|_| {
                probe_lazy!(opendal, poll_close_complete_success, c_path.as_ptr());
            })
            .map_err(|err| {
                probe_lazy!(opendal, poll_close_complete_failed, c_path.as_ptr());
                err
            })
    }
}

impl<R: oio::BlockingWrite> oio::BlockingWrite for DtraceLayerWarpper<R> {
    fn write(&mut self, bs: &dyn oio::WriteBuf) -> Result<usize> {
        let c_path = CString::new(self.path.clone()).unwrap();
        probe_lazy!(opendal, write_start, c_path.as_ptr());
        self.inner
            .write(bs)
            .map(|n| {
                probe_lazy!(opendal, write_complete_success, c_path.as_ptr(), n);
                n
            })
            .map_err(|err| {
                probe_lazy!(opendal, write_complete_failed, c_path.as_ptr());
                err
            })
    }

    fn close(&mut self) -> Result<()> {
        let c_path = CString::new(self.path.clone()).unwrap();
        probe_lazy!(opendal, close_start, c_path.as_ptr());
        self.inner
            .close()
            .map(|_| {
                probe_lazy!(opendal, close_complete_success, c_path.as_ptr());
            })
            .map_err(|err| {
                probe_lazy!(opendal, close_complete_failed, c_path.as_ptr());
                err
            })
    }
}
