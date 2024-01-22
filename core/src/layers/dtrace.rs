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

use async_trait::async_trait;
use std::fmt::Debug;
use std::fmt::Formatter;

use crate::raw::*;
use crate::*;
use std::ffi:: CString;

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
///     let op: Operator = Operator::new(builder)?.layer(DTraceLayer{}).finish();
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
pub struct DTraceLayer {}

impl<A: Accessor> Layer<A> for DTraceLayer {
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
    type Reader = A::Reader;
    type BlockingReader = A::BlockingReader;
    type Writer = A::Writer;
    type BlockingWriter = A::BlockingWriter;
    type Lister = A::Lister;
    type BlockingLister = A::BlockingLister;
    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        let c_path = CString::new(path).unwrap();
        probe::probe_lazy!(opendal, create_dir_start, c_path.as_ptr());
        let create_res = self.inner.create_dir(path, args).await;
        probe::probe_lazy!(opendal, create_dir_end, c_path.as_ptr());
        create_res
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let c_path = CString::new(path).unwrap();
        probe::probe_lazy!(opendal, read_start, c_path.as_ptr());
        let read_res = self.inner.read(path, args).await;
        probe::probe_lazy!(opendal, read_end, c_path.as_ptr());
        read_res
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let c_path = CString::new(path).unwrap();
        probe::probe_lazy!(opendal, write_start, c_path.as_ptr());
        let write_res = self.inner.write(path, args).await;
        probe::probe_lazy!(opendal, write_end, c_path.as_ptr());
        write_res
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let c_path = CString::new(path).unwrap();
        probe::probe_lazy!(opendal, stat_start, c_path.as_ptr());
        let stat_res = self.inner.stat(path, args).await;
        probe::probe_lazy!(opendal, stat_end, c_path.as_ptr());
        stat_res
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        let c_path = CString::new(path).unwrap();
        probe::probe_lazy!(opendal, delete_start, c_path.as_ptr());
        let delete_res = self.inner.delete(path, args).await;
        probe::probe_lazy!(opendal, delete_end, c_path.as_ptr());
        delete_res
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let c_path = CString::new(path).unwrap();
        probe::probe_lazy!(opendal, list_start, c_path.as_ptr());
        let list_res = self.inner.list(path, args).await;
        probe::probe_lazy!(opendal, list_end, c_path.as_ptr());
        list_res
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        self.inner.batch(args).await
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        let c_path = CString::new(path).unwrap();
        probe::probe_lazy!(opendal, presign_start, c_path.as_ptr());
        let result = self.inner.presign(path, args).await;
        probe::probe_lazy!(opendal, presign_end, c_path.as_ptr());
        result
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        let c_path = CString::new(path).unwrap();
        probe::probe_lazy!(opendal, blocking_create_dir_start, c_path.as_ptr());
        let result = self.inner.blocking_create_dir(path, args);
        probe::probe_lazy!(opendal, blocking_create_dir_end, c_path.as_ptr());
        result
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        let c_path = CString::new(path).unwrap();
        probe::probe_lazy!(opendal, blocking_read_start, c_path.as_ptr());
        let result = self.inner.blocking_read(path, args);
        probe::probe_lazy!(opendal, blocking_read_end, c_path.as_ptr());
        result
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        let c_path = CString::new(path).unwrap();
        probe::probe_lazy!(opendal, blocking_write_start, c_path.as_ptr());
        let result = self.inner.blocking_write(path, args);
        probe::probe_lazy!(opendal, blocking_write_end, c_path.as_ptr());
        result
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let c_path = CString::new(path).unwrap();
        probe::probe_lazy!(opendal, blocking_stat_start, c_path.as_ptr());
        let result = self.inner.blocking_stat(path, args);
        probe::probe_lazy!(opendal, blocking_stat_end, c_path.as_ptr());
        result
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        let c_path = CString::new(path).unwrap();
        probe::probe_lazy!(opendal, blocking_delete_start, c_path.as_ptr());
        let result = self.inner.blocking_delete(path, args);
        probe::probe_lazy!(opendal, blocking_delete_end, c_path.as_ptr());
        result
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        let c_path = CString::new(path).unwrap();
        probe::probe_lazy!(opendal, blocking_list_start, c_path.as_ptr());
        let result = self.inner.blocking_list(path, args);
        probe::probe_lazy!(opendal, blocking_list_end, c_path.as_ptr());
        result
    }
}
