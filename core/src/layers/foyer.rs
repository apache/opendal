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

use std::{
    future::Future,
    ops::{Bound, Deref, RangeBounds},
    sync::Arc,
};

use foyer::{Code, CodeError, Error as FoyerError, HybridCache};

use crate::{
    raw::{
        oio::{self, QueueBuf, Read as _},
        Access, AccessorInfo, BytesContentRange, BytesRange, Layer, LayeredAccess, MaybeSend,
        OpCopy, OpCreateDir, OpDelete, OpList, OpPresign, OpRead, OpRename, OpStat, OpWrite,
        RpCopy, RpCreateDir, RpDelete, RpList, RpPresign, RpRead, RpRename, RpStat, RpWrite,
    },
    Buffer, Error, ErrorKind, Metadata, Result,
};

fn extract_err(e: FoyerError) -> Error {
    let e = match e.downcast::<Error>() {
        Ok(e) => return e,
        Err(e) => e,
    };
    Error::new(ErrorKind::Unexpected, e.to_string())
}

#[derive(Debug)]
pub struct Value(Buffer);

impl Deref for Value {
    type Target = Buffer;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Buffer> for Value {
    fn from(buf: Buffer) -> Self {
        Value(buf)
    }
}

impl Code for Value {
    fn encode(&self, writer: &mut impl std::io::Write) -> std::result::Result<(), CodeError> {
        let len = self.0.len() as u64;
        writer.write_all(&len.to_le_bytes())?;
        std::io::copy(&mut self.0.clone(), writer)?;
        Ok(())
    }

    fn decode(reader: &mut impl std::io::Read) -> std::result::Result<Self, CodeError>
    where
        Self: Sized,
    {
        let mut len_bytes = [0u8; 8];
        reader.read_exact(&mut len_bytes)?;
        let len = u64::from_le_bytes(len_bytes) as usize;
        let mut buffer = vec![0u8; len];
        reader.read_exact(&mut buffer[..len])?;
        Ok(Value(buffer.into()))
    }

    fn estimated_size(&self) -> usize {
        8 + self.0.len()
    }
}

/// Hybrid cache layer for OpenDAL that uses [foyer](https://github.com/foyer-rs/foyer) for caching.
///
/// # Operation Behavior
/// - `write`: [`FoyerLayer`] will write to the foyer hybrid cache after the service's write operation is completed.
/// - `read`: [`FoyerLayer`] will first check the foyer hybrid cache for the data. If the data is not found, it will perform the read operation on the service and cache the result.
/// - `delete`: [`FoyerLayer`] will remove the data from the foyer hybrid cache regardless of whether the service's delete operation is successful.
/// - Other operations: [`FoyerLayer`] will not cache the results of other operations, such as `list`, `copy`, `rename`, etc. They will be passed through to the underlying accessor without caching.
///
/// # Examples
///
/// ```rust
/// use opendal::layers::FoyerLayer;
/// use opendal::services::S3;
///
/// ```
#[derive(Debug)]
pub struct FoyerLayer {
    cache: HybridCache<String, Value>,
}

impl FoyerLayer {
    /// Creates a new `FoyerLayer` with the given foyer hybrid cache.
    pub fn new(cache: HybridCache<String, Value>) -> Self {
        FoyerLayer { cache }
    }
}

impl From<HybridCache<String, Value>> for FoyerLayer {
    fn from(cache: HybridCache<String, Value>) -> Self {
        Self::new(cache)
    }
}

impl<A: Access> Layer<A> for FoyerLayer {
    type LayeredAccess = FoyerAccessor<A>;

    fn layer(&self, accessor: A) -> Self::LayeredAccess {
        let cache = self.cache.clone();
        FoyerAccessor {
            inner: Arc::new(Inner { accessor, cache }),
        }
    }
}

#[derive(Debug)]
struct Inner<A: Access> {
    accessor: A,
    cache: HybridCache<String, Value>,
}

#[derive(Debug)]
pub struct FoyerAccessor<A: Access> {
    inner: Arc<Inner<A>>,
}

impl<A: Access> LayeredAccess for FoyerAccessor<A> {
    type Inner = A;
    type Reader = Buffer;
    type Writer = Writer<A>;
    type Lister = A::Lister;
    type Deleter = Deleter<A>;

    fn inner(&self) -> &Self::Inner {
        &self.inner.accessor
    }

    fn read(
        &self,
        path: &str,
        args: OpRead,
    ) -> impl Future<Output = Result<(RpRead, Self::Reader)>> + MaybeSend {
        let r = args.range();
        let path = path.to_string();
        async move {
            let entry = self
                .inner
                .cache
                .fetch(path.clone(), || {
                    let inner = self.inner.clone();
                    async move {
                        let (_, mut reader) = inner
                            .accessor
                            .read(&path, args.with_range(BytesRange::new(0, None)))
                            .await
                            .map_err(FoyerError::other)?;
                        let buffer = reader.read_all().await.map_err(FoyerError::other)?;
                        Ok(buffer.into())
                    }
                })
                .await
                .map_err(extract_err)?;

            let r = r.to_range();
            let start = match r.start_bound() {
                Bound::Included(i) => *i,
                Bound::Excluded(i) => *i + 1,
                Bound::Unbounded => 0,
            };
            let end = match r.end_bound() {
                Bound::Included(i) => *i + 1,
                Bound::Excluded(i) => *i,
                Bound::Unbounded => entry.len() as u64,
            };
            let range = BytesContentRange::default()
                .with_range(start, end - 1)
                .with_size(entry.len() as _);
            let buffer = entry.slice(start as usize..end as usize);
            let rp = RpRead::new()
                .with_size(Some(buffer.len() as _))
                .with_range(Some(range));
            Ok((rp, buffer))
        }
    }

    fn write(
        &self,
        path: &str,
        args: OpWrite,
    ) -> impl Future<Output = Result<(RpWrite, Self::Writer)>> + MaybeSend {
        let inner = self.inner.clone();
        async move {
            let (rp, w) = self.inner.accessor.write(path, args).await?;
            Ok((
                rp,
                Writer {
                    w,
                    q: Some(QueueBuf::new()),
                    path: path.to_string(),
                    inner,
                },
            ))
        }
    }

    fn delete(&self) -> impl Future<Output = Result<(RpDelete, Self::Deleter)>> + MaybeSend {
        let inner = self.inner.clone();
        async move {
            let (rp, d) = inner.accessor.delete().await?;
            Ok((
                rp,
                Deleter {
                    d,
                    keys: vec![],
                    inner,
                },
            ))
        }
    }

    fn list(
        &self,
        path: &str,
        args: OpList,
    ) -> impl Future<Output = Result<(RpList, Self::Lister)>> + MaybeSend {
        self.inner.accessor.list(path, args)
    }

    fn info(&self) -> std::sync::Arc<AccessorInfo> {
        self.inner.accessor.info()
    }

    fn create_dir(
        &self,
        path: &str,
        args: OpCreateDir,
    ) -> impl Future<Output = Result<RpCreateDir>> + MaybeSend {
        self.inner.accessor.create_dir(path, args)
    }

    fn copy(
        &self,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> impl Future<Output = Result<RpCopy>> + MaybeSend {
        // TODO(MrCroxx): Implement copy with foyer cache.
        self.inner.accessor.copy(from, to, args)
    }

    fn rename(
        &self,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> impl Future<Output = Result<RpRename>> + MaybeSend {
        // TODO(MrCroxx): Implement copy with foyer cache.
        self.inner.accessor.rename(from, to, args)
    }

    fn stat(&self, path: &str, args: OpStat) -> impl Future<Output = Result<RpStat>> + MaybeSend {
        // TODO(MrCroxx): Implement copy with foyer cache.
        self.inner.accessor.stat(path, args)
    }

    fn presign(
        &self,
        path: &str,
        args: OpPresign,
    ) -> impl Future<Output = Result<RpPresign>> + MaybeSend {
        self.inner.accessor.presign(path, args)
    }
}

pub struct Writer<A: Access> {
    w: A::Writer,
    q: Option<QueueBuf>,
    path: String,
    inner: Arc<Inner<A>>,
}

impl<A: Access> oio::Write for Writer<A> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        self.q.as_mut().unwrap().push(bs.clone());
        self.w.write(bs).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        let buffer = self.q.take().unwrap().collect();
        let res = self.w.close().await;
        self.inner.cache.insert(self.path.clone(), buffer.into());
        res
    }

    async fn abort(&mut self) -> Result<()> {
        self.w.abort().await
    }
}

pub struct Deleter<A: Access> {
    d: A::Deleter,
    keys: Vec<String>,
    inner: Arc<Inner<A>>,
}

impl<A: Access> oio::Delete for Deleter<A> {
    fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        self.d.delete(path, args.clone())?;
        self.keys.push(path.to_string());
        Ok(())
    }

    async fn flush(&mut self) -> Result<usize> {
        for key in &self.keys {
            self.inner.cache.remove(key);
        }
        let res = self.d.flush().await;
        res
    }
}

#[cfg(test)]
mod tests {

    use foyer::{
        DirectFsDeviceOptions, Engine, HybridCacheBuilder, LargeEngineOptions, RecoverMode,
    };
    use size::consts::MiB;

    use crate::{services::Dashmap, Operator};

    use super::*;

    fn key(i: u8) -> String {
        format!("obj-{i}")
    }

    fn value(i: u8) -> Vec<u8> {
        // ~ 64KiB with metadata
        vec![i; 63 * 1024]
    }

    #[test_log::test(tokio::test)]
    async fn test() {
        let dir = tempfile::tempdir().unwrap();

        let cache = HybridCacheBuilder::new()
            .memory(10)
            .with_shards(1)
            .storage(Engine::Large(LargeEngineOptions::new()))
            .with_device_options(
                DirectFsDeviceOptions::new(dir.path())
                    .with_capacity(16 * MiB as usize)
                    .with_file_size(MiB as usize),
            )
            .with_recover_mode(RecoverMode::None)
            .build()
            .await
            .unwrap();

        let op = Operator::new(Dashmap::default())
            .unwrap()
            .layer(FoyerLayer::new(cache.clone()))
            .finish();

        assert!(op.list("/").await.unwrap().is_empty());

        for i in 0..64 {
            op.write(&key(i), value(i)).await.unwrap();
        }

        assert_eq!(op.list("/").await.unwrap().len(), 64);

        for i in 0..64 {
            let buf = op.read(&key(i)).await.unwrap();
            assert_eq!(buf.to_vec(), value(i));
        }

        cache.clear().await.unwrap();

        for i in 0..64 {
            let buf = op.read(&key(i)).await.unwrap();
            assert_eq!(buf.to_vec(), value(i));
        }

        for i in 0..64 {
            op.delete(&key(i)).await.unwrap();
        }

        assert!(op.list("/").await.unwrap().is_empty());

        for i in 0..64 {
            let res = op.read(&key(i)).await;
            let e = res.unwrap_err();
            assert_eq!(e.kind(), ErrorKind::NotFound);
        }
    }

    #[test]
    fn test_error() {
        let e = Error::new(ErrorKind::NotFound, "not found");
        let fe = FoyerError::other(e);
        let oe = extract_err(fe);
        assert_eq!(oe.kind(), ErrorKind::NotFound);
    }
}
