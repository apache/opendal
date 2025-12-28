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
    ops::{Bound, Deref, Range, RangeBounds},
    sync::Arc,
};

use foyer::{Code, CodeError, Error as FoyerError, HybridCache};

use opendal_core::raw::oio::*;
use opendal_core::raw::*;
use opendal_core::*;

fn extract_err(e: FoyerError) -> Error {
    let e = match e.downcast::<Error>() {
        Ok(e) => return e,
        Err(e) => e,
    };
    Error::new(ErrorKind::Unexpected, e.to_string())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FoyerKey {
    pub path: String,
    pub version: Option<String>,
}

impl Code for FoyerKey {
    fn encode(&self, writer: &mut impl std::io::Write) -> std::result::Result<(), CodeError> {
        let path_len = self.path.len() as u64;
        writer.write_all(&path_len.to_le_bytes())?;
        writer.write_all(self.path.as_bytes())?;
        if let Some(ref version) = self.version {
            let version_len = version.len() as u64;
            writer.write_all(&version_len.to_le_bytes())?;
            writer.write_all(version.as_bytes())?;
        } else {
            writer.write_all(&0u64.to_le_bytes())?;
        }
        Ok(())
    }

    fn decode(reader: &mut impl std::io::Read) -> std::result::Result<Self, CodeError>
    where
        Self: Sized,
    {
        let mut u64_buf = [0u8; 8];
        reader.read_exact(&mut u64_buf)?;
        let path_len = u64::from_le_bytes(u64_buf) as usize;
        let mut path_buf = vec![0u8; path_len];
        reader.read_exact(&mut path_buf[..path_len])?;
        let path = String::from_utf8(path_buf).map_err(|e| CodeError::Other(Box::new(e)))?;

        reader.read_exact(&mut u64_buf)?;
        let version_len = u64::from_le_bytes(u64_buf) as usize;
        let version = if version_len > 0 {
            let mut version_buf = vec![0u8; path_len];
            reader.read_exact(&mut version_buf[..path_len])?;
            let version =
                String::from_utf8(version_buf).map_err(|e| CodeError::Other(Box::new(e)))?;
            Some(version)
        } else {
            None
        };
        Ok(FoyerKey { path, version })
    }

    fn estimated_size(&self) -> usize {
        // 8B length prefix + path length
        8 + self.path.len()
        // 8B version length prefix + version length, if present
        + 8 + self.version.as_ref().map_or(0, |v| v.len())
    }
}

/// [`FoyerValue`] is a wrapper around `Buffer` that implements the `Code` trait.
#[derive(Debug)]
pub struct FoyerValue(pub Buffer);

impl Deref for FoyerValue {
    type Target = Buffer;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Code for FoyerValue {
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
        Ok(FoyerValue(buffer.into()))
    }

    fn estimated_size(&self) -> usize {
        // 8B length prefix + buffer length
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
///
/// # Note
///
/// If the object version is enabled, the foyer cache layer will treat the objects with same key but different versions as different objects.
#[derive(Debug)]
pub struct FoyerLayer {
    cache: HybridCache<FoyerKey, FoyerValue>,
    size_limit: Range<usize>,
}

impl FoyerLayer {
    /// Creates a new `FoyerLayer` with the given foyer hybrid cache.
    pub fn new(cache: HybridCache<FoyerKey, FoyerValue>) -> Self {
        FoyerLayer {
            cache,
            size_limit: 0..usize::MAX,
        }
    }

    /// Sets the size limit for caching.
    ///
    /// It is recommended to set a size limit to avoid caching large files that may not be suitable for caching.
    pub fn with_size_limit<R: RangeBounds<usize>>(mut self, size_limit: R) -> Self {
        let start = match size_limit.start_bound() {
            Bound::Included(v) => *v,
            Bound::Excluded(v) => *v + 1,
            Bound::Unbounded => 0,
        };
        let end = match size_limit.end_bound() {
            Bound::Included(v) => *v + 1,
            Bound::Excluded(v) => *v,
            Bound::Unbounded => usize::MAX,
        };
        self.size_limit = start..end;
        self
    }
}

impl<A: Access> Layer<A> for FoyerLayer {
    type LayeredAccess = FoyerAccessor<A>;

    fn layer(&self, accessor: A) -> Self::LayeredAccess {
        let cache = self.cache.clone();
        FoyerAccessor {
            inner: Arc::new(Inner {
                accessor,
                cache,
                size_limit: self.size_limit.clone(),
            }),
        }
    }
}

#[derive(Debug)]
struct Inner<A: Access> {
    accessor: A,
    cache: HybridCache<FoyerKey, FoyerValue>,
    size_limit: Range<usize>,
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

    fn info(&self) -> Arc<AccessorInfo> {
        self.inner.accessor.info()
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let path = path.to_string();
        let version = args.version().map(|v| v.to_string());

        // Extract range bounds before async block to avoid lifetime issues
        let (range_start, range_end) = {
            let r = args.range();
            let start = r.offset();
            let end = r.size().map(|size| start + size);
            (start, end)
        };

        let entry = self
            .inner
            .cache
            .fetch(
                FoyerKey {
                    path: path.clone(),
                    version,
                },
                || {
                    let inner = self.inner.clone();
                    async move {
                        let (_, mut reader) = inner
                            .accessor
                            .read(&path, args.with_range(BytesRange::new(0, None)))
                            .await
                            .map_err(FoyerError::other)?;
                        let buffer = reader.read_all().await.map_err(FoyerError::other)?;
                        Ok(FoyerValue(buffer))
                    }
                },
            )
            .await
            .map_err(extract_err)?;

        let end = range_end.unwrap_or(entry.len() as u64);
        let range = BytesContentRange::default()
            .with_range(range_start, end - 1)
            .with_size(entry.len() as _);
        let buffer = entry.slice(range_start as usize..end as usize);
        let rp = RpRead::new()
            .with_size(Some(buffer.len() as _))
            .with_range(Some(range));
        Ok((rp, buffer))
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
                    buf: QueueBuf::new(),
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
                    deleter: d,
                    keys: vec![],
                    inner,
                },
            ))
        }
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.inner.accessor.list(path, args).await
    }

    // TODO(MrCroxx): Implement copy, rename with foyer cache.
}

pub struct Writer<A: Access> {
    w: A::Writer,
    buf: QueueBuf,
    path: String,
    inner: Arc<Inner<A>>,
}

impl<A: Access> oio::Write for Writer<A> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        self.buf.push(bs.clone());
        self.w.write(bs).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        let buffer = self.buf.clone().collect();
        let res = self.w.close().await;
        if let Ok(metadata) = &res {
            if self.inner.size_limit.contains(&buffer.len()) {
                self.inner.cache.insert(
                    FoyerKey {
                        path: self.path.clone(),
                        version: metadata.version().map(|v| v.to_string()),
                    },
                    FoyerValue(buffer),
                );
            }
        }
        res
    }

    async fn abort(&mut self) -> Result<()> {
        self.w.abort().await
    }
}

pub struct Deleter<A: Access> {
    deleter: A::Deleter,
    keys: Vec<FoyerKey>,
    inner: Arc<Inner<A>>,
}

impl<A: Access> oio::Delete for Deleter<A> {
    async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        self.deleter.delete(path, args.clone()).await?;
        self.keys.push(FoyerKey {
            path: path.to_string(),
            version: args.version().map(|v| v.to_string()),
        });
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        for key in &self.keys {
            self.inner.cache.remove(key);
        }
        self.deleter.close().await
    }
}

#[cfg(test)]
mod tests {
    use foyer::{
        DirectFsDeviceOptions, Engine, HybridCacheBuilder, LargeEngineOptions, RecoverMode,
    };
    use size::consts::MiB;

    use opendal_core::{services::Dashmap, Operator};

    use super::*;

    fn key(i: u8) -> String {
        format!("obj-{i}")
    }

    fn value(i: u8) -> Vec<u8> {
        // ~ 64KiB with metadata
        vec![i; 63 * 1024]
    }

    #[tokio::test]
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
