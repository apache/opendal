use std::sync::Arc;

use crate::raw::*;
use crate::*;
use bytes::Bytes;
use foyer::{DirectFsDeviceOptions, Engine, HybridCache, HybridCacheBuilder};
use serde::{Deserialize, Serialize};

pub struct CacheLayer {
    cache: Arc<HybridCache<CacheKey, CacheValue>>,
}

// TODO: rename to foyer layer and receive a HybridCacheBuilder as the parameter?
// TODO: expose foyer metrics here?
impl CacheLayer {
    pub async fn new(
        disk_cache_dir: &str,
        disk_capacity_mb: usize,
        memory_capacity_mb: usize,
    ) -> Result<Self> {
        const MB: usize = 1 << 20;

        let cache = HybridCacheBuilder::new()
            .with_name("opendal")
            .memory(memory_capacity_mb * MB)
            .storage(Engine::Large)
            .with_device_options(
                DirectFsDeviceOptions::new(disk_cache_dir).with_capacity(disk_capacity_mb * MB),
            )
            .build()
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;

        Ok(Self {
            cache: Arc::new(cache),
        })
    }
}

impl<A: Access> Layer<A> for CacheLayer {
    type LayeredAccess = CacheAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        CacheAccessor {
            inner,
            cache: Arc::clone(&self.cache),
        }
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
struct CacheKey {
    path: String,
    args: OpRead,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheValue {
    rp: RpRead,
    // TODO: store Buffer or Bytes?
    bytes: Bytes,
}

#[derive(Debug)]
pub struct CacheAccessor<A: Access> {
    inner: A,
    // TODO: if cache should be used for other operations (such as stat or list)
    // maybe we should create different caches for each operation?
    // So the keys and values does not mix
    // Although, we could use an enum as the cachekey and as the cache value, but
    // this way we wouldn't be making invalid states unrepresentable, as given
    // a Read Cache key, there might be a List cached value associated.
    cache: Arc<HybridCache<CacheKey, CacheValue>>,
}

impl<A: Access> LayeredAccess for CacheAccessor<A> {
    type Inner = A;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    // TODO: add a comment here that we use `oio::Reader` (i.e Box<dyn ReadDyn>) because
    // if there is a cache hit, we return Box<Buffer>
    // but if there isn't, we return a Box<CacheWrapper<..>> so when the reader
    // is read, the value is inserted into the cache.
    // This allow to lazy reading and inserting in cache not to be done in the
    // `CacheAccessor::read` method, but when calling `reader.read()` on the reader
    // output of `CacheAccessor`
    type Reader = oio::Reader;

    type Writer = A::Writer;

    // TODO: lister cache?
    type Lister = A::Lister;

    type Deleter = A::Deleter;

    type BlockingReader = A::BlockingReader;

    type BlockingWriter = A::BlockingWriter;

    type BlockingLister = A::BlockingLister;

    type BlockingDeleter = A::BlockingDeleter;

    // TODO: stat cache?

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let cache_key = CacheKey {
            path: path.to_string(),
            args: args.clone(),
        };
        if let Some(entry) = self
            .cache
            .get(&cache_key)
            .await
            // TODO: handle this error
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?
        {
            // TODO: log that we have a cache hit?
            return Ok((entry.rp, Box::new(entry.bytes.clone())));
        }

        self.inner.read(path, args).await.map(|(rp, reader)| {
            let reader: oio::Reader = Box::new(CacheWrapper::new(
                reader,
                rp,
                Arc::clone(&self.cache),
                cache_key,
            ));
            (rp, reader)
        })
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.inner.write(path, args).await
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        self.inner.delete().await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.inner.list(path, args).await
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.inner.blocking_read(path, args)
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.inner.blocking_write(path, args)
    }

    fn blocking_delete(&self) -> Result<(RpDelete, Self::BlockingDeleter)> {
        self.inner.blocking_delete()
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        self.inner.blocking_list(path, args)
    }
}

pub struct CacheWrapper<R> {
    inner: R,
    rp: RpRead,
    cache: Arc<HybridCache<CacheKey, CacheValue>>,
    cache_key: CacheKey,
    buffers: Vec<Buffer>,
}

impl<R> CacheWrapper<R> {
    fn new(
        inner: R,
        rp: RpRead,
        cache: Arc<HybridCache<CacheKey, CacheValue>>,
        cache_key: CacheKey,
    ) -> Self {
        Self {
            inner,
            rp,
            cache_key,
            cache,
            buffers: Vec::new(),
        }
    }
}

impl<R: oio::Read> oio::Read for CacheWrapper<R> {
    async fn read(&mut self) -> Result<Buffer> {
        // TODO: ask if we should accumulate the bytes like this
        // or use self.inner.read_all() instead
        let buffer = self.inner.read().await?;

        if !buffer.is_empty() {
            self.buffers.push(buffer.clone());
            return Ok(buffer);
        }
        // TODO: comment that we only insert in cache when we read all the bytes and
        // `self.inner.read()` returns empty bytes
        let flattened_buffer: Buffer = self.buffers.drain(..).flatten().collect();

        let cache_value = CacheValue {
            rp: self.rp,
            bytes: flattened_buffer.to_bytes(),
        };
        // TODO: clone or store Option<CacheKey> and use here `self.cache_key.take()`?
        self.cache.insert(self.cache_key.clone(), cache_value);

        Ok(buffer)
    }
}
