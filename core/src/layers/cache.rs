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
    // TODO: add a comment that i had to make this function async due to the HybridCacheBuilder::build method,
    // the alternative is to do it in the Layer::layer method, but that method is sync and couldn't make it to work.
    // Maybe we can use `futures::executor::block_on` in this `new` method so it is no async and users in
    // sync contexts doesn't have to create a new runtime to call this.
    pub fn new() -> Result<Self> {
        // TODO: Use `Handle` or create a new tokio `Runtime`? If we use `Handle` as
        // `BlockingLayer`, we force users to create a new runtime (in blocking contexts)
        // before creating this layer
        // let handle = Handle::try_current()
        //     .map_err(|_| Error::new(ErrorKind::Unexpected, "failed to get current handle"))?;
        let cache_builder = HybridCacheBuilder::new()
            .with_name("foyer")
            .memory(1024)
            .storage(Engine::Large)
            .with_device_options(DirectFsDeviceOptions::new("/tmp/foyer"));

        // TODO: should we mark this method as `async fn` and not use `futures::executor::block_on`?
        // Using tokio::runtime::Handle::current().block_on(..) is not an option since it panics and couldn't make it work
        let cache = futures::executor::block_on(async {
            cache_builder.build().await
            // TODO: improve panic error
        })
        .expect("Failed to build cache");

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
    // Should we combile all non-contiguos bytes of Buffer into a Bytes
    // with `Buffer::to_bytes()`?
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

    type BlockingReader = oio::BlockingReader;

    type BlockingWriter = A::BlockingWriter;

    // TODO lister cache?
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
            .expect("cache failed")
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
        let cache_key = CacheKey {
            path: path.to_string(),
            args: args.clone(),
        };

        // TODO: Using Handle::current().block_on panics with
        // ERROR: Cannot start a runtime from within a runtime. This happens because a function (like `block_on`)
        // attempted to block the current thread while the thread is being used to drive asynchronous tasks.
        // The workaround I found is to use `futures::executor::block_on`
        if let Some(entry) = futures::executor::block_on(async {
            self.cache.get(&cache_key).await.expect("cache failed")
        }) {
            return Ok((entry.rp, Box::new(entry.bytes.clone())));
        }

        self.inner.blocking_read(path, args).map(|(rp, reader)| {
            let reader: oio::BlockingReader = Box::new(CacheWrapper::new(
                reader,
                rp,
                Arc::clone(&self.cache),
                cache_key,
            ));
            (rp, reader)
        })
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

impl<R: oio::BlockingRead> oio::BlockingRead for CacheWrapper<R> {
    fn read(&mut self) -> Result<Buffer> {
        let buffer = self.inner.read()?;

        if !buffer.is_empty() {
            self.buffers.push(buffer.clone());
            return Ok(buffer);
        }

        let flattened_buffer: Buffer = self.buffers.drain(..).flatten().collect();

        let cache_value = CacheValue {
            rp: self.rp,
            bytes: flattened_buffer.to_bytes(),
        };
        self.cache.insert(self.cache_key.clone(), cache_value);

        Ok(buffer)
    }
}
