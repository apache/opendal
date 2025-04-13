use std::{ops::Deref, sync::Arc};

use crate::raw::*;
use crate::*;
use foyer::{Code, CodeError, HybridCache, HybridCacheBuilderPhaseStorage};
use foyer_common::code::HashBuilder;
use serde::{Deserialize, Serialize};

pub struct FoyerLayer<S>
where
    S: HashBuilder + core::fmt::Debug,
{
    cache: Arc<HybridCache<CacheKey, Buffer, S>>,
}

// TODO: re-export foyer crate to have access to the cache builder
impl<S> FoyerLayer<S>
where
    S: HashBuilder + core::fmt::Debug,
{
    pub async fn new(builder: HybridCacheBuilderPhaseStorage<CacheKey, Buffer, S>) -> Result<Self> {
        let cache = builder
            .build()
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;

        Ok(Self {
            cache: Arc::new(cache),
        })
    }
}

impl<A, S> Layer<A> for FoyerLayer<S>
where
    A: Access,
    S: HashBuilder + core::fmt::Debug,
{
    type LayeredAccess = CacheAccessor<A, S>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        CacheAccessor {
            inner,
            cache: Arc::clone(&self.cache),
        }
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct CacheKey {
    path: String,
    args: OpRead,
}

#[derive(Debug)]
pub struct CacheAccessor<A, S>
where
    A: Access,
    S: HashBuilder + core::fmt::Debug,
{
    inner: A,
    cache: Arc<HybridCache<CacheKey, Buffer, S>>,
}

impl Code for Buffer {
    fn encode(&self, writer: &mut impl std::io::Write) -> std::result::Result<(), CodeError> {
        writer
            .write_vectored(&self.to_io_slice())
            .map_err(CodeError::Io)?;
        Ok(())
    }

    fn decode(reader: &mut impl std::io::Read) -> std::result::Result<Self, CodeError>
    where
        Self: Sized,
    {
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).map_err(CodeError::Io)?;
        Ok(Buffer::from(buf))
    }

    fn estimated_size(&self) -> usize {
        self.len()
    }
}

impl<A, S> LayeredAccess for CacheAccessor<A, S>
where
    A: Access,
    S: HashBuilder + core::fmt::Debug,
{
    type Inner = A;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    type Reader = oio::Reader;

    type Writer = A::Writer;

    type Lister = A::Lister;

    type Deleter = A::Deleter;

    type BlockingReader = A::BlockingReader;

    type BlockingWriter = A::BlockingWriter;

    type BlockingLister = A::BlockingLister;

    type BlockingDeleter = A::BlockingDeleter;

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let cache_key = CacheKey {
            path: path.to_string(),
            args: args.clone(),
        };
        if let Some(entry) = self
            .cache
            .get(&cache_key)
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?
        {
            return Ok((RpRead::default(), Box::new(entry.deref().clone())));
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

pub struct CacheWrapper<R, S>
where
    S: HashBuilder + core::fmt::Debug,
{
    inner: R,
    rp: RpRead,
    cache: Arc<HybridCache<CacheKey, Buffer, S>>,
    cache_key: CacheKey,
    buffers: Vec<Buffer>,
}

impl<R, S> CacheWrapper<R, S>
where
    S: HashBuilder + core::fmt::Debug,
{
    fn new(
        inner: R,
        rp: RpRead,
        cache: Arc<HybridCache<CacheKey, Buffer, S>>,
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

impl<R, S> oio::Read for CacheWrapper<R, S>
where
    R: oio::Read,
    S: HashBuilder + core::fmt::Debug,
{
    async fn read(&mut self) -> Result<Buffer> {
        let buffer = self.inner.read().await?;

        if !buffer.is_empty() {
            self.buffers.push(buffer.clone());
            return Ok(buffer);
        }
        let flattened_buffer: Buffer = self.buffers.drain(..).flatten().collect();

        self.cache.insert(self.cache_key.clone(), flattened_buffer);

        Ok(buffer)
    }
}
