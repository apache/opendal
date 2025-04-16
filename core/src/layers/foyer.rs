use crate::raw::*;
use crate::*;
use foyer::{Code, CodeError, HybridCache};
use std::sync::Arc;

pub struct FoyerLayer {
    cache: Arc<HybridCache<String, CacheValue>>,
}

impl FoyerLayer {
    pub async fn new(cache: HybridCache<String, CacheValue>) -> Result<Self> {
        Ok(Self {
            cache: Arc::new(cache),
        })
    }
}

impl<A: Access> Layer<A> for FoyerLayer {
    type LayeredAccess = CacheAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        CacheAccessor {
            inner,
            cache: Arc::clone(&self.cache),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CacheValue(Buffer);

impl Code for CacheValue {
    fn encode(&self, writer: &mut impl std::io::Write) -> std::result::Result<(), CodeError> {
        writer.write_vectored(&self.0.to_io_slice())?;
        Ok(())
    }

    fn decode(reader: &mut impl std::io::Read) -> std::result::Result<Self, CodeError>
    where
        Self: Sized,
    {
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).map_err(CodeError::Io)?;
        Ok(CacheValue(Buffer::from(buf)))
    }

    fn estimated_size(&self) -> usize {
        self.0.len()
    }
}

#[derive(Debug)]
pub struct CacheAccessor<A: Access> {
    inner: A,
    cache: Arc<HybridCache<String, CacheValue>>,
}

impl<A: Access> LayeredAccess for CacheAccessor<A> {
    type Inner = A;

    type Reader = TwoWays<Buffer, CacheWrapper<A::Reader>>;

    type Writer = A::Writer;

    type Lister = A::Lister;

    type Deleter = A::Deleter;

    type BlockingReader = A::BlockingReader;

    type BlockingWriter = A::BlockingWriter;

    type BlockingLister = A::BlockingLister;

    type BlockingDeleter = A::BlockingDeleter;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let cache_key = build_cache_key(path, &args);
        if let Some(entry) = self
            .cache
            .get(&cache_key)
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?
        {
            return Ok((RpRead::default(), TwoWays::One(entry.0.clone())));
        }

        self.inner.read(path, args).await.map(|(rp, reader)| {
            let reader = TwoWays::Two(CacheWrapper::new(
                reader,
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
    cache: Arc<HybridCache<String, CacheValue>>,
    cache_key: String,
    buffers: Vec<Buffer>,
}

impl<R> CacheWrapper<R> {
    fn new(inner: R, cache: Arc<HybridCache<String, CacheValue>>, cache_key: String) -> Self {
        Self {
            inner,
            cache_key,
            cache,
            buffers: Vec::new(),
        }
    }
}

impl<R: oio::Read> oio::Read for CacheWrapper<R> {
    async fn read(&mut self) -> Result<Buffer> {
        let buffer = self.inner.read().await?;

        if !buffer.is_empty() {
            self.buffers.push(buffer.clone());
            return Ok(buffer);
        }
        let flattened_buffer: Buffer = self.buffers.drain(..).flatten().collect();

        self.cache
            .insert(self.cache_key.clone(), CacheValue(flattened_buffer));

        Ok(buffer)
    }
}

fn build_cache_key(path: &str, args: &OpRead) -> String {
    let version = args.version().unwrap_or_default();
    let range = args.range();
    format!("{path}-{version}-{range}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_cache_key_with_default_args() {
        let args = OpRead::default();
        let path = "test";

        let cache_key = build_cache_key(path, &args);
        assert_eq!("test--0-", cache_key);
    }

    #[test]
    fn test_build_cache_key_with_version() {
        let args = OpRead::default().with_version("version");
        let path = "test";

        let cache_key = build_cache_key(path, &args);
        assert_eq!("test-version-0-", cache_key);
    }

    #[test]
    fn test_build_cache_key_with_range() {
        let args = OpRead::default().with_range(BytesRange::from(1024..2048));
        let path = "test";

        let cache_key = build_cache_key(path, &args);
        assert_eq!("test--1024-2047", cache_key);
    }

    #[test]
    fn test_build_cache_key_with_range_and_version() {
        let args = OpRead::default()
            .with_version("version")
            .with_range(BytesRange::from(1024..2048));
        let path = "test";

        let cache_key = build_cache_key(path, &args);
        assert_eq!("test-version-1024-2047", cache_key);
    }
}
