use crate::raw::*;
use crate::{Error, ErrorKind, Result};

/// SanityCheckLayer adds a simple sanity check for list operations.
/// It ensures that the paths returned by underlying storage start with
/// the requested prefix. If an unexpected path is returned, it will return
/// an Unexpected error.
///
/// This layer can detect misbehaving services that return responses for
/// unrelated keys (see issue #6646).
#[derive(Default)]
pub struct SanityCheckLayer;

impl<A: Access> Layer<A> for SanityCheckLayer {
    type LayeredAccess = SanityCheckAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        SanityCheckAccessor {
            inner,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SanityCheckAccessor<A: Access> {
    inner: A,
}

impl<A: Access> LayeredAccess for SanityCheckAccessor<A> {
    type Inner = A;
    type Reader = A::Reader;
    type Writer = A::Writer;
    type Lister = SanityCheckLister<A::Lister>;
    type Deleter = A::Deleter;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.inner.read(path, args).await
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.inner.write(path, args).await
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        self.inner.delete().await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let (rp, lister) = self.inner.list(path, args).await?;
        let prefix = path.to_string();
        let checker = SanityCheckLister {
            inner: lister,
            prefix,
        };
        Ok((rp, checker))
    }
}

pub struct SanityCheckLister<L: oio::List> {
    inner: L,
    prefix: String,
}

impl<L: oio::List> oio::List for SanityCheckLister<L> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        match self.inner.next().await? {
            Some(entry) => {
                let p = entry.path();
                if !p.starts_with(&self.prefix) {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        &format!(
                            "sanity check failed: entry {} is outside prefix {}",
                            p, self.prefix
                        ),
                    ));
                }
                Ok(Some(entry))
            }
            None => Ok(None),
        }
    }
}
