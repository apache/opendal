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

use std::sync::Arc;

use opendal_core::raw::*;
use opendal_core::*;

use crate::{Bucket, Buckets};

#[derive(Debug)]
pub(crate) struct AimdService {
    pub(crate) inner: Servicer,
    pub(crate) buckets: Arc<Buckets>,
}

impl Service for AimdService {
    type Reader = Observed<oio::Reader>;
    type Writer = Observed<oio::Writer>;
    type Lister = Observed<oio::Lister>;
    type Deleter = Observed<oio::Deleter>;
    type Copier = Observed<oio::Copier>;
    type Composer = Observed<oio::Composer>;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        self.inner.capability()
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        let result = self.inner.create_dir(ctx, path, args).await;
        self.buckets.write.observe(&result);
        result
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        let result = self.inner.stat(ctx, path, args).await;
        self.buckets.read.observe(&result);
        result
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        let result = self.inner.rename(ctx, from, to, args).await;
        self.buckets.write.observe(&result);
        result
    }

    async fn restore(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRestore,
    ) -> Result<RpRestore> {
        let result = self.inner.restore(ctx, path, args).await;
        self.buckets.write.observe(&result);
        result
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let result = self.inner.read(ctx, path, args);
        self.buckets.read.observe(&result);
        result.map(|inner| Observed {
            inner,
            bucket: self.buckets.read.clone(),
        })
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let result = self.inner.write(ctx, path, args);
        self.buckets.write.observe(&result);
        result.map(|inner| Observed {
            inner,
            bucket: self.buckets.write.clone(),
        })
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        let result = self.inner.delete(ctx);
        self.buckets.delete.observe(&result);
        result.map(|inner| Observed {
            inner,
            bucket: self.buckets.delete.clone(),
        })
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        let result = self.inner.list(ctx, path, args);
        self.buckets.list.observe(&result);
        result.map(|inner| Observed {
            inner,
            bucket: self.buckets.list.clone(),
        })
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> Result<Self::Copier> {
        let result = self.inner.copy(ctx, from, to, args);
        self.buckets.write.observe(&result);
        result.map(|inner| Observed {
            inner,
            bucket: self.buckets.write.clone(),
        })
    }

    fn compose(&self, ctx: &OperationContext, to: &str, args: OpCompose) -> Result<Self::Composer> {
        let result = self.inner.compose(ctx, to, args);
        self.buckets.write.observe(&result);
        result.map(|inner| Observed {
            inner,
            bucket: self.buckets.write.clone(),
        })
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        self.inner.presign(ctx, path, args).await
    }
}

pub(crate) struct Observed<T> {
    inner: T,
    bucket: Arc<Bucket>,
}

impl<T: oio::Read> oio::Read for Observed<T> {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let result = self.inner.open(range).await;
        self.bucket.observe(&result);
        result.map(|(rp, inner)| {
            (
                rp,
                Box::new(Observed {
                    inner,
                    bucket: self.bucket.clone(),
                }) as Box<dyn oio::ReadStreamDyn>,
            )
        })
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        let result = self.inner.read(range).await;
        self.bucket.observe(&result);
        result
    }
}

impl<T: oio::ReadStream> oio::ReadStream for Observed<T> {
    async fn read(&mut self) -> Result<Buffer> {
        let result = self.inner.read().await;
        self.bucket.observe(&result);
        result
    }

    async fn read_all(&mut self) -> Result<Buffer> {
        let result = self.inner.read_all().await;
        self.bucket.observe(&result);
        result
    }
}

impl<T: oio::Write> oio::Write for Observed<T> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let result = self.inner.write(bs).await;
        self.bucket.observe(&result);
        result
    }

    async fn copy_from(&mut self, path: &str, args: OpRead, range: BytesRange) -> Result<()> {
        let result = self.inner.copy_from(path, args, range).await;
        self.bucket.observe(&result);
        result
    }

    async fn close(&mut self) -> Result<Metadata> {
        let result = self.inner.close().await;
        self.bucket.observe(&result);
        result
    }

    async fn abort(&mut self) -> Result<()> {
        let result = self.inner.abort().await;
        self.bucket.observe(&result);
        result
    }
}

impl<T: oio::List> oio::List for Observed<T> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        let result = self.inner.next().await;
        self.bucket.observe(&result);
        result
    }
}

impl<T: oio::Delete> oio::Delete for Observed<T> {
    async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        let result = self.inner.delete(path, args).await;
        self.bucket.observe(&result);
        result
    }

    async fn close(&mut self) -> Result<()> {
        let result = self.inner.close().await;
        self.bucket.observe(&result);
        result
    }
}

impl<T: oio::Copy> oio::Copy for Observed<T> {
    async fn next(&mut self) -> Result<Option<usize>> {
        let result = self.inner.next().await;
        self.bucket.observe(&result);
        result
    }

    async fn close(&mut self) -> Result<Metadata> {
        let result = self.inner.close().await;
        self.bucket.observe(&result);
        result
    }

    async fn abort(&mut self) -> Result<()> {
        let result = self.inner.abort().await;
        self.bucket.observe(&result);
        result
    }
}

impl<T: oio::Compose> oio::Compose for Observed<T> {
    async fn compose(&mut self, path: &str, args: OpRead) -> Result<()> {
        let result = self.inner.compose(path, args).await;
        self.bucket.observe(&result);
        result
    }

    async fn close(&mut self) -> Result<Metadata> {
        let result = self.inner.close().await;
        self.bucket.observe(&result);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AimdConfig;
    use oio::List;
    use std::time::Duration;

    struct LimitedLister;

    impl oio::List for LimitedLister {
        async fn next(&mut self) -> Result<Option<oio::Entry>> {
            Err(Error::new(ErrorKind::RateLimited, "limited").set_temporary())
        }
    }

    #[tokio::test]
    async fn deferred_errors_preserve_retry_status_and_feed_the_budget() {
        let bucket = Arc::new(Bucket::new(AimdConfig {
            window: Duration::from_millis(10),
            ..Default::default()
        }));
        let mut lister = Observed {
            inner: LimitedLister,
            bucket: bucket.clone(),
        };
        let error = lister.next().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::RateLimited);
        assert!(error.is_temporary());
        tokio::time::sleep(Duration::from_millis(10)).await;
        bucket.acquire(tokio::time::sleep).await;
        assert_eq!(bucket.rate(), 1000.0);
    }
}
