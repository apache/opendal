// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use crate::ops::{OpCreate, OpDelete, OpList, OpRead, OpStat, OpWrite};
use crate::{Accessor, BytesReader, BytesWriter, Layer, Metadata, ObjectStreamer};

use async_trait::async_trait;
use backon::{Retryable};

use std::fmt::{Debug};


use std::io::{ErrorKind, Result};
use std::sync::Arc;


#[derive(Debug, Clone)]
pub struct Retry<B: backon::Backoff + Debug + Send + Sync + 'static> {
    backoff: B,
}

impl<B> Layer for Retry<B>
where
    B: backon::Backoff + Debug + Send + Sync,
{
    fn layer(&self, inner: Arc<dyn Accessor>) -> Arc<dyn Accessor> {
        Arc::new(RetryableAccessor::create(inner, self.backoff.clone()))
    }
}

#[derive(Debug)]
struct RetryableAccessor<B: backon::Backoff + Debug + Send + Sync> {
    inner: Arc<dyn Accessor>,
    backoff: B,
}

impl<B> RetryableAccessor<B>
where
    B: backon::Backoff + Debug + Send + Sync,
{
    fn create(inner: Arc<dyn Accessor>, backoff: B) -> Self {
        Self { inner, backoff }
    }
}

#[async_trait]
impl<B> Accessor for RetryableAccessor<B>
where
    B: backon::Backoff + Debug + Send + Sync,
{
    async fn create(&self, args: &OpCreate) -> Result<()> {
        { || self.inner.create(args) }
            .retry(self.backoff.clone())
            .with_error_fn(|e| e.kind() == ErrorKind::Interrupted)
            .await
    }
    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        { || self.inner.read(args) }
            .retry(self.backoff.clone())
            .with_error_fn(|e| e.kind() == ErrorKind::Interrupted)
            .await
    }
    async fn write(&self, args: &OpWrite) -> Result<BytesWriter> {
        { || self.inner.write(args) }
            .retry(self.backoff.clone())
            .with_error_fn(|e| e.kind() == ErrorKind::Interrupted)
            .await
    }
    async fn stat(&self, args: &OpStat) -> Result<Metadata> {
        { || self.inner.stat(args) }
            .retry(self.backoff.clone())
            .with_error_fn(|e| e.kind() == ErrorKind::Interrupted)
            .await
    }
    async fn delete(&self, args: &OpDelete) -> Result<()> {
        { || self.inner.delete(args) }
            .retry(self.backoff.clone())
            .with_error_fn(|e| e.kind() == ErrorKind::Interrupted)
            .await
    }
    async fn list(&self, args: &OpList) -> Result<ObjectStreamer> {
        { || self.inner.list(args) }
            .retry(self.backoff.clone())
            .with_error_fn(|e| e.kind() == ErrorKind::Interrupted)
            .await
    }
}
