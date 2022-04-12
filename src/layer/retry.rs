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
use async_compat::CompatExt;
use async_trait::async_trait;
use backoff::backoff::Backoff;
use backoff::exponential::ExponentialBackoff;
use backoff::{future::retry, SystemClock};
use futures::TryFutureExt;
use std::fmt::{Debug, Formatter};
use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Retry {
    backoff: ExponentialBackoff<SystemClock>,
}

impl Retry {}

impl Layer for Retry {
    fn layer(&self, inner: Arc<dyn Accessor>) -> Arc<dyn Accessor> {
        Arc::new(RetryableAccessor::create(inner, self.clone()))
    }
}

#[derive(Debug)]
struct RetryableAccessor {
    inner: Arc<dyn Accessor>,
    retry: Retry,
}

impl RetryableAccessor {
    fn create(inner: Arc<dyn Accessor>, retry: Retry) -> Self {
        Self { inner, retry }
    }
}

#[async_trait]
impl Accessor for RetryableAccessor {
    async fn create(&self, args: &OpCreate) -> Result<()> {
        retry(self.retry.backoff.clone(), || async {
            self.inner.create(args).await.map_err(retryable_error)
        })
        .await
    }
    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        retry(self.retry.backoff.clone(), || async {
            self.inner.read(args).await.map_err(retryable_error)
        })
        .await
    }
    async fn write(&self, args: &OpWrite) -> Result<BytesWriter> {
        retry(self.retry.backoff.clone(), || async {
            self.inner.write(args).await.map_err(retryable_error)
        })
        .await
    }
    async fn stat(&self, args: &OpStat) -> Result<Metadata> {
        retry(self.retry.backoff.clone(), || async {
            self.inner.stat(args).await.map_err(retryable_error)
        })
        .await
    }
    async fn delete(&self, args: &OpDelete) -> Result<()> {
        retry(self.retry.backoff.clone(), || async {
            self.inner.delete(args).await.map_err(retryable_error)
        })
        .await
    }
    async fn list(&self, args: &OpList) -> Result<ObjectStreamer> {
        retry(self.retry.backoff.clone(), || async {
            self.inner.list(args).await.map_err(retryable_error)
        })
        .await
    }
}

struct RetryableReader {
    is_read: bool,
}

struct RetryableWriter {
    is_written: bool,
}

fn retryable_error(e: Error) -> backoff::Error<Error> {
    if e.kind() == ErrorKind::Interrupted {
        backoff::Error::transient(e)
    } else {
        backoff::Error::permanent(e)
    }
}
