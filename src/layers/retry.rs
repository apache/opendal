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

//! Provide backoff retry support via implement [`Layer`] for [`backon::Backoff`](https://docs.rs/backon/latest/backon/trait.Backoff.html)

use std::fmt::Debug;
use std::io::ErrorKind;
use std::io::Result;
use std::sync::Arc;

use async_trait::async_trait;
use backon::Retryable;

use crate::ops::OpCreate;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::BytesReader;
use crate::BytesWriter;
use crate::Layer;
use crate::ObjectMetadata;
use crate::{Accessor, DirStreamer};

/// Implement [`Layer`] for [`backon::Backoff`](https://docs.rs/backon/latest/backon/trait.Backoff.html) so that all backoff can be used as a layer
///
/// # Example
///
///
/// ```
/// # use std::sync::Arc;
/// # use anyhow::Result;
/// # use opendal::services::fs;
/// # use opendal::services::fs::Builder;
/// use backon::ExponentialBackoff;
/// use opendal::Operator;
///
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// #     let accessor = fs::Backend::build().finish().await?;
/// let op = Operator::new(accessor).layer(ExponentialBackoff::default());
/// // All operations will be retried if the error is retryable
/// let _ = op.object("test_file").read();
/// # Ok(())
/// # }
/// ```
impl<B: 'static> Layer for B
where
    B: backon::Backoff + Debug + Send + Sync,
{
    fn layer(&self, inner: Arc<dyn Accessor>) -> Arc<dyn Accessor> {
        Arc::new(RetryableAccessor::create(inner, self.clone()))
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
    async fn stat(&self, args: &OpStat) -> Result<ObjectMetadata> {
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
    async fn list(&self, args: &OpList) -> Result<DirStreamer> {
        { || self.inner.list(args) }
            .retry(self.backoff.clone())
            .with_error_fn(|e| e.kind() == ErrorKind::Interrupted)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::Arc;
    use std::time::Duration;

    use anyhow::anyhow;
    use async_trait::async_trait;
    use backon::ConstantBackoff;
    use tokio::sync::Mutex;

    use crate::error::other;
    use crate::ops::OpRead;
    use crate::Accessor;
    use crate::BytesReader;
    use crate::Operator;

    #[derive(Debug, Clone, Default)]
    struct MockService {
        attempt: Arc<Mutex<usize>>,
    }

    #[async_trait]
    impl Accessor for MockService {
        async fn read(&self, args: &OpRead) -> std::io::Result<BytesReader> {
            let mut attempt = self.attempt.lock().await;
            *attempt += 1;

            match args.path() {
                "retryable_error" => Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    anyhow!("retryable_error"),
                )),
                _ => Err(other(anyhow!("not_retryable_error"))),
            }
        }
    }

    #[tokio::test]
    async fn test_retry_retryable_error() -> anyhow::Result<()> {
        let srv = Arc::new(MockService::default());

        let backoff = ConstantBackoff::default()
            .with_delay(Duration::from_micros(1))
            .with_max_times(10);
        let op = Operator::new(srv.clone()).layer(backoff);

        let result = op.object("retryable_error").read().await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "retryable_error");
        // The error is retryable, we should request it 1 + 10 times.
        assert_eq!(*srv.attempt.lock().await, 11);

        Ok(())
    }

    #[tokio::test]
    async fn test_retry_not_retryable_error() -> anyhow::Result<()> {
        let srv = Arc::new(MockService::default());

        let backoff = ConstantBackoff::default();
        let op = Operator::new(srv.clone()).layer(backoff);

        let result = op.object("not_retryable_error").read().await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "not_retryable_error");
        // The error is not retryable, we should only request it once.
        assert_eq!(*srv.attempt.lock().await, 1);

        Ok(())
    }
}
