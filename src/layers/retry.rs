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

use std::cell::RefCell;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::thread::sleep;

use async_trait::async_trait;
use backon::Backoff;
use backon::Retryable;
use futures::ready;
use futures::AsyncRead;
use log::warn;
use pin_project::pin_project;
use tokio::time::Sleep;

use crate::object::BlockingObjectPager;
use crate::object::ObjectPager;
use crate::ops::*;
use crate::*;

/// RetryLayer will add retry for OpenDAL.
///
/// # Examples
///
/// ```
/// use anyhow::Result;
/// use backon::ExponentialBackoff;
/// use opendal::layers::RetryLayer;
/// use opendal::Operator;
/// use opendal::Scheme;
///
/// let _ = Operator::from_env(Scheme::Fs)
///     .expect("must init")
///     .layer(RetryLayer::new(ExponentialBackoff::default()));
/// ```
pub struct RetryLayer<B: Backoff + Send + Sync + Debug + 'static>(B);

impl<B> RetryLayer<B>
where
    B: Backoff + Send + Sync + Debug + 'static,
{
    /// Create a new retry layer.
    /// # Examples
    ///
    /// ```
    /// use anyhow::Result;
    /// use backon::ExponentialBackoff;
    /// use opendal::layers::RetryLayer;
    /// use opendal::Operator;
    /// use opendal::Scheme;
    ///
    /// let _ = Operator::from_env(Scheme::Fs)
    ///     .expect("must init")
    ///     .layer(RetryLayer::new(ExponentialBackoff::default()));
    /// ```
    pub fn new(b: B) -> Self {
        Self(b)
    }
}

impl<B> Layer for RetryLayer<B>
where
    B: Backoff + Send + Sync + Debug + 'static,
{
    fn layer(&self, inner: Arc<dyn Accessor>) -> Arc<dyn Accessor> {
        Arc::new(RetryAccessor {
            inner,
            backoff: self.0.clone(),
        })
    }
}

#[derive(Clone)]
struct RetryAccessor<B: Backoff + Debug + Send + Sync> {
    inner: Arc<dyn Accessor>,
    backoff: B,
}

impl<B: Backoff + Debug + Send + Sync> Debug for RetryAccessor<B> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetryAccessor")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<B> Accessor for RetryAccessor<B>
where
    B: Backoff + Debug + Send + Sync + 'static,
{
    fn inner(&self) -> Option<Arc<dyn Accessor>> {
        Some(self.inner.clone())
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        { || self.inner.create(path, args.clone()) }
            .retry(self.backoff.clone())
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                warn!(
                    target: "opendal::service",
                    "operation={} -> retry after {}s: error={:?}",
                    Operation::Create, dur.as_secs_f64(), err)
            })
            .await
            .map_err(|e| e.set_persistent())
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, BytesReader)> {
        let (rp, r) = { || self.inner.read(path, args.clone()) }
            .retry(self.backoff.clone())
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                warn!(
                    target: "opendal::service",
                    "operation={} -> retry after {}s: error={:?}",
                    Operation::Read, dur.as_secs_f64(), err)
            })
            .await
            .map_err(|e| e.set_persistent())?;

        Ok((
            rp,
            Box::new(RetryReader::new(r, Operation::Read, self.backoff.clone())) as BytesReader,
        ))
    }

    /// Return `Interrupted` Error even after retry.
    ///
    /// Allowing users to retry the write request from upper logic.
    async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<RpWrite> {
        let r = Box::new(RetryReader::new(r, Operation::Write, self.backoff.clone()));
        let r = Box::new(CloneableReader::new(r));

        { || self.inner.write(path, args.clone(), r.clone()) }
            .retry(self.backoff.clone())
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                warn!(
                    target: "opendal::service",
                    "operation={} -> retry after {}s: error={:?}",
                    Operation::Write, dur.as_secs_f64(), err)
            })
            .await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        { || self.inner.stat(path, args.clone()) }
            .retry(self.backoff.clone())
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                warn!(
                    target: "opendal::service",
                    "operation={} -> retry after {}s: error={:?}",
                    Operation::Stat, dur.as_secs_f64(), err)
            })
            .await
            .map_err(|e| e.set_persistent())
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        { || self.inner.delete(path, args.clone()) }
            .retry(self.backoff.clone())
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                warn!(
                    target: "opendal::service",
                    "operation={} -> retry after {}s: error={:?}",
                    Operation::Delete, dur.as_secs_f64(), err)
            })
            .await
            .map_err(|e| e.set_persistent())
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, ObjectPager)> {
        { || self.inner.list(path, args.clone()) }
            .retry(self.backoff.clone())
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                warn!(
                    target: "opendal::service",
                    "operation={} -> retry after {}s: error={:?}",
                    Operation::List, dur.as_secs_f64(), err)
            })
            .await
            .map_err(|e| e.set_persistent())
    }

    async fn create_multipart(
        &self,
        path: &str,
        args: OpCreateMultipart,
    ) -> Result<RpCreateMultipart> {
        { || self.inner.create_multipart(path, args.clone()) }
            .retry(self.backoff.clone())
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                warn!(
                    target: "opendal::service",
                    "operation={} -> retry after {}s: error={:?}",
                    Operation::CreateMultipart, dur.as_secs_f64(), err)
            })
            .await
            .map_err(|e| e.set_persistent())
    }

    async fn write_multipart(
        &self,
        path: &str,
        args: OpWriteMultipart,
        r: BytesReader,
    ) -> Result<RpWriteMultipart> {
        // Write can't retry, until can reset this reader.
        self.inner
            .write_multipart(path, args.clone(), r)
            .await
            .map_err(|e| e.set_persistent())
    }

    async fn complete_multipart(
        &self,
        path: &str,
        args: OpCompleteMultipart,
    ) -> Result<RpCompleteMultipart> {
        { || self.inner.complete_multipart(path, args.clone()) }
            .retry(self.backoff.clone())
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                warn!(
                    target: "opendal::service",
                    "operation={} -> retry after {}s: error={:?}",
                    Operation::CompleteMultipart, dur.as_secs_f64(), err)
            })
            .await
            .map_err(|e| e.set_persistent())
    }

    async fn abort_multipart(
        &self,
        path: &str,
        args: OpAbortMultipart,
    ) -> Result<RpAbortMultipart> {
        { || self.inner.abort_multipart(path, args.clone()) }
            .retry(self.backoff.clone())
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                warn!(
                    target: "opendal::service",
                    "operation={} -> retry after {}s: error={:?}",
                    Operation::AbortMultipart, dur.as_secs_f64(), err)
            })
            .await
            .map_err(|e| e.set_persistent())
    }

    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        let retry = self.backoff.clone();

        let mut e = None;

        for dur in retry {
            let res = self.inner.blocking_create(path, args.clone());

            match res {
                Ok(v) => return Ok(v),
                Err(err) => {
                    let retryable = err.is_temporary();
                    e = Some(err);

                    if retryable {
                        sleep(dur);
                        warn!(
                            target: "opendal::service",
                            "operation={} path={} -> retry after {}s: error={:?}",
                            Operation::BlockingCreate, path, dur.as_secs_f64(), e);
                        continue;
                    } else {
                        return Err(e.unwrap());
                    }
                }
            }
        }

        Err(e.unwrap())
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, BlockingBytesReader)> {
        let retry = self.backoff.clone();

        let mut e = None;

        for dur in retry {
            let res = self.inner.blocking_read(path, args.clone());

            match res {
                Ok(v) => return Ok(v),
                Err(err) => {
                    let retryable = err.is_temporary();
                    e = Some(err);

                    if retryable {
                        sleep(dur);
                        warn!(
                            target: "opendal::service",
                            "operation={} path={} -> retry after {}s: error={:?}",
                            Operation::BlockingRead, path, dur.as_secs_f64(), e);
                        continue;
                    } else {
                        return Err(e.unwrap());
                    }
                }
            }
        }

        Err(e.unwrap())
    }

    fn blocking_write(&self, path: &str, args: OpWrite, r: BlockingBytesReader) -> Result<RpWrite> {
        self.inner.blocking_write(path, args, r)
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let retry = self.backoff.clone();

        let mut e = None;

        for dur in retry {
            let res = self.inner.blocking_stat(path, args.clone());

            match res {
                Ok(v) => return Ok(v),
                Err(err) => {
                    let retryable = err.is_temporary();
                    e = Some(err);

                    if retryable {
                        sleep(dur);
                        warn!(
                            target: "opendal::service",
                            "operation={} path={} -> retry after {}s: error={:?}",
                            Operation::BlockingStat, path, dur.as_secs_f64(), e);
                        continue;
                    } else {
                        return Err(e.unwrap());
                    }
                }
            }
        }

        Err(e.unwrap())
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        let retry = self.backoff.clone();

        let mut e = None;

        for dur in retry {
            let res = self.inner.blocking_delete(path, args.clone());

            match res {
                Ok(v) => return Ok(v),
                Err(err) => {
                    let retryable = err.is_temporary();
                    e = Some(err);

                    if retryable {
                        sleep(dur);
                        warn!(
                            target: "opendal::service",
                            "operation={} path={} -> retry after {}s: error={:?}",
                            Operation::BlockingDelete, path, dur.as_secs_f64(), e);
                        continue;
                    } else {
                        return Err(e.unwrap());
                    }
                }
            }
        }

        Err(e.unwrap())
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, BlockingObjectPager)> {
        let retry = self.backoff.clone();

        let mut e = None;

        for dur in retry {
            let res = self.inner.blocking_list(path, args.clone());

            match res {
                Ok(v) => return Ok(v),
                Err(err) => {
                    let retryable = err.is_temporary();
                    e = Some(err);

                    if retryable {
                        sleep(dur);
                        warn!(
                            target: "opendal::service",
                            "operation={} path={} -> retry after {}s: error={:?}",
                            Operation::BlockingList, path, dur.as_secs_f64(), e);
                        continue;
                    } else {
                        return Err(e.unwrap());
                    }
                }
            }
        }

        Err(e.unwrap())
    }
}

#[pin_project]
struct RetryReader<B: Backoff + Debug + Send + Sync> {
    inner: BytesReader,
    op: Operation,

    backoff: B,
    retry: Option<B>,
    sleep: Option<Pin<Box<Sleep>>>,
}

impl<B: Backoff + Debug + Send + Sync> RetryReader<B> {
    fn new(inner: BytesReader, op: Operation, backoff: B) -> Self {
        Self {
            inner,
            op,
            backoff,
            retry: None,
            sleep: None,
        }
    }
}

impl<B> AsyncRead for RetryReader<B>
where
    B: Backoff + Debug + Send + Sync,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.project();

        loop {
            if let Some(fut) = this.sleep {
                ready!(fut.as_mut().poll(cx));
                *this.sleep = None;
            }

            let res = ready!(Pin::new(&mut *this.inner).poll_read(cx, buf));

            match res {
                Ok(v) => {
                    // Reset retry to none.
                    *this.retry = None;

                    return Poll::Ready(Ok(v));
                }
                Err(err) => {
                    let kind = err.kind();

                    if kind == io::ErrorKind::Interrupted {
                        let retry = if let Some(retry) = this.retry {
                            retry
                        } else {
                            *this.retry = Some(this.backoff.clone());
                            this.retry.as_mut().unwrap()
                        };

                        match retry.next() {
                            None => {
                                // Reset retry to none.
                                *this.retry = None;

                                return Poll::Ready(Err(err));
                            }
                            Some(dur) => {
                                warn!(
                                    target: "opendal::service",
                                    "operation={}  -> retry after {}s: error={:?}",
                                    *this.op, dur.as_secs_f64(), err);

                                *this.sleep = Some(Box::pin(tokio::time::sleep(dur)));
                                continue;
                            }
                        }
                    } else {
                        // Reset retry to none.
                        *this.retry = None;

                        return Poll::Ready(Err(err));
                    }
                }
            }
        }
    }
}

/// CloneableReader makes a reader cloneable.
///
/// # Safety
///
/// `AsyncRead` makes sure that only one mutable reference will be alive.
///
/// Instead of `Mutex`, we use a `RefCell` to borrow the inner reader at runtime.
#[derive(Clone)]
struct CloneableReader {
    inner: Arc<RefCell<BytesReader>>,
}

unsafe impl Send for CloneableReader {}
unsafe impl Sync for CloneableReader {}

impl CloneableReader {
    fn new(r: BytesReader) -> Self {
        Self {
            inner: Arc::new(RefCell::new(r)),
        }
    }
}

impl AsyncRead for CloneableReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let mut r = (*self.inner).borrow_mut();
        Pin::new(r.as_mut()).poll_read(cx, buf)
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::task::Context;
    use std::task::Poll;
    use std::time::Duration;

    use anyhow::anyhow;
    use async_trait::async_trait;
    use backon::ConstantBackoff;
    use futures::io::Cursor;
    use futures::AsyncRead;
    use futures::AsyncReadExt;

    use crate::layers::RetryLayer;
    use crate::ops::*;
    use crate::Accessor;
    use crate::BytesReader;
    use crate::Error;
    use crate::ErrorKind;
    use crate::Operator;
    use crate::Result;

    #[derive(Debug, Clone, Default)]
    struct MockService {
        attempt: Arc<Mutex<usize>>,
    }

    #[async_trait]
    impl Accessor for MockService {
        async fn read(&self, path: &str, _: OpRead) -> Result<(RpRead, BytesReader)> {
            let mut attempt = self.attempt.lock().unwrap();
            *attempt += 1;

            match path {
                "retryable_error" => {
                    Err(Error::new(ErrorKind::Unexpected, "retryable_error").set_temporary())
                }
                _ => Err(Error::new(ErrorKind::Unexpected, "not_retryable_error")),
            }
        }

        async fn write(&self, path: &str, _: OpWrite, _: BytesReader) -> Result<RpWrite> {
            let mut attempt = self.attempt.lock().unwrap();
            *attempt += 1;

            match path {
                "retryable_error" => {
                    Err(Error::new(ErrorKind::Unexpected, "retryable_error").set_temporary())
                }
                _ => Err(Error::new(ErrorKind::Unexpected, "not_retryable_error")),
            }
        }
    }

    #[tokio::test]
    async fn test_retry_retryable_error() -> anyhow::Result<()> {
        let _ = env_logger::try_init();

        let srv = Arc::new(MockService::default());

        let backoff = ConstantBackoff::default()
            .with_delay(Duration::from_micros(1))
            .with_max_times(10);
        let op = Operator::new(srv.clone()).layer(RetryLayer::new(backoff));

        let result = op.object("retryable_error").read().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("retryable_error"));
        // The error is retryable, we should request it 1 + 10 times.
        assert_eq!(*srv.attempt.lock().unwrap(), 11);

        Ok(())
    }

    #[tokio::test]
    async fn test_retry_retryable_error_write() -> anyhow::Result<()> {
        let _ = env_logger::try_init();

        let srv = Arc::new(MockService::default());

        let backoff = ConstantBackoff::default()
            .with_delay(Duration::from_micros(1))
            .with_max_times(10);
        let op = Operator::new(srv.clone()).layer(RetryLayer::new(backoff));

        let bs = Box::new(Cursor::new("Hello, World!".as_bytes()));
        let result = op.object("retryable_error").write_from(13, bs).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("retryable_error"));
        // The error is retryable, we should request it 1 + 10 times.
        assert_eq!(*srv.attempt.lock().unwrap(), 11);

        Ok(())
    }

    #[tokio::test]
    async fn test_retry_not_retryable_error() -> anyhow::Result<()> {
        let srv = Arc::new(MockService::default());

        let backoff = ConstantBackoff::default();
        let op = Operator::new(srv.clone()).layer(RetryLayer::new(backoff));

        let result = op.object("not_retryable_error").read().await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not_retryable_error"));
        // The error is not retryable, we should only request it once.
        assert_eq!(*srv.attempt.lock().unwrap(), 1);

        Ok(())
    }

    #[derive(Debug, Clone, Default)]
    struct MockReadService {
        attempt: Arc<Mutex<usize>>,
    }

    #[async_trait]
    impl Accessor for MockReadService {
        async fn read(&self, _: &str, _: OpRead) -> Result<(RpRead, BytesReader)> {
            Ok((
                RpRead::new(0),
                Box::new(MockReader {
                    attempt: self.attempt.clone(),
                }) as BytesReader,
            ))
        }

        async fn write(&self, _: &str, args: OpWrite, mut r: BytesReader) -> Result<RpWrite> {
            {
                let mut attempt = self.attempt.lock().unwrap();
                *attempt += 1;

                if *attempt < 2 {
                    return Err(
                        Error::new(ErrorKind::Unexpected, "retryable_error").set_temporary()
                    );
                }
            }

            let size = futures::io::copy(&mut r, &mut futures::io::sink())
                .await
                .map_err(|err| Error::new(ErrorKind::Unexpected, "copy failed").set_source(err))?;
            assert_eq!(size, args.size());
            Ok(RpWrite::new(args.size()))
        }
    }

    #[derive(Debug, Clone, Default)]
    struct MockReader {
        attempt: Arc<Mutex<usize>>,
    }

    impl AsyncRead for MockReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            let mut attempt = self.attempt.lock().unwrap();
            *attempt += 1;

            Poll::Ready(match *attempt {
                1 => Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    anyhow!("retryable_error from reader"),
                )),
                2 => {
                    buf[..7].copy_from_slice("Hello, ".as_bytes());
                    Ok(7)
                }
                3 => Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    anyhow!("retryable_error from reader"),
                )),
                4 => {
                    buf[..6].copy_from_slice("World!".as_bytes());
                    Ok(6)
                }
                5 => Ok(0),
                _ => unreachable!(),
            })
        }
    }

    #[tokio::test]
    async fn test_retry_read() -> anyhow::Result<()> {
        let _ = env_logger::try_init();

        let srv = Arc::new(MockReadService::default());

        let backoff = ConstantBackoff::default();
        let op = Operator::new(srv.clone()).layer(RetryLayer::new(backoff));

        let mut r = op.object("retryable_error").reader().await.unwrap();
        let mut content = Vec::new();
        let size = r
            .read_to_end(&mut content)
            .await
            .expect("read must succeed");
        assert_eq!(size, 13);
        assert_eq!(content, "Hello, World!".as_bytes());
        // The error is retryable, we should request it 1 + 10 times.
        assert_eq!(*srv.attempt.lock().unwrap(), 5);

        Ok(())
    }

    #[tokio::test]
    async fn test_retry_write() -> anyhow::Result<()> {
        let _ = env_logger::try_init();

        let srv = Arc::new(MockReadService::default());

        let backoff = ConstantBackoff::default();
        let op = Operator::new(srv.clone()).layer(RetryLayer::new(backoff));

        op.object("retryable_error")
            .write_from(
                6,
                Box::new(MockReader {
                    attempt: srv.attempt.clone(),
                }),
            )
            .await
            .expect("write from must succeed");

        Ok(())
    }
}
