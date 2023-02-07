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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::io;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::thread::sleep;

use async_trait::async_trait;
use backon::Backoff;
use backon::Retryable;
use futures::ready;
use futures::FutureExt;
use log::warn;

use crate::raw::*;
use crate::*;

/// Add retry for temporary failed operations.
///
/// # Examples
///
/// ```
/// use anyhow::Result;
/// use backon::ExponentialBackoff;
/// use opendal::layers::RetryLayer;
/// use opendal::services;
/// use opendal::Operator;
/// use opendal::Scheme;
///
/// let _ = Operator::create(services::Memory::default())
///     .expect("must init")
///     .layer(RetryLayer::new(ExponentialBackoff::default()))
///     .finish();
/// ```
pub struct RetryLayer<B: Backoff + Send + Sync + Debug + Unpin + 'static>(B);

impl<B> RetryLayer<B>
where
    B: Backoff + Send + Sync + Debug + Unpin + 'static,
{
    /// Create a new retry layer.
    /// # Examples
    ///
    /// ```
    /// use anyhow::Result;
    /// use backon::ExponentialBackoff;
    /// use opendal::layers::RetryLayer;
    /// use opendal::services;
    /// use opendal::Operator;
    /// use opendal::Scheme;
    ///
    /// let _ = Operator::create(services::Memory::default())
    ///     .expect("must init")
    ///     .layer(RetryLayer::new(ExponentialBackoff::default()));
    /// ```
    pub fn new(b: B) -> Self {
        Self(b)
    }
}

impl<A, B> Layer<A> for RetryLayer<B>
where
    A: Accessor,
    B: Backoff + Send + Sync + Debug + Unpin + 'static,
{
    type LayeredAccessor = RetryAccessor<A, B>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        RetryAccessor {
            inner,
            backoff: self.0.clone(),
        }
    }
}

#[derive(Clone)]
pub struct RetryAccessor<A: Accessor, B: Backoff + Debug + Send + Sync + Unpin> {
    inner: A,
    backoff: B,
}

impl<A: Accessor, B: Backoff + Debug + Send + Sync + Unpin> Debug for RetryAccessor<A, B> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetryAccessor")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<A, B> LayeredAccessor for RetryAccessor<A, B>
where
    A: Accessor,
    B: Backoff + Debug + Send + Sync + Unpin + 'static,
{
    type Inner = A;
    type Reader = RetryReader<A::Reader, B>;
    type BlockingReader = RetryReader<A::BlockingReader, B>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
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
            .map(|v| v.map_err(|e| e.set_persistent()))
            .await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        { || self.inner.read(path, args.clone()) }
            .retry(self.backoff.clone())
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                warn!(
                    target: "opendal::service",
                    "operation={} -> retry after {}s: error={:?}",
                    Operation::Read, dur.as_secs_f64(), err)
            })
            .map(|v| {
                v.map(|(rp, r)| (rp, RetryReader::new(r, path, self.backoff.clone())))
                    .map_err(|e| e.set_persistent())
            })
            .await
    }

    /// Return `Interrupted` Error even after retry.
    ///
    /// Allowing users to retry the write request from upper logic.
    async fn write(&self, path: &str, args: OpWrite, r: input::Reader) -> Result<RpWrite> {
        self.inner.write(path, args, r).await
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
            .map(|v| v.map_err(|e| e.set_persistent()))
            .await
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
            .map(|v| v.map_err(|e| e.set_persistent()))
            .await
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
            .map(|v| v.map_err(|e| e.set_persistent()))
            .await
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
            .map(|v| v.map_err(|e| e.set_persistent()))
            .await
    }

    async fn write_multipart(
        &self,
        path: &str,
        args: OpWriteMultipart,
        r: input::Reader,
    ) -> Result<RpWriteMultipart> {
        // Write can't retry, until can reset this reader.
        self.inner.write_multipart(path, args.clone(), r).await
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
            .map(|v| v.map_err(|e| e.set_persistent()))
            .await
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
            .map(|v| v.map_err(|e| e.set_persistent()))
            .await
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

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        let retry = self.backoff.clone();

        let mut e = None;

        for dur in retry {
            let res = self.inner.blocking_read(path, args.clone());

            match res {
                Ok((rp, r)) => return Ok((rp, RetryReader::new(r, path, self.backoff.clone()))),
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

    fn blocking_write(
        &self,
        path: &str,
        args: OpWrite,
        r: input::BlockingReader,
    ) -> Result<RpWrite> {
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

/// TODO: Refactor me to replace duplicated code.
pub struct RetryReader<R, B: Backoff + Debug + Send + Sync + Unpin> {
    inner: R,
    path: String,
    backoff: B,
    current_backoff: Option<B>,
    sleep: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl<R, B: Backoff + Debug + Send + Sync + Unpin> RetryReader<R, B> {
    fn new(inner: R, path: &str, backoff: B) -> Self {
        Self {
            inner,
            path: path.to_string(),
            backoff,
            current_backoff: None,
            sleep: None,
        }
    }

    fn is_retryable_error(err: &io::Error) -> bool {
        matches!(
            err.kind(),
            io::ErrorKind::ConnectionRefused
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::ConnectionAborted
                | io::ErrorKind::NotConnected
                | io::ErrorKind::BrokenPipe
                | io::ErrorKind::WouldBlock
                | io::ErrorKind::TimedOut
                | io::ErrorKind::Interrupted
                | io::ErrorKind::UnexpectedEof
                | io::ErrorKind::Other
        )
    }
}

impl<R: output::Read, B: Backoff + Debug + Send + Sync + Unpin> output::Read for RetryReader<R, B> {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        if let Some(sleep) = self.sleep.as_mut() {
            ready!(sleep.poll_unpin(cx));
            self.sleep = None;
        }

        match ready!(self.inner.poll_read(cx, buf)) {
            Ok(v) => {
                self.current_backoff = None;
                Poll::Ready(Ok(v))
            }
            Err(err) if !Self::is_retryable_error(&err) => {
                self.current_backoff = None;
                Poll::Ready(Err(err))
            }
            Err(err) => {
                let backoff = match self.current_backoff.as_mut() {
                    Some(backoff) => backoff,
                    None => {
                        self.current_backoff = Some(self.backoff.clone());
                        self.current_backoff.as_mut().unwrap()
                    }
                };

                match backoff.next() {
                    None => {
                        self.current_backoff = None;
                        Poll::Ready(Err(err))
                    }
                    Some(dur) => {
                        warn!(
                            target: "opendal::service",
                            "operation={} path={} -> retry after {}s: error={:?}",
                            Operation::Read, self.path, dur.as_secs_f64(), err);
                        self.sleep = Some(Box::pin(tokio::time::sleep(dur)));
                        self.poll_read(cx, buf)
                    }
                }
            }
        }
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: io::SeekFrom) -> Poll<io::Result<u64>> {
        if let Some(sleep) = self.sleep.as_mut() {
            ready!(sleep.poll_unpin(cx));
            self.sleep = None;
        }

        match ready!(self.inner.poll_seek(cx, pos)) {
            Ok(v) => {
                self.current_backoff = None;
                Poll::Ready(Ok(v))
            }
            Err(err) if !Self::is_retryable_error(&err) => {
                self.current_backoff = None;
                Poll::Ready(Err(err))
            }
            Err(err) => {
                let backoff = match self.current_backoff.as_mut() {
                    Some(backoff) => backoff,
                    None => {
                        self.current_backoff = Some(self.backoff.clone());
                        self.current_backoff.as_mut().unwrap()
                    }
                };

                match backoff.next() {
                    None => {
                        self.current_backoff = None;
                        Poll::Ready(Err(err))
                    }
                    Some(dur) => {
                        warn!(
                            target: "opendal::service",
                            "operation={} path={} -> retry after {}s: error={:?}",
                            Operation::Read, self.path, dur.as_secs_f64(), err);
                        self.sleep = Some(Box::pin(tokio::time::sleep(dur)));
                        self.poll_seek(cx, pos)
                    }
                }
            }
        }
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<bytes::Bytes>>> {
        if let Some(sleep) = self.sleep.as_mut() {
            ready!(sleep.poll_unpin(cx));
            self.sleep = None;
        }

        match ready!(self.inner.poll_next(cx)) {
            None => {
                self.current_backoff = None;
                Poll::Ready(None)
            }
            Some(Ok(v)) => {
                self.current_backoff = None;
                Poll::Ready(Some(Ok(v)))
            }
            Some(Err(err)) if !Self::is_retryable_error(&err) => {
                self.current_backoff = None;
                Poll::Ready(Some(Err(err)))
            }
            Some(Err(err)) => {
                let backoff = match self.current_backoff.as_mut() {
                    Some(backoff) => backoff,
                    None => {
                        self.current_backoff = Some(self.backoff.clone());
                        self.current_backoff.as_mut().unwrap()
                    }
                };

                match backoff.next() {
                    None => {
                        self.current_backoff = None;
                        Poll::Ready(Some(Err(err)))
                    }
                    Some(dur) => {
                        warn!(
                            target: "opendal::service",
                            "operation={} path={} -> retry after {}s: error={:?}",
                            Operation::Read, self.path, dur.as_secs_f64(), err);
                        self.sleep = Some(Box::pin(tokio::time::sleep(dur)));
                        self.poll_next(cx)
                    }
                }
            }
        }
    }
}

impl<R: output::BlockingRead, B: Backoff + Debug + Send + Sync + Unpin + 'static>
    output::BlockingRead for RetryReader<R, B>
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let retry = self.backoff.clone();

        let mut e = None;

        for dur in retry {
            let res = self.inner.read(buf);
            match res {
                Ok(v) => return Ok(v),
                Err(err) => {
                    let retryable = Self::is_retryable_error(&err);
                    e = Some(err);

                    if retryable {
                        sleep(dur);
                        warn!(
                            target: "opendal::service",
                            "operation={} path={} -> retry after {}s: error={:?}",
                            Operation::BlockingRead, self.path, dur.as_secs_f64(), e);
                        continue;
                    } else {
                        return Err(e.unwrap());
                    }
                }
            }
        }

        Err(e.unwrap())
    }

    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        let retry = self.backoff.clone();

        let mut e = None;

        for dur in retry {
            let res = self.inner.seek(pos);

            match res {
                Ok(v) => return Ok(v),
                Err(err) => {
                    let retryable = Self::is_retryable_error(&err);
                    e = Some(err);

                    if retryable {
                        sleep(dur);
                        warn!(
                            target: "opendal::service",
                            "operation={} path={} -> retry after {}s: error={:?}",
                            Operation::BlockingRead, self.path, dur.as_secs_f64(), e);
                        continue;
                    } else {
                        return Err(e.unwrap());
                    }
                }
            }
        }

        Err(e.unwrap())
    }

    fn next(&mut self) -> Option<io::Result<bytes::Bytes>> {
        let retry = self.backoff.clone();

        let mut e = None;

        for dur in retry {
            let res = self.inner.next();

            match res {
                None => return None,
                Some(Ok(v)) => return Some(Ok(v)),
                Some(Err(err)) => {
                    let retryable = Self::is_retryable_error(&err);
                    e = Some(err);

                    if retryable {
                        sleep(dur);
                        warn!(
                            target: "opendal::service",
                            "operation={} path={} -> retry after {}s: error={:?}",
                            Operation::BlockingRead, self.path, dur.as_secs_f64(), e);
                        continue;
                    } else {
                        return Some(Err(e.unwrap()));
                    };
                }
            }
        }

        Some(Err(e.unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;
    use async_trait::async_trait;
    use backon::ConstantBackoff;
    use bytes::Bytes;
    use futures::AsyncReadExt;
    use std::io;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::task::Context;
    use std::task::Poll;

    use super::*;

    #[derive(Debug, Clone, Default)]
    struct MockService {
        attempt: Arc<Mutex<usize>>,
    }

    #[async_trait]
    impl Accessor for MockService {
        type Reader = MockReader;
        type BlockingReader = ();

        fn metadata(&self) -> AccessorMetadata {
            let mut am = AccessorMetadata::default();
            am.set_hints(AccessorHint::ReadIsStreamable);

            am
        }

        async fn read(&self, _: &str, _: OpRead) -> Result<(RpRead, Self::Reader)> {
            Ok((
                RpRead::new(13),
                MockReader {
                    attempt: self.attempt.clone(),
                    pos: 0,
                },
            ))
        }
    }

    #[derive(Debug, Clone, Default)]
    struct MockReader {
        attempt: Arc<Mutex<usize>>,
        pos: u64,
    }

    impl output::Read for MockReader {
        fn poll_read(&mut self, _: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
            let mut attempt = self.attempt.lock().unwrap();
            *attempt += 1;

            Poll::Ready(match *attempt {
                1 => Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    anyhow!("retryable_error from reader"),
                )),
                2 => {
                    buf[..7].copy_from_slice("Hello, ".as_bytes());
                    self.pos += 7;
                    Ok(7)
                }
                3 => Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    anyhow!("retryable_error from reader"),
                )),
                4 => {
                    buf[..6].copy_from_slice("World!".as_bytes());
                    self.pos += 6;
                    Ok(6)
                }
                5 => Ok(0),
                _ => unreachable!(),
            })
        }

        fn poll_seek(&mut self, _: &mut Context<'_>, pos: io::SeekFrom) -> Poll<io::Result<u64>> {
            self.pos = match pos {
                io::SeekFrom::Current(n) => (self.pos as i64 + n) as u64,
                io::SeekFrom::Start(n) => n,
                io::SeekFrom::End(n) => (13 + n) as u64,
            };

            Poll::Ready(Ok(self.pos))
        }

        fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<Bytes>>> {
            let mut bs = vec![0; 1];
            match ready!(self.poll_read(cx, &mut bs)) {
                Ok(v) if v == 0 => Poll::Ready(None),
                Ok(v) => Poll::Ready(Some(Ok(Bytes::from(bs[..v].to_vec())))),
                Err(err) => Poll::Ready(Some(Err(err))),
            }
        }
    }

    #[tokio::test]
    async fn test_retry() {
        let _ = env_logger::try_init();

        let srv = Arc::new(MockService::default());
        let backoff = ConstantBackoff::default();
        let op = Operator::new(srv.clone())
            .layer(RetryLayer::new(backoff))
            .finish();

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
    }
}
