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

use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::task::ready;
use std::{fmt, fmt::Formatter};

use futures::Stream;

use crate::raw::*;
use crate::*;

/// `Copier` drives a long-running copy operation one step at a time.
///
/// - `Copier` implements `Stream<Item = Result<usize>>`.
/// - `Some(Ok(n))` means the copy operation made progress by `n` bytes.
/// - `None` means the copy operation has completed.
///
/// A copier completes the copy before it terminates: the inner copier is closed
/// and its completion response is processed before the stream yields `None`, and
/// [`Copier::close`] returns the metadata from that response. Aborting or
/// failing a copier makes it terminal: later [`Copier::next`] calls and stream
/// polls return `None` without driving the inner copier again, and
/// [`Copier::close`] fails instead of reporting a result.
pub struct Copier {
    /// The current state of this copier.
    state: State,
}

/// State of a [`Copier`].
///
/// While the copy makes progress the copier alternates between `Idle` and
/// `InFlight`. `Closed` and `Aborted` end the copy, and `Failed` records that
/// the copy cannot continue, though the inner copier can still be aborted.
enum State {
    /// The inner copier is ready to be advanced.
    Idle(oio::Copier),
    /// A step is in flight and owns the inner copier.
    ///
    /// The step owns the inner copier, so a canceled stream poll never drops
    /// progress the inner copier already made.
    InFlight(BoxedStaticFuture<Step>),
    /// The copy completed and `close_dyn` returned its metadata.
    Closed(Metadata),
    /// The copy was aborted by the caller.
    Aborted,
    /// The copy, or the final `close_dyn`, failed.
    ///
    /// The copier is kept so the caller can still abort a partially created
    /// server-side copy.
    Failed(oio::Copier),
}

/// The outcome of one [`advance`] step.
enum Step {
    /// The copy made progress by this many bytes and can be advanced again.
    Progress(oio::Copier, usize),
    /// The copy completed and the inner copier has been closed.
    Closed(Metadata),
    /// Advancing or closing the copy failed.
    ///
    /// The copier is kept so the caller can still abort a partially created
    /// server-side copy.
    Failed(oio::Copier, Error),
}

/// Drive one step of `copier`: advance the copy and close it once it completes.
async fn advance(mut copier: oio::Copier) -> Step {
    match copier.next_dyn().await {
        Ok(Some(n)) => Step::Progress(copier, n),
        Ok(None) => match copier.close_dyn().await {
            Ok(metadata) => Step::Closed(metadata),
            Err(err) => Step::Failed(copier, err),
        },
        Err(err) => Step::Failed(copier, err),
    }
}

impl fmt::Debug for Copier {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let state = match &self.state {
            State::Idle(_) => "idle",
            State::InFlight(_) => "in_flight",
            State::Closed(_) => "closed",
            State::Aborted => "aborted",
            State::Failed(_) => "failed",
        };

        f.debug_struct("Copier").field("state", &state).finish()
    }
}

/// # Safety
///
/// Copier will only be accessed by `&mut Self`.
unsafe impl Sync for Copier {}

impl Copier {
    /// Create a new copier.
    pub(crate) fn create(
        ctx: OperationContext,
        srv: Servicer,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> Result<Self> {
        let copier = srv.copy(&ctx, from, to, args)?;

        Ok(Self {
            state: State::Idle(copier),
        })
    }

    /// Advance the copy operation by one step.
    ///
    /// Returns `None` once the copy completed or the copier became terminal.
    pub async fn next(&mut self) -> Result<Option<usize>> {
        match &self.state {
            // Terminal copiers never drive the inner copier again.
            State::Closed(_) | State::Aborted | State::Failed(_) => Ok(None),
            // A step started by a canceled stream poll is awaited instead of
            // dropped, so the progress it already made is not lost.
            State::Idle(_) | State::InFlight(_) => self.step().await,
        }
    }

    /// Abort the pending copy operation.
    ///
    /// Aborting a completed copier is a no-op. After a successful abort, later
    /// [`Copier::next`] calls and stream polls return `None`, and
    /// [`Copier::close`] fails.
    pub async fn abort(&mut self) -> Result<()> {
        match &self.state {
            // The copy already completed or was aborted, there is nothing left
            // to abort.
            State::Closed(_) | State::Aborted => return Ok(()),
            State::Idle(_) | State::InFlight(_) | State::Failed(_) => {}
        }

        if matches!(self.state, State::InFlight(_)) {
            // Await the in-flight step before aborting: the step owns the inner
            // copier, so aborting without it would silently do nothing. The
            // step error is not returned here because `abort` reports whether
            // the server-side copy was cleaned up, and callers retry `abort` on
            // its own error.
            let _ = self.step().await;
        }

        // A failed close keeps the copier, so both states can still hold a copy
        // that needs to be aborted.
        let copier = match &mut self.state {
            State::Idle(copier) | State::Failed(copier) => copier,
            // The step completed the copy, so there is nothing left to abort.
            _ => return Ok(()),
        };

        // Keep the copier when the abort fails: the inner copier is retryable,
        // so the next `abort` call can clean up the same server-side copy.
        copier.abort_dyn().await?;
        self.state = State::Aborted;
        Ok(())
    }

    /// Close the copier and return metadata from the server-side completion response.
    ///
    /// This drives the copy to completion when it has not finished yet.
    pub async fn close(&mut self) -> Result<Metadata> {
        match &self.state {
            State::Closed(metadata) => return Ok(metadata.clone()),
            State::Aborted => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "copier has already been aborted and can't be closed",
                ));
            }
            State::Failed(_) => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "copier has already failed and can't be closed",
                ));
            }
            State::Idle(_) | State::InFlight(_) => {}
        }

        while self.next().await?.is_some() {}

        match &self.state {
            State::Closed(metadata) => Ok(metadata.clone()),
            _ => Ok(MetadataBuilder::unknown().build()),
        }
    }

    /// Run one step of the copy and record its outcome.
    ///
    /// The step lives in the state before it is awaited, so canceling the
    /// caller's future never drops progress the inner copier already made.
    async fn step(&mut self) -> Result<Option<usize>> {
        self.start_step();

        // Terminal copiers have no step left to run.
        let State::InFlight(fut) = &mut self.state else {
            return Ok(None);
        };
        let step = fut.await;

        self.apply_step(step)
    }

    /// Start a step unless one is in flight or the copier is terminal.
    fn start_step(&mut self) {
        // The step takes ownership of the inner copier, so the state has to move
        // out before the next state can be built from it. `State::Aborted` only
        // fills the slot during that move, and no await point is crossed here.
        self.state = match std::mem::replace(&mut self.state, State::Aborted) {
            State::Idle(copier) => State::InFlight(Box::pin(advance(copier))),
            // An in-flight step is resumed, and terminal copiers have no step
            // left to start.
            state => state,
        };
    }

    /// Record the outcome of one step and transition to the matching state.
    fn apply_step(&mut self, step: Step) -> Result<Option<usize>> {
        match step {
            Step::Progress(copier, n) => {
                self.state = State::Idle(copier);
                Ok(Some(n))
            }
            Step::Closed(metadata) => {
                self.state = State::Closed(metadata);
                Ok(None)
            }
            Step::Failed(copier, err) => {
                self.state = State::Failed(copier);
                Err(err)
            }
        }
    }
}

impl Stream for Copier {
    type Item = Result<usize>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.start_step();

        // Terminal copiers have no step left to run.
        let State::InFlight(fut) = &mut this.state else {
            return Poll::Ready(None);
        };
        let step = ready!(fut.as_mut().poll(cx));

        match this.apply_step(step) {
            Ok(Some(n)) => Poll::Ready(Some(Ok(n))),
            Ok(None) => Poll::Ready(None),
            Err(err) => Poll::Ready(Some(Err(err))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use futures::StreamExt;
    use futures::TryStreamExt;
    use futures::poll;

    use super::*;

    /// Content length returned by [`MockCopy::close`].
    const CLOSE_CONTENT_LENGTH: u64 = 42;

    /// Call counters shared with the mock copier, so tests can assert how often
    /// each inner operation ran.
    #[derive(Default)]
    struct MockCalls {
        next: AtomicUsize,
        close: AtomicUsize,
        abort: AtomicUsize,
    }

    impl MockCalls {
        fn next(&self) -> usize {
            self.next.load(Ordering::Relaxed)
        }

        fn close(&self) -> usize {
            self.close.load(Ordering::Relaxed)
        }

        fn abort(&self) -> usize {
            self.abort.load(Ordering::Relaxed)
        }
    }

    struct MockCopy {
        /// Results returned by `next`, in order. A missing entry completes the copy.
        steps: VecDeque<Result<Option<usize>>>,
        /// Whether the first `next` call returns `Poll::Pending` once.
        pending_once: bool,
        /// Whether `close` fails instead of returning metadata.
        close_fails: bool,
        calls: Arc<MockCalls>,
    }

    impl MockCopy {
        fn new(
            calls: &Arc<MockCalls>,
            steps: impl IntoIterator<Item = Result<Option<usize>>>,
        ) -> Self {
            Self {
                steps: steps.into_iter().collect(),
                pending_once: false,
                close_fails: false,
                calls: calls.clone(),
            }
        }
    }

    impl oio::Copy for MockCopy {
        async fn next(&mut self) -> Result<Option<usize>> {
            self.calls.next.fetch_add(1, Ordering::Relaxed);

            if self.pending_once {
                self.pending_once = false;
                yield_once().await;
            }

            self.steps.pop_front().unwrap_or(Ok(None))
        }

        async fn close(&mut self) -> Result<Metadata> {
            self.calls.close.fetch_add(1, Ordering::Relaxed);

            if self.close_fails {
                return Err(Error::new(ErrorKind::Unexpected, "mock close failed"));
            }

            let mut builder = MetadataBuilder::unknown();
            builder.set_file(CLOSE_CONTENT_LENGTH);
            Ok(builder.build())
        }

        async fn abort(&mut self) -> Result<()> {
            self.calls.abort.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    /// Return `Poll::Pending` on the first poll and `Poll::Ready` afterwards.
    ///
    /// Lets a test leave a `next` step in flight and cancel the stream poll.
    async fn yield_once() {
        let mut yielded = false;

        std::future::poll_fn(move |cx| {
            if yielded {
                return Poll::Ready(());
            }

            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        })
        .await;
    }

    fn mock_copier(mock: MockCopy) -> Copier {
        Copier {
            state: State::Idle(Box::new(mock)),
        }
    }

    #[tokio::test]
    async fn test_copier_stream_closes_inner_copier_once() {
        let calls = Arc::new(MockCalls::default());
        let copier = mock_copier(MockCopy::new(&calls, [Ok(Some(4)), Ok(Some(6)), Ok(None)]));

        // Driving the copier as a stream must complete the copy, not only end
        // the stream: `close_dyn` is the step that finalizes it.
        let sizes = copier.try_collect::<Vec<_>>().await.unwrap();

        assert_eq!(sizes, vec![4, 6]);
        assert_eq!(calls.next(), 3);
        assert_eq!(calls.close(), 1);
    }

    #[tokio::test]
    async fn test_copier_stream_repoll_after_close() {
        let calls = Arc::new(MockCalls::default());
        let mut copier = mock_copier(MockCopy::new(&calls, [Ok(Some(4)), Ok(None)]));

        let sizes = (&mut copier).try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(sizes, vec![4]);
        assert_eq!(calls.next(), 2);
        assert_eq!(calls.close(), 1);
        assert!(matches!(copier.state, State::Closed(_)));

        // A completed stream stays completed: it returns `None` without driving
        // the inner copier or closing it again.
        assert!(matches!(
            poll!(StreamExt::next(&mut copier)),
            Poll::Ready(None)
        ));
        assert!(copier.next().await.unwrap().is_none());
        assert_eq!(calls.next(), 2);
        assert_eq!(calls.close(), 1);

        // `close` reports the metadata returned by `close_dyn`.
        assert_eq!(
            copier.close().await.unwrap().content_length(),
            CLOSE_CONTENT_LENGTH
        );
        assert_eq!(calls.close(), 1);
    }

    #[tokio::test]
    async fn test_copier_stream_surfaces_close_error() {
        let calls = Arc::new(MockCalls::default());
        let mut copier = mock_copier(MockCopy {
            close_fails: true,
            ..MockCopy::new(&calls, [Ok(None)])
        });

        let err = (&mut copier).try_collect::<Vec<_>>().await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert_eq!(calls.next(), 1);
        assert_eq!(calls.close(), 1);
        assert!(matches!(copier.state, State::Failed(_)));

        // A failed close terminates the copier instead of retrying it.
        assert!(copier.next().await.unwrap().is_none());
        assert!(matches!(
            poll!(StreamExt::next(&mut copier)),
            Poll::Ready(None)
        ));
        assert!(copier.close().await.is_err());
        assert_eq!(calls.next(), 1);
        assert_eq!(calls.close(), 1);
    }

    #[tokio::test]
    async fn test_copier_abort_after_cancelled_poll() {
        let calls = Arc::new(MockCalls::default());
        let mut copier = mock_copier(MockCopy {
            pending_once: true,
            ..MockCopy::new(&calls, [Ok(Some(4))])
        });

        // Cancel the first poll while the `next` step is in flight.
        assert!(poll!(StreamExt::next(&mut copier)).is_pending());
        assert!(matches!(copier.state, State::InFlight(_)));

        // `abort` must reach the inner copier instead of returning early
        // because the in-flight step owns it.
        copier.abort().await.unwrap();
        assert_eq!(calls.abort(), 1);
        assert!(matches!(copier.state, State::Aborted));

        // The aborted copier is terminal.
        assert!(copier.next().await.unwrap().is_none());
        assert!(matches!(
            poll!(StreamExt::next(&mut copier)),
            Poll::Ready(None)
        ));
        assert!(copier.close().await.is_err());
        assert_eq!(calls.next(), 1);
        assert_eq!(calls.close(), 0);

        // Aborting again is a no-op.
        copier.abort().await.unwrap();
        assert_eq!(calls.abort(), 1);
    }

    #[tokio::test]
    async fn test_copier_abort_after_failed_close() {
        let calls = Arc::new(MockCalls::default());
        let mut copier = mock_copier(MockCopy {
            close_fails: true,
            ..MockCopy::new(&calls, [Ok(None)])
        });

        assert!(copier.close().await.is_err());
        // The failed close keeps the inner copier so it can still be aborted.
        assert!(matches!(copier.state, State::Failed(_)));

        copier.abort().await.unwrap();
        assert_eq!(calls.abort(), 1);
        assert!(matches!(copier.state, State::Aborted));
    }

    #[tokio::test]
    async fn test_copier_close_after_cancelled_poll() {
        let calls = Arc::new(MockCalls::default());
        let mut copier = mock_copier(MockCopy {
            pending_once: true,
            ..MockCopy::new(&calls, [Ok(Some(4)), Ok(None)])
        });

        assert!(poll!(StreamExt::next(&mut copier)).is_pending());

        // `close` resumes the in-flight step, drives the copy to completion and
        // closes the inner copier.
        let metadata = copier.close().await.unwrap();
        assert_eq!(metadata.content_length(), CLOSE_CONTENT_LENGTH);
        assert_eq!(calls.next(), 2);
        assert_eq!(calls.close(), 1);
    }

    #[tokio::test]
    async fn test_copier_next_after_cancelled_poll() {
        let calls = Arc::new(MockCalls::default());
        let mut copier = mock_copier(MockCopy {
            pending_once: true,
            ..MockCopy::new(&calls, [Ok(Some(4)), Ok(None)])
        });

        assert!(poll!(StreamExt::next(&mut copier)).is_pending());

        // `next` resumes the in-flight step instead of dropping its progress.
        assert_eq!(copier.next().await.unwrap(), Some(4));
        assert_eq!(copier.next().await.unwrap(), None);
        assert_eq!(calls.next(), 2);
        assert_eq!(calls.close(), 1);
    }

    #[tokio::test]
    async fn test_copier_next_cancelled_mid_step() {
        let calls = Arc::new(MockCalls::default());
        let mut copier = mock_copier(MockCopy {
            pending_once: true,
            ..MockCopy::new(&calls, [Ok(Some(4)), Ok(None)])
        });

        {
            // Cancel `next` itself while its step is in flight.
            let mut fut = std::pin::pin!(copier.next());
            assert!(poll!(&mut fut).is_pending());
        }

        // Canceled `next` calls keep the in-flight step resumable.
        assert_eq!(copier.next().await.unwrap(), Some(4));
        assert_eq!(copier.next().await.unwrap(), None);
        assert_eq!(calls.next(), 2);
        assert_eq!(calls.close(), 1);
    }
}
