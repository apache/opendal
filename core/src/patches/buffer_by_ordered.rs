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

use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use futures::future::Future;
use futures::stream::Fuse;
use futures::stream::FuturesOrdered;
use futures::stream::Stream;
use futures::stream::StreamExt;

use pin_project_lite::pin_project;

pin_project! {
    /// Stream for the [`buffer_by_ordered`] method.
    ///
    /// [`buffer_by_ordered`]: crate::StreamExt::buffer_by_ordered
    #[must_use = "streams do nothing unless polled"]
    pub struct BufferByOrdered<St, F>
    where
        St: Stream<Item = (F, usize)>,
        F: Future,
    {
        #[pin]
        stream: Fuse<St>,
        in_progress_queue: FuturesOrdered<SizedFuture<F>>,
        ready_queue: VecDeque<(F::Output, usize)>,
        max_size: usize,
        current_size: usize,
    }
}

impl<St, F> fmt::Debug for BufferByOrdered<St, F>
where
    St: Stream<Item = (F, usize)> + fmt::Debug,
    F: Future,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("BufferByOrdered")
            .field("stream", &self.stream)
            .field("in_progress_queue", &self.in_progress_queue)
            .field("max_size", &self.max_size)
            .field("current_size", &self.current_size)
            .finish()
    }
}

impl<St, F> BufferByOrdered<St, F>
where
    St: Stream<Item = (F, usize)>,
    F: Future,
{
    pub(crate) fn new(stream: St, max_size: usize) -> Self {
        Self {
            stream: stream.fuse(),
            in_progress_queue: FuturesOrdered::new(),
            ready_queue: VecDeque::new(),
            max_size,
            current_size: 0,
        }
    }
}

impl<St, F> Stream for BufferByOrdered<St, F>
where
    St: Stream<Item = (F, usize)>,
    F: Future,
{
    type Item = F::Output;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // First up, try to spawn off as many futures as possible by filling up
        // our queue of futures.
        while this.current_size < this.max_size {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some((future, size))) => {
                    *this.current_size += size;
                    this.in_progress_queue
                        .push_back(SizedFuture { future, size });
                }
                Poll::Ready(None) => break,
                Poll::Pending => break,
            }
        }

        // Try to poll all ready futures in the in_progress_queue.
        loop {
            match this.in_progress_queue.poll_next_unpin(cx) {
                Poll::Ready(Some(output)) => {
                    this.ready_queue.push_back(output);
                }
                Poll::Ready(None) => break,
                Poll::Pending => break,
            }
        }

        if let Some((output, size)) = this.ready_queue.pop_front() {
            // If we have any ready outputs, return the first one.
            *this.current_size -= size;
            Poll::Ready(Some(output))
        } else if this.stream.is_done() && this.in_progress_queue.is_empty() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let queue_len = self.in_progress_queue.len() + self.ready_queue.len();
        let (lower, upper) = self.stream.size_hint();
        let lower = lower.saturating_add(queue_len);
        let upper = match upper {
            Some(x) => x.checked_add(queue_len),
            None => None,
        };
        (lower, upper)
    }
}

pin_project! {
    struct SizedFuture<F> {
        #[pin]
        future: F,
        size: usize,
    }
}

impl<F: Future> Future for SizedFuture<F> {
    type Output = (F::Output, usize);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.poll(cx) {
            Poll::Ready(output) => Poll::Ready((output, *this.size)),
            Poll::Pending => Poll::Pending,
        }
    }
}
