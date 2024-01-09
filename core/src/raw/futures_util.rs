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
use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use futures::stream::FuturesOrdered;
use futures::FutureExt;
use futures::StreamExt;

/// MaybeSendFuture is a wrapper trait allow us to use `Send` or not depends on the target.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSendFuture<T>: Future<Output = T> + Send {}

#[cfg(not(target_arch = "wasm32"))]
impl<T, F> MaybeSendFuture<T> for F where F: Future<Output = T> + Send {}

/// MaybeSendFuture is a wrapper trait allow us to use `Send` or not depends on the target.
#[cfg(target_arch = "wasm32")]
pub trait MaybeSendFuture<T>: Future<Output = T> {}

#[cfg(target_arch = "wasm32")]
impl<T, F> MaybeSendFuture<T> for F where F: Future<Output = T> {}

/// BoxedFuture is the type alias of [`futures::future::BoxFuture`].
///
/// We will switch to [`futures::future::LocalBoxFuture`] on wasm32 target.
#[cfg(not(target_arch = "wasm32"))]
pub type BoxedFuture<T> = futures::future::BoxFuture<'static, T>;
#[cfg(target_arch = "wasm32")]
pub type BoxedFuture<T> = futures::future::LocalBoxFuture<'static, T>;

/// CONCURRENT_LARGE_THRESHOLD is the threshold to determine whether to use
/// [`FuturesOrdered`] or not.
///
/// The value of `8` is picked by random, no strict benchmark is done.
/// Please raise an issue if you found the value is not good enough or you want to configure
/// this value at runtime.
const CONCURRENT_LARGE_THRESHOLD: usize = 8;

/// ConcurrentFutures is a stream that can hold a stream of concurrent futures.
///
/// - the order of the futures is the same.
/// - the number of concurrent futures is limited by concurrent.
/// - optimized for small number of concurrent futures.
/// - zero cost for non-concurrent futures cases (concurrent == 1).
pub struct ConcurrentFutures<F: Future + Unpin> {
    tasks: Tasks<F>,
    concurrent: usize,
}

/// Tasks is used to hold the entire task queue.
enum Tasks<F: Future + Unpin> {
    /// The special case for concurrent == 1.
    ///
    /// It works exactly the same like `Option<Fut>` in a struct.
    Once(Option<F>),
    /// The special cases for concurrent is small.
    ///
    /// At this case, the cost to loop poll is lower than using `FuturesOrdered`.
    ///
    /// We will replace the future by `TaskResult::Ready` once it's ready to avoid consume it again.
    Small(VecDeque<TaskResult<F>>),
    /// The general cases for large concurrent.
    ///
    /// We use `FuturesOrdered` to avoid huge amount of poll on futures.
    Large(FuturesOrdered<F>),
}

impl<F: Future + Unpin> Unpin for Tasks<F> {}

enum TaskResult<F: Future + Unpin> {
    Polling(F),
    Ready(F::Output),
}

impl<F> ConcurrentFutures<F>
where
    F: Future + Unpin + 'static,
{
    /// Create a new ConcurrentFutures by specifying the number of concurrent futures.
    pub fn new(concurrent: usize) -> Self {
        if (0..2).contains(&concurrent) {
            Self {
                tasks: Tasks::Once(None),
                concurrent,
            }
        } else if (2..=CONCURRENT_LARGE_THRESHOLD).contains(&concurrent) {
            Self {
                tasks: Tasks::Small(VecDeque::with_capacity(concurrent)),
                concurrent,
            }
        } else {
            Self {
                tasks: Tasks::Large(FuturesOrdered::new()),
                concurrent,
            }
        }
    }

    /// Drop all tasks.
    pub fn clear(&mut self) {
        match &mut self.tasks {
            Tasks::Once(fut) => *fut = None,
            Tasks::Small(tasks) => tasks.clear(),
            Tasks::Large(tasks) => *tasks = FuturesOrdered::new(),
        }
    }

    /// Return the length of current concurrent futures (both ongoing and ready).
    pub fn len(&self) -> usize {
        match &self.tasks {
            Tasks::Once(fut) => fut.is_some() as usize,
            Tasks::Small(v) => v.len(),
            Tasks::Large(v) => v.len(),
        }
    }

    /// Return true if there is no futures in the queue.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the number of remaining space to push new futures.
    pub fn remaining(&self) -> usize {
        self.concurrent - self.len()
    }

    /// Return true if there is remaining space to push new futures.
    pub fn has_remaining(&self) -> bool {
        self.remaining() > 0
    }

    /// Push new future into the queue.
    pub fn push(&mut self, f: F) {
        debug_assert!(
            self.has_remaining(),
            "concurrent futures must have remaining space"
        );

        match &mut self.tasks {
            Tasks::Once(fut) => {
                *fut = Some(f);
            }
            Tasks::Small(v) => v.push_back(TaskResult::Polling(f)),
            Tasks::Large(v) => v.push_back(f),
        }
    }
}

impl<F> futures::Stream for ConcurrentFutures<F>
where
    F: Future + Unpin + 'static,
{
    type Item = F::Output;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.get_mut().tasks {
            Tasks::Once(fut) => match fut {
                Some(x) => x.poll_unpin(cx).map(|v| {
                    *fut = None;
                    Some(v)
                }),
                None => Poll::Ready(None),
            },
            Tasks::Small(v) => {
                // Poll all tasks together.
                for task in v.iter_mut() {
                    if let TaskResult::Polling(f) = task {
                        match f.poll_unpin(cx) {
                            Poll::Pending => {}
                            Poll::Ready(res) => {
                                // Replace with ready value if this future has been resolved.
                                *task = TaskResult::Ready(res);
                            }
                        }
                    }
                }

                // Pick the first one to check.
                match v.front_mut() {
                    // Return pending if the first one is still polling.
                    Some(TaskResult::Polling(_)) => Poll::Pending,
                    Some(TaskResult::Ready(_)) => {
                        let res = v.pop_front().unwrap();
                        match res {
                            TaskResult::Polling(_) => unreachable!(),
                            TaskResult::Ready(res) => Poll::Ready(Some(res)),
                        }
                    }
                    None => Poll::Ready(None),
                }
            }
            Tasks::Large(v) => v.poll_next_unpin(cx),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::task::ready;
    use std::time::Duration;

    use futures::future::BoxFuture;
    use futures::Stream;
    use rand::Rng;

    use super::*;

    struct Lister {
        size: usize,
        idx: usize,
        concurrent: usize,
        tasks: ConcurrentFutures<BoxFuture<'static, usize>>,
    }

    impl Stream for Lister {
        type Item = usize;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            // Randomly sleep for a while, simulate some io operations that up to 100 microseconds.
            let timeout = Duration::from_micros(rand::thread_rng().gen_range(0..100));
            let idx = self.idx;
            if self.tasks.len() < self.concurrent && self.idx < self.size {
                let fut = async move {
                    tokio::time::sleep(timeout).await;
                    idx
                };
                self.idx += 1;
                self.tasks.push(Box::pin(fut));
            }

            if let Some(v) = ready!(self.tasks.poll_next_unpin(cx)) {
                Poll::Ready(Some(v))
            } else {
                Poll::Ready(None)
            }
        }
    }

    #[tokio::test]
    async fn test_concurrent_futures() {
        let cases = vec![
            ("once", 1),
            ("small", CONCURRENT_LARGE_THRESHOLD - 1),
            ("large", CONCURRENT_LARGE_THRESHOLD + 1),
        ];

        for (name, concurrent) in cases {
            let lister = Lister {
                size: 1000,
                idx: 0,
                concurrent,
                tasks: ConcurrentFutures::new(concurrent),
            };
            let expected: Vec<usize> = (0..1000).collect();
            let result: Vec<usize> = lister.collect().await;

            assert_eq!(expected, result, "concurrent futures failed: {}", name);
        }
    }
}
