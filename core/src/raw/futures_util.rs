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

use futures::poll;
use futures::stream::FuturesOrdered;
use futures::FutureExt;
use futures::StreamExt;

use crate::*;

/// BoxedFuture is the type alias of [`futures::future::BoxFuture`].
///
/// We will switch to [`futures::future::LocalBoxFuture`] on wasm32 target.
#[cfg(not(target_arch = "wasm32"))]
pub type BoxedFuture<'a, T> = futures::future::BoxFuture<'a, T>;
#[cfg(target_arch = "wasm32")]
pub type BoxedFuture<'a, T> = futures::future::LocalBoxFuture<'a, T>;

/// BoxedStaticFuture is the type alias of [`futures::future::BoxFuture`].
///
/// We will switch to [`futures::future::LocalBoxFuture`] on wasm32 target.
#[cfg(not(target_arch = "wasm32"))]
pub type BoxedStaticFuture<T> = futures::future::BoxFuture<'static, T>;
#[cfg(target_arch = "wasm32")]
pub type BoxedStaticFuture<T> = futures::future::LocalBoxFuture<'static, T>;

/// MaybeSend is a marker to determine whether a type is `Send` or not.
/// We use this trait to wrap the `Send` requirement for wasm32 target.
///
/// # Safety
///
/// [`MaybeSend`] is equivalent to `Send` on non-wasm32 target.
/// And it's empty trait on wasm32 target to indicate that a type is not `Send`.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSend: Send {}
#[cfg(target_arch = "wasm32")]
pub trait MaybeSend {}

#[cfg(not(target_arch = "wasm32"))]
impl<T: Send> MaybeSend for T {}
#[cfg(target_arch = "wasm32")]
impl<T> MaybeSend for T {}

/// ConcurrentTasks is used to execute tasks concurrently.
///
/// ConcurrentTasks has two generic types:
///
/// - `I` represents the input type of the task.
/// - `O` represents the output type of the task.
pub struct ConcurrentTasks<I, O> {
    /// The executor to execute the tasks.
    ///
    /// If user doesn't provide an executor, the tasks will be executed with the default executor.
    executor: Executor,
    /// The factory to create the task.
    ///
    /// Caller of ConcurrentTasks must provides a factory to create the task for executing.
    ///
    /// The factory must accept an input and return a future that resolves to a tuple of input and
    /// output result. If the given result is error, the error will be returned to users and the
    /// task will be retried.
    factory: fn(I) -> BoxedStaticFuture<(I, Result<O>)>,

    /// `tasks` holds the ongoing tasks.
    ///
    /// Please keep in mind that all tasks are running in the background by `Executor`. We only need
    /// to poll the tasks to see if they are ready.
    ///
    /// Dropping task without `await` it will cancel the task.
    tasks: VecDeque<Task<(I, Result<O>)>>,
    /// `results` stores the successful results.
    results: VecDeque<O>,

    /// hitting the last unrecoverable error.
    ///
    /// If concurrent tasks hit an unrecoverable error, it will stop executing new tasks and return
    /// an unrecoverable error to users.
    errored: bool,
}

impl<I: Send + 'static, O: Send + 'static> ConcurrentTasks<I, O> {
    /// Create a new concurrent tasks with given executor, concurrent and factory.
    ///
    /// The factory is a function pointer that shouldn't capture any context.
    pub fn new(
        executor: Executor,
        concurrent: usize,
        factory: fn(I) -> BoxedStaticFuture<(I, Result<O>)>,
    ) -> Self {
        Self {
            executor,
            factory,

            tasks: VecDeque::with_capacity(concurrent),
            results: VecDeque::with_capacity(concurrent),
            errored: false,
        }
    }

    /// Return true if the tasks are running concurrently.
    #[inline]
    fn is_concurrent(&self) -> bool {
        self.tasks.capacity() > 1
    }

    /// Clear all tasks and results.
    ///
    /// All ongoing tasks will be canceled.
    pub fn clear(&mut self) {
        self.tasks.clear();
        self.results.clear();
    }

    /// Check if there are remaining space to push new tasks.
    #[inline]
    pub fn has_remaining(&self) -> bool {
        self.tasks.len() < self.tasks.capacity()
    }

    /// Chunk if there are remaining results to fetch.
    #[inline]
    pub fn has_result(&self) -> bool {
        !self.results.is_empty()
    }

    /// Execute the task with given input.
    ///
    /// - Execute the task in the current thread if is not concurrent.
    /// - Execute the task in the background if there are available slots.
    /// - Await the first task in the queue if there is no available slots.
    pub async fn execute(&mut self, input: I) -> Result<()> {
        if self.errored {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "concurrent tasks met an unrecoverable error",
            ));
        }

        // Short path for non-concurrent case.
        if !self.is_concurrent() {
            let (_, o) = (self.factory)(input).await;
            return match o {
                Ok(o) => {
                    self.results.push_back(o);
                    Ok(())
                }
                // We don't need to rebuild the future if it's not concurrent.
                Err(err) => Err(err),
            };
        }

        loop {
            // Try poll once to see if there is any ready task.
            if let Some(task) = self.tasks.front_mut() {
                if let Poll::Ready((i, o)) = poll!(task) {
                    match o {
                        Ok(o) => {
                            let _ = self.tasks.pop_front();
                            self.results.push_back(o)
                        }
                        Err(err) => {
                            // Retry this task if the error is temporary
                            if err.is_temporary() {
                                self.tasks
                                    .front_mut()
                                    .expect("tasks must have at least one task")
                                    .replace(self.executor.execute((self.factory)(i)));
                            } else {
                                self.clear();
                                self.errored = true;
                            }
                            return Err(err);
                        }
                    }
                }
            }

            // Try to push new task if there are available space.
            if self.tasks.len() < self.tasks.capacity() {
                self.tasks
                    .push_back(self.executor.execute((self.factory)(input)));
                return Ok(());
            }

            // Wait for the next task to be ready.
            let task = self
                .tasks
                .front_mut()
                .expect("tasks must have at least one task");
            let (i, o) = task.await;
            match o {
                Ok(o) => {
                    let _ = self.tasks.pop_front();
                    self.results.push_back(o);
                    continue;
                }
                Err(err) => {
                    // Retry this task if the error is temporary
                    if err.is_temporary() {
                        self.tasks
                            .front_mut()
                            .expect("tasks must have at least one task")
                            .replace(self.executor.execute((self.factory)(i)));
                    } else {
                        self.clear();
                        self.errored = true;
                    }
                    return Err(err);
                }
            }
        }
    }

    /// Fetch the successful result from the result queue.
    pub async fn next(&mut self) -> Option<Result<O>> {
        if self.errored {
            return Some(Err(Error::new(
                ErrorKind::Unexpected,
                "concurrent tasks met an unrecoverable error",
            )));
        }

        if let Some(result) = self.results.pop_front() {
            return Some(Ok(result));
        }

        if let Some(task) = self.tasks.front_mut() {
            let (i, o) = task.await;
            return match o {
                Ok(o) => {
                    let _ = self.tasks.pop_front();
                    Some(Ok(o))
                }
                Err(err) => {
                    // Retry this task if the error is temporary
                    if err.is_temporary() {
                        self.tasks
                            .front_mut()
                            .expect("tasks must have at least one task")
                            .replace(self.executor.execute((self.factory)(i)));
                    } else {
                        self.clear();
                        self.errored = true;
                    }
                    Some(Err(err))
                }
            };
        }

        None
    }
}

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

    /// Push new future into the end of queue.
    pub fn push_back(&mut self, f: F) {
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

    /// Push new future into the start of queue, this task will be exactly the next to poll.
    pub fn push_front(&mut self, f: F) {
        debug_assert!(
            self.has_remaining(),
            "concurrent futures must have remaining space"
        );

        match &mut self.tasks {
            Tasks::Once(fut) => {
                *fut = Some(f);
            }
            Tasks::Small(v) => v.push_front(TaskResult::Polling(f)),
            Tasks::Large(v) => v.push_front(f),
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
    use tokio::time::sleep;

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
                self.tasks.push_back(Box::pin(fut));
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

    #[tokio::test]
    async fn test_concurrent_tasks() {
        let executor = Executor::new();

        let mut tasks = ConcurrentTasks::new(executor, 16, |(i, dur)| {
            Box::pin(async move {
                sleep(dur).await;

                // 5% rate to fail.
                if rand::thread_rng().gen_range(0..100) > 90 {
                    return (
                        (i, dur),
                        Err(Error::new(ErrorKind::Unexpected, "I'm lucky").set_temporary()),
                    );
                }
                ((i, dur), Ok(i))
            })
        });

        let mut ans = vec![];

        for i in 0..10240 {
            // Sleep up to 10ms
            let dur = Duration::from_millis(rand::thread_rng().gen_range(0..10));
            loop {
                let res = tasks.execute((i, dur)).await;
                if res.is_ok() {
                    break;
                }
            }
        }

        loop {
            match tasks.next().await.transpose() {
                Ok(Some(i)) => ans.push(i),
                Ok(None) => break,
                Err(_) => continue,
            }
        }

        assert_eq!(ans, (0..10240).collect::<Vec<_>>())
    }
}
