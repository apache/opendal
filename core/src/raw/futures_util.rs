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

use std::{
    collections::VecDeque,
    sync::atomic::Ordering,
    sync::{atomic::AtomicUsize, Arc},
};

use futures::FutureExt;

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
///
/// # Implementation Notes
///
/// The code patterns below are intentional; please do not modify them unless you fully understand these notes.
///
/// ```skip
///  let (i, o) = self
///     .tasks
///     .front_mut()                                        // Use `front_mut` instead of `pop_front`
///     .expect("tasks must be available")
///     .await;
/// ...
/// match o {
///     Ok(o) => {
///         let _ = self.tasks.pop_front();                 // `pop_front` after got `Ok(o)`
///         self.results.push_back(o)
///     }
///     Err(err) => {
///         if err.is_temporary() {
///             let task = self.create_task(i);
///             self.tasks
///                 .front_mut()
///                 .expect("tasks must be available")
///                 .replace(task)                          // Use replace here to instead of `push_front`
///         } else {
///             self.clear();
///             self.errored = true;
///         }
///         return Err(err);
///     }
/// }
/// ```
///
/// Please keep in mind that there is no guarantee the task will be `await`ed until completion. It's possible
/// the task may be dropped before it resolves. Therefore, we should keep the `Task` in the `tasks` queue until
/// it is resolved.
///
/// For example, users may have a timeout for the task, and the task will be dropped if it exceeds the timeout.
/// If we `pop_front` the task before it resolves, the task will be canceled and the result will be lost.
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

    /// The maximum number of concurrent tasks.
    concurrent: usize,
    /// Tracks the number of tasks that have finished execution but have not yet been collected.
    /// This count is subtracted from the total concurrency capacity, ensuring that the system
    /// always schedules new tasks to maintain the user's desired concurrency level.
    ///
    /// Example: If `concurrency = 10` and `completed_but_unretrieved = 3`,
    ///          the system can still spawn 7 new tasks (since 3 slots are "logically occupied"
    ///          by uncollected results).
    completed_but_unretrieved: Arc<AtomicUsize>,
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
            concurrent,
            completed_but_unretrieved: Arc::default(),
            errored: false,
        }
    }

    /// Return true if the tasks are running concurrently.
    #[inline]
    fn is_concurrent(&self) -> bool {
        self.concurrent > 1
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
        self.tasks.len() < self.concurrent + self.completed_but_unretrieved.load(Ordering::Relaxed)
    }

    /// Chunk if there are remaining results to fetch.
    #[inline]
    pub fn has_result(&self) -> bool {
        !self.results.is_empty()
    }

    /// Create a task with given input.
    pub fn create_task(&self, input: I) -> Task<(I, Result<O>)> {
        let completed = self.completed_but_unretrieved.clone();

        let fut = (self.factory)(input).inspect(move |_| {
            completed.fetch_add(1, Ordering::Relaxed);
        });

        self.executor.execute(fut)
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

        if !self.has_remaining() {
            let (i, o) = self
                .tasks
                .front_mut()
                .expect("tasks must be available")
                .await;
            self.completed_but_unretrieved
                .fetch_sub(1, Ordering::Relaxed);
            match o {
                Ok(o) => {
                    let _ = self.tasks.pop_front();
                    self.results.push_back(o)
                }
                Err(err) => {
                    // Retry this task if the error is temporary
                    if err.is_temporary() {
                        let task = self.create_task(i);
                        self.tasks
                            .front_mut()
                            .expect("tasks must be available")
                            .replace(task)
                    } else {
                        self.clear();
                        self.errored = true;
                    }
                    return Err(err);
                }
            }
        }

        self.tasks.push_back(self.create_task(input));
        Ok(())
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
            self.completed_but_unretrieved
                .fetch_sub(1, Ordering::Relaxed);
            return match o {
                Ok(o) => {
                    let _ = self.tasks.pop_front();
                    Some(Ok(o))
                }
                Err(err) => {
                    // Retry this task if the error is temporary
                    if err.is_temporary() {
                        let task = self.create_task(i);
                        self.tasks
                            .front_mut()
                            .expect("tasks must be available")
                            .replace(task)
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rand::Rng;
    use tokio::time::sleep;

    use super::*;
    use pretty_assertions::assert_eq;

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
