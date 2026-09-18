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
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use futures::FutureExt;

use crate::*;

/// BoxedFuture is the type alias of [`futures::future::BoxFuture`].
#[cfg(not(target_arch = "wasm32"))]
pub type BoxedFuture<'a, T> = futures::future::BoxFuture<'a, T>;
#[cfg(target_arch = "wasm32")]
/// BoxedFuture is the type alias of [`futures::future::LocalBoxFuture`].
pub type BoxedFuture<'a, T> = futures::future::LocalBoxFuture<'a, T>;

/// BoxedStaticFuture is the type alias of [`futures::future::BoxFuture`].
#[cfg(not(target_arch = "wasm32"))]
pub type BoxedStaticFuture<T> = futures::future::BoxFuture<'static, T>;
#[cfg(target_arch = "wasm32")]
/// BoxedStaticFuture is the type alias of [`futures::future::LocalBoxFuture`].
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

/// MaybeSend is a marker to determine whether a type is `Send` or not.
/// We use this trait to wrap the `Send` requirement for wasm32 target.
///
/// # Safety
///
/// [`MaybeSend`] is equivalent to `Send` on non-wasm32 target.
/// And it's empty trait on wasm32 target to indicate that a type is not `Send`.
#[cfg(target_arch = "wasm32")]
pub trait MaybeSend {}

#[cfg(not(target_arch = "wasm32"))]
impl<T: Send> MaybeSend for T {}
#[cfg(target_arch = "wasm32")]
impl<T> MaybeSend for T {}

/// ConcurrentTasks executes tasks concurrently and collects outputs in submission order.
///
/// Submit inputs with [`Self::execute`] and collect outputs with [`Self::next`].
/// The queue owns the task handles and tracks their completion for concurrency control.
/// Completed failures stop new submissions until they are collected in submission order.
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
///  let result = self
///     .tasks
///     .front_mut()                                        // Use `front_mut` instead of `pop_front`
///     .expect("tasks must be available")
///     .await;
/// ...
/// match result {
///     Ok(o) => {
///         let _ = self.tasks.pop_front();                 // `pop_front` after got `Ok(o)`
///         self.results.push_back(o)
///     }
///     Err((i, err)) => {
///         if err.is_temporary() {
///             let task = self.spawn_task(i);
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
    /// Caller of ConcurrentTasks must provide a factory to create the task for executing.
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
    tasks: VecDeque<Task<Result<O, (I, Error)>>>,
    /// `results` stores the successful results.
    results: VecDeque<O>,

    /// The maximum number of concurrent tasks.
    concurrent: usize,
    /// The extra queue capacity available for completed tasks.
    prefetch: usize,
    /// Completed tasks still held in the queue can provide prefetch capacity.
    completed_but_unretrieved: Arc<AtomicUsize>,
    /// Failed tasks still retain their inputs and must be collected before submitting more work.
    failed_but_unretrieved: Arc<AtomicUsize>,
    /// hitting the last unrecoverable error.
    ///
    /// If concurrent tasks hit an unrecoverable error, it will stop executing new tasks and return
    /// an unrecoverable error to users.
    errored: bool,
}

impl<I: Send + 'static, O: Send + 'static> ConcurrentTasks<I, O> {
    /// Create a new concurrent tasks with given executor, concurrent, prefetch and factory.
    ///
    /// The factory is a function pointer that shouldn't capture any context.
    pub fn new(
        executor: Executor,
        concurrent: usize,
        prefetch: usize,
        factory: fn(I) -> BoxedStaticFuture<(I, Result<O>)>,
    ) -> Self {
        Self {
            executor,
            factory,

            tasks: VecDeque::with_capacity(concurrent),
            results: VecDeque::with_capacity(concurrent),
            concurrent,
            prefetch,
            completed_but_unretrieved: Arc::default(),
            failed_but_unretrieved: Arc::default(),
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
        // Canceled tasks may still finish on the executor. Keep their accounting
        // separate from tasks submitted after clearing the queue.
        self.completed_but_unretrieved = Arc::default();
        self.failed_but_unretrieved = Arc::default();
    }

    /// Check if there are remaining space to push new tasks.
    #[inline]
    pub fn has_remaining(&self) -> bool {
        // Observe failures before using the capacity provided by their completion.
        let completed = self.completed_but_unretrieved.load(Ordering::Acquire);
        if self.failed_but_unretrieved.load(Ordering::Relaxed) > 0 {
            return false;
        }
        // Allow up to `prefetch` completed tasks to be buffered
        self.tasks.len() < self.concurrent + completed.min(self.prefetch)
    }

    /// Chunk if there are remaining results to fetch.
    #[inline]
    pub fn has_result(&self) -> bool {
        !self.results.is_empty()
    }

    fn spawn_task(&self, input: I) -> Task<Result<O, (I, Error)>> {
        let completed = self.completed_but_unretrieved.clone();
        let failed = self.failed_but_unretrieved.clone();
        let fut = (self.factory)(input)
            // Completed tasks can remain queued while the caller produces more work.
            // Only failures need to retain their input for a retry.
            .map(|(input, result)| result.map_err(|err| (input, err)))
            .inspect(move |result| {
                if result.is_err() {
                    failed.fetch_add(1, Ordering::Relaxed);
                }
                completed.fetch_add(1, Ordering::Release);
            });

        self.executor.execute(fut)
    }

    /// Execute the task with given input.
    ///
    /// - Execute the task in the current thread if is not concurrent.
    /// - Execute the task in the background if there are available slots.
    /// - Collect tasks in submission order while the queue is full or has a completed failure.
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

        while !self.has_remaining() {
            let result = self
                .tasks
                .front_mut()
                .expect("tasks must be available")
                .await;
            self.completed_but_unretrieved
                .fetch_sub(1, Ordering::Relaxed);
            match result {
                Ok(o) => {
                    let _ = self.tasks.pop_front();
                    self.results.push_back(o)
                }
                Err((i, err)) => {
                    self.failed_but_unretrieved.fetch_sub(1, Ordering::Relaxed);
                    // Retry this task if the error is temporary
                    if err.is_temporary() {
                        let task = self.spawn_task(i);
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

        self.tasks.push_back(self.spawn_task(input));
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
            let result = task.await;
            self.completed_but_unretrieved
                .fetch_sub(1, Ordering::Relaxed);
            return match result {
                Ok(o) => {
                    let _ = self.tasks.pop_front();
                    Some(Ok(o))
                }
                Err((i, err)) => {
                    self.failed_but_unretrieved.fetch_sub(1, Ordering::Relaxed);
                    // Retry this task if the error is temporary
                    if err.is_temporary() {
                        let task = self.spawn_task(i);
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
    use std::sync::Mutex;

    use pretty_assertions::assert_eq;
    use rand::RngExt;
    use tokio::time::sleep;

    use super::*;
    use crate::raw::Duration;

    #[derive(Clone, Default)]
    struct ControlledExecutor(Arc<Mutex<VecDeque<BoxedStaticFuture<()>>>>);

    impl Execute for ControlledExecutor {
        fn execute(&self, future: BoxedStaticFuture<()>) {
            self.0.lock().unwrap().push_back(future);
        }
    }

    impl ControlledExecutor {
        async fn complete_pending(&self) {
            let pending = std::mem::take(&mut *self.0.lock().unwrap());
            for future in pending {
                // Await the entire Remote future, including completion accounting.
                future.await;
            }
        }
    }

    #[tokio::test]
    async fn test_completed_failure_blocks_prefetch_and_new_input() {
        for temporary in [false, true] {
            let executor = ControlledExecutor::default();
            let mut tasks = ConcurrentTasks::new(
                Executor::with(executor.clone()),
                8,
                8192,
                |mut input: (Arc<Vec<u8>>, Option<bool>)| {
                    Box::pin(async move {
                        let result = match input.1.take() {
                            Some(true) => {
                                Err(Error::new(ErrorKind::Unexpected, "retry").set_temporary())
                            }
                            Some(false) => Err(Error::new(ErrorKind::PermissionDenied, "stop")),
                            None => Ok(input.0[0]),
                        };
                        (input, result)
                    })
                },
            );
            tasks.execute((Arc::new(vec![0]), None)).await.unwrap();
            tasks.execute((Arc::new(vec![1]), None)).await.unwrap();
            let failed_payload = Arc::new(vec![2; 1024]);
            tasks
                .execute((failed_payload.clone(), Some(temporary)))
                .await
                .unwrap();
            executor.complete_pending().await;

            assert!(
                !tasks.has_remaining(),
                "completed failures must stop prefetch"
            );
            let new_payload = Arc::new(vec![3; 1024]);
            let error = tasks
                .execute((new_payload.clone(), None))
                .await
                .unwrap_err();
            assert_eq!(error.is_temporary(), temporary);
            assert_eq!(
                error.kind(),
                if temporary {
                    ErrorKind::Unexpected
                } else {
                    ErrorKind::PermissionDenied
                }
            );
            assert_eq!(
                Arc::strong_count(&new_payload),
                1,
                "new input was not accepted"
            );
            assert_eq!(
                Arc::strong_count(&failed_payload),
                if temporary { 2 } else { 1 }
            );

            executor.complete_pending().await;
            assert_eq!(Arc::strong_count(&failed_payload), 1);
            if temporary {
                let mut outputs = Vec::new();
                while let Some(output) = tasks.next().await {
                    outputs.push(output.unwrap());
                }
                assert_eq!(outputs, vec![0, 1, 2]);
            } else {
                assert!(tasks.next().await.unwrap().is_err());
            }
        }
    }

    #[tokio::test]
    async fn test_repeated_failures_do_not_accumulate_inputs() {
        let executor = ControlledExecutor::default();
        let mut tasks = ConcurrentTasks::new(Executor::with(executor.clone()), 8, 8192, |input| {
            Box::pin(async move {
                (
                    input,
                    Err::<(), _>(Error::new(ErrorKind::Unexpected, "retry").set_temporary()),
                )
            })
        });
        let payload = Arc::new(vec![42; 1024]);
        tasks.execute(payload.clone()).await.unwrap();
        executor.complete_pending().await;

        for _ in 0..32 {
            assert!(
                tasks
                    .execute(payload.clone())
                    .await
                    .unwrap_err()
                    .is_temporary()
            );
            executor.complete_pending().await;
            assert_eq!(
                Arc::strong_count(&payload),
                2,
                "only the original retry input may remain"
            );
        }
        tasks.clear();
        assert_eq!(Arc::strong_count(&payload), 1);
    }

    #[tokio::test]
    async fn test_failure_backpressure_is_cancel_safe_behind_pending_head() {
        let executor = ControlledExecutor::default();
        let mut tasks = ConcurrentTasks::new(
            Executor::with(executor.clone()),
            4,
            8192,
            |mut input: (usize, Option<futures::channel::oneshot::Receiver<()>>, bool)| {
                Box::pin(async move {
                    if let Some(receiver) = input.1.take() {
                        receiver.await.unwrap();
                    }
                    let result = if std::mem::take(&mut input.2) {
                        Err(Error::new(ErrorKind::Unexpected, "retry").set_temporary())
                    } else {
                        Ok(input.0)
                    };
                    (input, result)
                })
            },
        );
        let (sender, receiver) = futures::channel::oneshot::channel();
        tasks.execute((0, Some(receiver), false)).await.unwrap();
        tasks.execute((1, None, true)).await.unwrap();
        tasks.execute((2, None, false)).await.unwrap();
        let head = executor.0.lock().unwrap().pop_front().unwrap();
        let head = tokio::spawn(head);
        executor.complete_pending().await;

        assert!(!tasks.has_remaining());
        {
            let submission = tasks.execute((3, None, false));
            futures::pin_mut!(submission);
            assert!(futures::poll!(submission).is_pending());
        }
        sender.send(()).unwrap();
        head.await.unwrap();
        assert!(
            tasks
                .execute((3, None, false))
                .await
                .unwrap_err()
                .is_temporary()
        );
        executor.complete_pending().await;
        tasks.execute((3, None, false)).await.unwrap();
        executor.complete_pending().await;

        let mut outputs = Vec::new();
        while let Some(output) = tasks.next().await {
            outputs.push(output.unwrap());
        }
        assert_eq!(outputs, vec![0, 1, 2, 3]);
    }

    #[tokio::test]
    async fn test_clear_discards_completion_accounting() {
        for fail in [false, true] {
            let executor = ControlledExecutor::default();
            let mut tasks =
                ConcurrentTasks::new(Executor::with(executor.clone()), 4, 8192, |fail| {
                    Box::pin(async move {
                        let result = if fail {
                            Err(Error::new(ErrorKind::Unexpected, "retry").set_temporary())
                        } else {
                            Ok(())
                        };
                        (fail, result)
                    })
                });
            for _ in 0..4 {
                tasks.execute(fail).await.unwrap();
            }
            executor.complete_pending().await;
            tasks.clear();

            for _ in 0..4 {
                tasks.execute(false).await.unwrap();
            }
            assert!(
                !tasks.has_remaining(),
                "cleared completions must not provide capacity"
            );
            executor.complete_pending().await;
            for _ in 0..4 {
                tasks.next().await.unwrap().unwrap();
            }
            assert!(tasks.next().await.is_none());
        }
    }

    #[tokio::test]
    async fn test_concurrent_tasks() {
        let executor = Executor::new();

        let mut tasks = ConcurrentTasks::new(executor, 16, 8, |(i, dur)| {
            Box::pin(async move {
                sleep(dur).await;

                // 5% rate to fail.
                if rand::rng().random_range(0..100) > 90 {
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
            let dur = Duration::from_millis(rand::rng().random_range(0..10));
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

    #[tokio::test]
    async fn test_prefetch_backpressure() {
        let executor = Executor::new();
        let concurrent = 4;
        let prefetch = 2;

        // Create a slower task to ensure they don't complete immediately
        let mut tasks = ConcurrentTasks::new(executor, concurrent, prefetch, |i: usize| {
            Box::pin(async move {
                sleep(Duration::from_millis(100)).await;
                (i, Ok(i))
            })
        });

        // Initially, we should have space for concurrent tasks
        assert!(tasks.has_remaining(), "Should have space initially");

        // Submit concurrent tasks
        for i in 0..concurrent {
            assert!(tasks.has_remaining(), "Should have space for task {i}");
            tasks.execute(i).await.unwrap();
        }

        // Now we shouldn't have any more space (since no tasks have completed yet)
        assert!(
            !tasks.has_remaining(),
            "Should not have space after submitting concurrent tasks"
        );

        // Wait for some tasks to complete
        sleep(Duration::from_millis(150)).await;

        // Now we should have space up to prefetch limit
        for i in concurrent..concurrent + prefetch {
            assert!(
                tasks.has_remaining(),
                "Should have space for prefetch task {i}"
            );
            tasks.execute(i).await.unwrap();
        }

        // Now has_remaining should return false
        assert!(
            !tasks.has_remaining(),
            "Should not have remaining space after filling up prefetch buffer"
        );

        // Retrieve one result
        let result = tasks.next().await;
        assert!(result.is_some());

        // Now there should be space for one more task
        assert!(
            tasks.has_remaining(),
            "Should have remaining space after retrieving one result"
        );
    }

    #[tokio::test]
    async fn test_prefetch_zero() {
        let executor = Executor::new();
        let concurrent = 4;
        let prefetch = 0; // No prefetching allowed

        let mut tasks = ConcurrentTasks::new(executor, concurrent, prefetch, |i: usize| {
            Box::pin(async move {
                sleep(Duration::from_millis(10)).await;
                (i, Ok(i))
            })
        });

        // With prefetch=0, we can only submit up to concurrent tasks
        for i in 0..concurrent {
            tasks.execute(i).await.unwrap();
        }

        // Should not have space for more
        assert!(
            !tasks.has_remaining(),
            "Should not have remaining space with prefetch=0"
        );

        // Retrieve one result
        let result = tasks.next().await;
        assert!(result.is_some());

        // Now there should be space for exactly one more task
        assert!(
            tasks.has_remaining(),
            "Should have remaining space after retrieving one result"
        );

        // Execute one more
        tasks.execute(concurrent).await.unwrap();

        // Should be full again
        assert!(!tasks.has_remaining(), "Should be full again");
    }
}
