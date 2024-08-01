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

use std::mem;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use flume::Receiver;
use flume::Sender;
use futures::channel::oneshot;
use futures::Future;
use monoio::FusionDriver;
use monoio::RuntimeBuilder;

/// a boxed function that spawns task in current monoio runtime
type TaskSpawner = Box<dyn FnOnce() + Send>;

#[derive(Debug)]
pub struct MonoiofsCore {
    root: PathBuf,
    #[allow(dead_code)]
    /// sender that sends [`TaskSpawner`] to worker threads
    tx: Sender<TaskSpawner>,
    #[allow(dead_code)]
    /// join handles of worker threads
    threads: Mutex<Vec<std::thread::JoinHandle<()>>>,
}

impl MonoiofsCore {
    pub fn new(root: PathBuf, worker_threads: usize, io_uring_entries: u32) -> Self {
        // Since users use monoiofs in a context of tokio, all monoio
        // operations need to be dispatched to a dedicated thread pool
        // where a monoio runtime runs on each thread. Here we spawn
        // these worker threads.
        let (tx, rx) = flume::unbounded();
        let threads = (0..worker_threads)
            .map(move |i| {
                let rx = rx.clone();
                std::thread::Builder::new()
                    .name(format!("monoiofs-worker-{i}"))
                    .spawn(move || Self::worker_entrypoint(rx, io_uring_entries))
                    .expect("spawn worker thread should success")
            })
            .collect();
        let threads = Mutex::new(threads);

        Self { root, tx, threads }
    }

    pub fn root(&self) -> &PathBuf {
        &self.root
    }

    /// entrypoint of each worker thread, sets up monoio runtimes and channels
    fn worker_entrypoint(rx: Receiver<TaskSpawner>, io_uring_entries: u32) {
        let mut rt = RuntimeBuilder::<FusionDriver>::new()
            .enable_all()
            .with_entries(io_uring_entries)
            .build()
            .expect("monoio runtime initialize should success");
        // run a infinite loop that receives TaskSpawner and calls
        // them in a context of monoio
        rt.block_on(async {
            while let Ok(spawner) = rx.recv_async().await {
                spawner();
            }
        })
    }

    #[allow(dead_code)]
    /// create a TaskSpawner, send it to the thread pool and wait
    /// for its result
    pub async fn dispatch<F, Fut, T>(&self, f: F) -> T
    where
        F: FnOnce() -> Fut + 'static + Send,
        Fut: Future<Output = T>,
        T: 'static + Send,
    {
        // oneshot channel to send result back
        let (tx, rx) = oneshot::channel();
        self.tx
            .send_async(Box::new(move || {
                monoio::spawn(async move {
                    tx.send(f().await)
                        // discard result because it may be non-Debug and
                        // we don't need it to appear in the panic message
                        .map_err(|_| ())
                        .expect("send result from worker thread should success");
                });
            }))
            .await
            .expect("send new TaskSpawner to worker thread should success");
        match rx.await {
            Ok(result) => result,
            // tx is dropped without sending result, probably the worker
            // thread has panicked.
            Err(_) => self.propagate_worker_panic(),
        }
    }

    /// This method always panics. It is called only when at least a
    /// worker thread has panicked or meet a broken rx, which is
    /// unrecoverable. It propagates worker thread's panic if there
    /// is any and panics on normally exited thread.
    fn propagate_worker_panic(&self) -> ! {
        let mut guard = self.threads.lock().unwrap();
        // wait until the panicked thread exits
        std::thread::sleep(Duration::from_millis(100));
        let threads = mem::take(&mut *guard);
        // we don't know which thread panicked, so check them one by one
        for thread in threads {
            if thread.is_finished() {
                // worker thread runs an infinite loop, hence finished
                // thread must have panicked or meet a broken rx.
                match thread.join() {
                    // rx is broken
                    Ok(()) => panic!("worker thread should not exit, tx may be dropped"),
                    // thread has panicked
                    Err(e) => std::panic::resume_unwind(e),
                }
            }
        }
        unreachable!("this method should panic")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use futures::channel::mpsc::UnboundedSender;
    use futures::channel::mpsc::{self};
    use futures::StreamExt;

    use super::*;

    fn new_core(worker_threads: usize) -> Arc<MonoiofsCore> {
        Arc::new(MonoiofsCore::new(PathBuf::new(), worker_threads, 1024))
    }

    async fn dispatch_simple(core: Arc<MonoiofsCore>) {
        let result = core.dispatch(|| async { 42 }).await;
        assert_eq!(result, 42);
        let bytes: Vec<u8> = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let bytes_clone = bytes.clone();
        let result = core.dispatch(move || async move { bytes }).await;
        assert_eq!(result, bytes_clone);
    }

    async fn dispatch_concurrent(core: Arc<MonoiofsCore>) {
        let (tx, mut rx) = mpsc::unbounded();

        async fn spawn_task(core: Arc<MonoiofsCore>, tx: UnboundedSender<u64>, sleep_millis: u64) {
            tokio::spawn(async move {
                let result = core
                    .dispatch(move || async move {
                        monoio::time::sleep(Duration::from_millis(sleep_millis)).await;
                        sleep_millis
                    })
                    .await;
                assert_eq!(result, sleep_millis);
                tx.unbounded_send(result).unwrap();
            });
        }

        spawn_task(core.clone(), tx.clone(), 200).await;
        spawn_task(core.clone(), tx.clone(), 20).await;
        drop(tx);
        let first = rx.next().await;
        let second = rx.next().await;
        let third = rx.next().await;
        assert_eq!(first, Some(20));
        assert_eq!(second, Some(200));
        assert_eq!(third, None);
    }

    async fn dispatch_panic(core: Arc<MonoiofsCore>) {
        core.dispatch(|| async { panic!("BOOM") }).await;
    }

    #[tokio::test]
    async fn test_monoio_single_thread_dispatch() {
        let core = new_core(1);
        assert_eq!(core.threads.lock().unwrap().len(), 1);
        dispatch_simple(core).await;
    }

    #[tokio::test]
    async fn test_monoio_single_thread_dispatch_concurrent() {
        let core = new_core(1);
        dispatch_concurrent(core).await;
    }

    #[tokio::test]
    #[should_panic(expected = "BOOM")]
    async fn test_monoio_single_thread_dispatch_panic() {
        let core = new_core(1);
        dispatch_panic(core).await;
    }

    #[tokio::test]
    async fn test_monoio_multi_thread_dispatch() {
        let core = new_core(4);
        assert_eq!(core.threads.lock().unwrap().len(), 4);
        dispatch_simple(core).await;
    }

    #[tokio::test]
    async fn test_monoio_multi_thread_dispatch_concurrent() {
        let core = new_core(4);
        dispatch_concurrent(core).await;
    }

    #[tokio::test]
    #[should_panic(expected = "BOOM")]
    async fn test_monoio_multi_thread_dispatch_panic() {
        let core = new_core(4);
        dispatch_panic(core).await;
    }
}
