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

use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use opendal_core::Executor;
use tokio::sync::watch;

pub(crate) struct Flights<K, T>(Arc<Mutex<HashMap<K, watch::Receiver<Option<T>>>>>);

impl<K, T> Default for Flights<K, T> {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(HashMap::new())))
    }
}

impl<K, T> Flights<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    pub(crate) async fn run<F>(
        &self,
        executor: &Executor,
        keys: Vec<K>,
        fill: impl FnOnce(usize) -> F,
    ) -> Option<T>
    where
        F: Future<Output = T> + opendal_core::raw::MaybeSend + 'static,
    {
        loop {
            let (mut receiver, leader) = {
                let mut pending = self.0.lock().unwrap();
                if let Some(flight) = pending.get(&keys[0]) {
                    (flight.clone(), None)
                } else {
                    let (sender, receiver) = watch::channel(None);
                    let count = keys
                        .iter()
                        .take_while(|key| !pending.contains_key(*key))
                        .count();
                    let reserved = keys[..count].to_vec();
                    for key in &reserved {
                        pending.insert(key.clone(), receiver.clone());
                    }
                    (receiver, Some((sender, reserved)))
                }
            };
            if let Some((sender, reserved)) = leader {
                let guard = Reservation {
                    table: self.0.clone(),
                    keys: reserved,
                };
                let future = fill(guard.keys.len());
                // Drop the leader's handle to `work` before awaiting `receiver` so an
                // executor that drops an unpolled task immediately releases `guard`
                // and closes `sender` instead of deadlocking waiters.
                let taken = {
                    let work = Arc::new(Mutex::new(Some(FlightWork {
                        future,
                        guard,
                        sender,
                    })));
                    let spawned = {
                        let work = work.clone();
                        executor
                            .inner()
                            .try_execute(Box::pin(async move {
                                let Some(work) = work.lock().unwrap().take() else {
                                    return;
                                };
                                // Bind `sender` before `guard` so panics unwind `future`,
                                // then remove keys in `guard`, then wake waiters via `sender`.
                                let sender = work.sender;
                                let guard = work.guard;
                                let value = work.future.await;
                                sender.send_replace(Some(value));
                                drop(guard);
                            }))
                            .is_ok()
                    };
                    if spawned {
                        None
                    } else {
                        work.lock().unwrap().take()
                    }
                };
                let Some(work) = taken else {
                    return receiver
                        .wait_for(Option::is_some)
                        .await
                        .ok()
                        .and_then(|value| value.as_ref().cloned());
                };
                let task = async move {
                    let sender = work.sender;
                    let guard = work.guard;
                    let value = work.future.await;
                    sender.send_replace(Some(value));
                    drop(guard);
                };
                #[cfg(not(target_arch = "wasm32"))]
                if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                    runtime.spawn(task);
                    return receiver
                        .wait_for(Option::is_some)
                        .await
                        .ok()
                        .and_then(|value| value.as_ref().cloned());
                }
                task.await;
                return receiver.borrow().as_ref().cloned();
            }
            if receiver.wait_for(Option::is_some).await.is_ok() {
                return receiver.borrow().as_ref().cloned();
            }
        }
    }
}

// Field order ensures an unpolled drop releases `future`, removes map keys in
// `guard`, and only then closes `sender` to wake waiters.
struct FlightWork<K: Eq + Hash, T, F> {
    future: F,
    guard: Reservation<K, T>,
    sender: watch::Sender<Option<T>>,
}

struct Reservation<K: Eq + Hash, T> {
    table: Arc<Mutex<HashMap<K, watch::Receiver<Option<T>>>>>,
    keys: Vec<K>,
}

impl<K: Eq + Hash, T> Drop for Reservation<K, T> {
    fn drop(&mut self) {
        let mut pending = self.table.lock().unwrap();
        for key in &self.keys {
            pending.remove(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use opendal_core::Execute;
    use opendal_core::raw::BoxedStaticFuture;
    use tokio::sync::Notify;

    use super::*;

    struct AbandoningExecutor;

    impl Execute for AbandoningExecutor {
        fn execute(&self, _f: BoxedStaticFuture<()>) {}
    }

    struct QueuingExecutor {
        queued: Arc<Mutex<Vec<BoxedStaticFuture<()>>>>,
        submitted: Arc<Notify>,
    }

    impl Execute for QueuingExecutor {
        fn execute(&self, f: BoxedStaticFuture<()>) {
            self.queued.lock().unwrap().push(f);
            self.submitted.notify_one();
        }
    }

    #[tokio::test]
    async fn abandoned_sync_drop_returns_none_and_clears_reservation() {
        let flights: Flights<u64, u64> = Flights::default();
        let abandoning = Executor::with(AbandoningExecutor);

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            flights.run(&abandoning, vec![1, 2], |_| async { 42 }),
        )
        .await
        .expect("abandoned flight must not hang");
        assert_eq!(result, None);
        assert!(
            flights.0.lock().unwrap().is_empty(),
            "abandoned flight must release all reserved keys"
        );

        let working = Executor::new();
        let result = flights
            .run(&working, vec![1, 2], |count| async move { count as u64 })
            .await;
        assert_eq!(result, Some(2));
        assert!(flights.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn abandoned_async_drop_wakes_leader_and_waiter_and_clears_reservation() {
        let flights: Arc<Flights<u64, u64>> = Arc::new(Flights::default());
        let queued = Arc::new(Mutex::new(Vec::new()));
        let submitted = Arc::new(Notify::new());
        let executor = Executor::with(QueuingExecutor {
            queued: queued.clone(),
            submitted: submitted.clone(),
        });

        let leader = tokio::spawn({
            let flights = flights.clone();
            let executor = executor.clone();
            async move { flights.run(&executor, vec![7], |_| async { 99 }).await }
        });
        submitted.notified().await;

        let waiter = tokio::spawn({
            let flights = flights.clone();
            let working = Executor::new();
            async move { flights.run(&working, vec![7], |_| async { 100 }).await }
        });

        // Wait until the waiter has cloned the leader's receiver before abandoning.
        tokio::task::yield_now().await;
        queued.lock().unwrap().clear();

        let (leader_res, waiter_res) = tokio::time::timeout(Duration::from_secs(1), async {
            (leader.await.unwrap(), waiter.await.unwrap())
        })
        .await
        .expect("leader and waiter must wake when unpolled task is dropped");
        assert_eq!(leader_res, None);
        assert_eq!(waiter_res, Some(100));
        assert!(
            flights.0.lock().unwrap().is_empty(),
            "flight map must be empty after abandoned leader and recovered waiter"
        );
    }
}
