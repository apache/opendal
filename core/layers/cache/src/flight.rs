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
                let work = Arc::new(Mutex::new(Some((sender, guard, future))));
                let spawned = {
                    let work = work.clone();
                    executor
                        .inner()
                        .try_execute(Box::pin(async move {
                            let Some((sender, guard, future)) = work.lock().unwrap().take() else {
                                return;
                            };
                            let value = future.await;
                            sender.send_replace(Some(value));
                            drop(guard);
                        }))
                        .is_ok()
                };
                if spawned {
                    return receiver
                        .wait_for(Option::is_some)
                        .await
                        .ok()
                        .and_then(|value| value.as_ref().cloned());
                }
                let Some((sender, guard, future)) = work.lock().unwrap().take() else {
                    return receiver
                        .wait_for(Option::is_some)
                        .await
                        .ok()
                        .and_then(|value| value.as_ref().cloned());
                };
                let task = async move {
                    let value = future.await;
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
