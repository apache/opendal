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

//! Helpers with no better home in a specific operation.

use asyncband::once::OnceCell;
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::hash::Hash;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// Bounds the cache far above any working set this service caches.
const DEFAULT_CAPACITY: usize = 8192;

/// A concurrent cache whose entries expire, bounded by a maximum count.
///
/// Concurrent lookups of one key share a single computation. Cloning shares
/// the entries; a [`Duration::ZERO`] `ttl` disables caching.
///
/// Hand-rolled because coalescing rules out most crates: `quick_cache` has no
/// expiry, `ttl_cache` is not `Sync`, `retainer` needs a monitor task,
/// `stretto` spawns threads. `moka` fits but adds twelve dependencies and
/// hands back `Arc<E>`, losing the context and temporary flag `RetryLayer`
/// reads.
pub(super) struct TtlCache<K, V> {
    map: Arc<Mutex<HashMap<K, Entry<V>>>>,
    ttl: Duration,
    cap: usize,
}

struct Entry<V> {
    /// Shared so a lookup can release the map lock before `init` runs.
    cell: Arc<OnceCell<V>>,
    inserted_at: Instant,
}

impl<K, V> Clone for TtlCache<K, V> {
    /// Hand-written: deriving would demand `K: Clone, V: Clone`.
    fn clone(&self) -> Self {
        Self {
            map: self.map.clone(),
            ttl: self.ttl,
            cap: self.cap,
        }
    }
}

impl<K, V> Debug for TtlCache<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TtlCache")
            .field("ttl", &self.ttl)
            .field("cap", &self.cap)
            .field("len", &self.lock().len())
            .finish()
    }
}

impl<K: Eq + Hash, V: Clone> TtlCache<K, V> {
    /// Creates a cache of [`DEFAULT_CAPACITY`] entries, each live for `ttl`.
    pub(super) fn new(ttl: Duration) -> Self {
        Self::with_capacity(ttl, DEFAULT_CAPACITY)
    }

    /// Creates a cache holding at most `cap` entries, each live for `ttl`.
    fn with_capacity(ttl: Duration, cap: usize) -> Self {
        Self {
            map: Arc::new(Mutex::new(HashMap::new())),
            ttl,
            cap,
        }
    }

    /// Returns the value for `key`, computing it with `init` when absent or
    /// expired. An error is not cached, so the next caller retries.
    pub(super) async fn get_or_try_init<E>(
        &self,
        key: K,
        init: impl AsyncFnOnce() -> Result<V, E>,
    ) -> Result<V, E> {
        if self.ttl.is_zero() {
            return init().await;
        }

        // Scoped so the lock is released before `init` runs.
        let cell = {
            let mut map = self.lock();
            match map.get(&key).filter(|entry| self.is_live(entry)) {
                Some(entry) => entry.cell.clone(),
                None => {
                    self.make_room(&mut map);
                    let cell = Arc::new(OnceCell::new());
                    map.insert(key, Entry::new(cell.clone()));
                    cell
                }
            }
        };

        cell.get_or_try_init(init).await.cloned()
    }

    /// Removes every entry.
    ///
    /// A computation already in flight keeps its own cell and may still
    /// complete; every later lookup recomputes.
    pub(super) fn clear(&self) {
        self.lock().clear();
    }

    /// Makes room for one more entry once the cache is full: drops what has
    /// already expired, and only if that is not enough drops everything.
    fn make_room(&self, map: &mut HashMap<K, Entry<V>>) {
        if map.len() < self.cap {
            return;
        }
        map.retain(|_, entry| self.is_live(entry));
        if map.len() >= self.cap {
            map.clear();
        }
    }

    fn is_live(&self, entry: &Entry<V>) -> bool {
        entry.inserted_at.elapsed() < self.ttl
    }
}

impl<K, V> TtlCache<K, V> {
    /// A poisoned lock means a caller panicked, not that the entries are
    /// unusable: a stale one is at worst recomputed. Recover rather than
    /// propagate the panic for the life of the cache.
    fn lock(&self) -> MutexGuard<'_, HashMap<K, Entry<V>>> {
        self.map.lock().unwrap_or_else(|err| err.into_inner())
    }
}

impl<V> Entry<V> {
    fn new(cell: Arc<OnceCell<V>>) -> Self {
        Self {
            cell,
            inserted_at: Instant::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    async fn init_ok(calls: &AtomicUsize, value: &str) -> Result<String, ()> {
        calls.fetch_add(1, Ordering::SeqCst);
        Ok(value.to_string())
    }

    #[tokio::test]
    async fn a_hit_does_not_recompute() {
        let cache: TtlCache<&str, String> = TtlCache::with_capacity(Duration::from_secs(60), 8);
        let calls = AtomicUsize::new(0);

        for _ in 0..3 {
            let v = cache
                .get_or_try_init("k", async || init_ok(&calls, "v").await)
                .await
                .unwrap();
            assert_eq!(v, "v");
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn keys_are_independent() {
        let cache: TtlCache<&str, String> = TtlCache::with_capacity(Duration::from_secs(60), 8);
        let calls = AtomicUsize::new(0);

        cache
            .get_or_try_init("a", async || init_ok(&calls, "v").await)
            .await
            .unwrap();
        cache
            .get_or_try_init("b", async || init_ok(&calls, "v").await)
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn an_expired_entry_is_recomputed() {
        let cache: TtlCache<&str, String> = TtlCache::with_capacity(Duration::from_millis(30), 8);
        let calls = AtomicUsize::new(0);

        cache
            .get_or_try_init("k", async || init_ok(&calls, "old").await)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let v = cache
            .get_or_try_init("k", async || init_ok(&calls, "new").await)
            .await
            .unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(v, "new", "the fresh value replaces the expired one");
    }

    #[tokio::test]
    async fn a_zero_ttl_disables_caching() {
        let cache: TtlCache<&str, String> = TtlCache::with_capacity(Duration::ZERO, 8);
        let calls = AtomicUsize::new(0);

        for _ in 0..3 {
            cache
                .get_or_try_init("k", async || init_ok(&calls, "v").await)
                .await
                .unwrap();
        }
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn clear_drops_everything() {
        let cache: TtlCache<&str, String> = TtlCache::with_capacity(Duration::from_secs(60), 8);
        let calls = AtomicUsize::new(0);

        cache
            .get_or_try_init("k", async || init_ok(&calls, "v").await)
            .await
            .unwrap();
        cache.clear();
        cache
            .get_or_try_init("k", async || init_ok(&calls, "v").await)
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    /// The cache is bounded: filling it past `capacity` drops what it holds
    /// rather than growing without limit.
    #[tokio::test]
    async fn capacity_bounds_the_cache() {
        let cache: TtlCache<&str, String> = TtlCache::with_capacity(Duration::from_secs(60), 2);
        let calls = AtomicUsize::new(0);

        for key in ["a", "b", "c"] {
            cache
                .get_or_try_init(key, async || init_ok(&calls, "v").await)
                .await
                .unwrap();
        }
        assert_eq!(calls.load(Ordering::SeqCst), 3);

        // "a" and "b" were live, so making room for "c" dropped them.
        cache
            .get_or_try_init("a", async || init_ok(&calls, "v").await)
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 4, "an evicted key recomputes");
    }

    /// A failed computation is not cached.
    #[tokio::test]
    async fn an_error_is_not_cached() {
        let cache: TtlCache<&str, String> = TtlCache::with_capacity(Duration::from_secs(60), 8);
        let calls = AtomicUsize::new(0);

        let err = cache
            .get_or_try_init("k", async || {
                calls.fetch_add(1, Ordering::SeqCst);
                Err::<String, &str>("boom")
            })
            .await;
        assert_eq!(err, Err("boom"));

        let v = cache
            .get_or_try_init("k", async || init_ok(&calls, "v").await)
            .await
            .unwrap();
        assert_eq!(v, "v");
        assert_eq!(calls.load(Ordering::SeqCst), 2, "the next caller retries");
    }

    /// A panic while the lock is held poisons it. The cache recovers rather
    /// than propagating that panic to every later lookup.
    #[tokio::test]
    async fn a_poisoned_lock_is_recovered() {
        let cache: TtlCache<&str, String> = TtlCache::with_capacity(Duration::from_secs(60), 8);

        let poisoner = cache.clone();
        let _ = std::thread::spawn(move || {
            let _guard = poisoner.map.lock().unwrap();
            panic!("poison the lock");
        })
        .join();
        assert!(cache.map.lock().is_err(), "the lock must be poisoned");

        let calls = AtomicUsize::new(0);
        let value = cache
            .get_or_try_init("k", async || init_ok(&calls, "v").await)
            .await
            .unwrap();
        assert_eq!(value, "v");
        cache.clear();
    }

    /// Concurrent readers of one key share a single computation.
    #[tokio::test]
    async fn concurrent_lookups_share_one_computation() {
        let cache: TtlCache<&str, String> = TtlCache::with_capacity(Duration::from_secs(60), 8);
        let calls = AtomicUsize::new(0);

        let slow = async || {
            calls.fetch_add(1, Ordering::SeqCst);
            tokio::task::yield_now().await;
            Ok::<String, ()>("v".to_string())
        };
        let (a, b, c) = futures::join!(
            cache.get_or_try_init("k", slow),
            cache.get_or_try_init("k", slow),
            cache.get_or_try_init("k", slow),
        );

        assert_eq!((a.unwrap(), b.unwrap(), c.unwrap()).0, "v");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
