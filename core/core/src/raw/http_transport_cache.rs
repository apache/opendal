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
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::RwLock;
use std::sync::Weak;

use crate::types::HttpTransportDyn;
use crate::types::HttpTransporter;

/// Weak handle to an [`HttpTransporter`] for caches keyed by transport identity.
///
/// Storing a `WeakHttpTransporter` inside a cached value (such as a request
/// signer) prevents a strong reference cycle between [`HttpTransportCache`] and
/// the transport it is keyed by.
#[derive(Clone)]
pub struct WeakHttpTransporter {
    inner: Weak<dyn HttpTransportDyn>,
}

impl Debug for WeakHttpTransporter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WeakHttpTransporter")
            .field("alive", &(self.inner.strong_count() > 0))
            .finish()
    }
}

impl WeakHttpTransporter {
    /// Downgrade an [`HttpTransporter`] into a non-owning weak handle.
    pub fn downgrade(transport: &HttpTransporter) -> Self {
        Self {
            inner: transport.downgrade(),
        }
    }

    /// Upgrade into a strong [`HttpTransporter`] if the transport is still alive.
    pub fn upgrade(&self) -> Option<HttpTransporter> {
        self.inner.upgrade().map(HttpTransporter::from_arc)
    }
}

#[cfg(feature = "reqsign")]
impl reqsign_core::HttpSend for WeakHttpTransporter {
    async fn http_send(
        &self,
        req: http::Request<bytes::Bytes>,
    ) -> reqsign_core::Result<http::Response<bytes::Bytes>> {
        let transport = self.upgrade().ok_or_else(|| {
            reqsign_core::Error::unexpected(
                "HTTP transport has been dropped before credential request completed",
            )
        })?;
        transport.http_send(req).await
    }
}

struct CacheEntry<V> {
    weak: Weak<dyn HttpTransportDyn>,
    value: V,
}

/// Cache of transport-scoped values keyed by [`HttpTransporter`] allocation identity.
///
/// Entries are keyed by the thin data pointer address of the transport's
/// underlying `Arc` allocation, which stays unique among all live allocations
/// (including zero-sized transports because `ArcInner` always contains its
/// atomic reference counts) and cannot be reused while a `Weak` remains in the
/// cache.
pub struct HttpTransportCache<V> {
    entries: RwLock<HashMap<usize, CacheEntry<V>>>,
}

impl<V> Debug for HttpTransportCache<V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let len = self
            .entries
            .read()
            .unwrap_or_else(|err| err.into_inner())
            .len();
        f.debug_struct("HttpTransportCache")
            .field("len", &len)
            .finish()
    }
}

impl<V> Default for HttpTransportCache<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V> HttpTransportCache<V> {
    /// Create an empty cache.
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// Return the cached value for `transport`, or initialize and store one via `init`.
    ///
    /// `init` runs outside any cache lock and receives a [`WeakHttpTransporter`]
    /// so the cached value does not keep the transport alive across cache eviction.
    /// Dead entries from dropped transports are pruned before inserting a new entry.
    pub fn get_or_init(
        &self,
        transport: &HttpTransporter,
        init: impl FnOnce(WeakHttpTransporter) -> V,
    ) -> V
    where
        V: Clone,
    {
        let key = transport.identity_addr();

        {
            let entries = self.entries.read().unwrap_or_else(|err| err.into_inner());
            if let Some(entry) = entries.get(&key)
                && entry.weak.strong_count() > 0
            {
                return entry.value.clone();
            }
        }

        let weak = transport.downgrade();
        let value = init(WeakHttpTransporter {
            inner: weak.clone(),
        });

        let mut evicted = Vec::new();
        let mut discarded = None;
        let result = {
            let mut entries = self.entries.write().unwrap_or_else(|err| err.into_inner());
            if let Some(entry) = entries.get(&key)
                && entry.weak.strong_count() > 0
            {
                discarded = Some(value);
                entry.value.clone()
            } else {
                evicted.extend(
                    entries
                        .extract_if(|_, entry| entry.weak.strong_count() == 0)
                        .map(|(_, entry)| entry),
                );
                if let Some(replaced) = entries.insert(
                    key,
                    CacheEntry {
                        weak,
                        value: value.clone(),
                    },
                ) {
                    evicted.push(replaced);
                }
                value
            }
        };

        // Drop evicted entries and any losing concurrent initialization after
        // releasing the write lock so arbitrary `Drop` impls cannot re-enter or
        // stall the cache.
        drop(discarded);
        drop(evicted);

        result
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries
            .read()
            .unwrap_or_else(|err| err.into_inner())
            .len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use http::{Request, Response};

    use super::*;
    use crate::types::{Buffer, HttpBody, HttpTransport, Result};

    #[derive(Clone, Default)]
    struct DummyTransport;

    impl HttpTransport for DummyTransport {
        #[cfg(not(target_arch = "wasm32"))]
        async fn fetch(&self, _req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let stream = futures::stream::iter(vec![Ok(Buffer::new())]);
            Ok(Response::new(HttpBody::new(stream, Some(0))))
        }

        #[cfg(target_arch = "wasm32")]
        async fn fetch(&self, _req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let stream = futures::stream::iter(vec![Ok(Buffer::new())]);
            Ok(Response::new(HttpBody::new(stream, Some(0))))
        }
    }

    struct NonZstTransport(u64);

    impl HttpTransport for NonZstTransport {
        #[cfg(not(target_arch = "wasm32"))]
        async fn fetch(&self, _req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let _ = self.0;
            let stream = futures::stream::iter(vec![Ok(Buffer::new())]);
            Ok(Response::new(HttpBody::new(stream, Some(0))))
        }

        #[cfg(target_arch = "wasm32")]
        async fn fetch(&self, _req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let _ = self.0;
            let stream = futures::stream::iter(vec![Ok(Buffer::new())]);
            Ok(Response::new(HttpBody::new(stream, Some(0))))
        }
    }

    #[test]
    fn test_same_transport_reuses_entry() {
        let cache = HttpTransportCache::new();
        let transport = HttpTransporter::new(NonZstTransport(1));
        let cloned = transport.clone();
        let inits = AtomicUsize::new(0);

        let v1 = cache.get_or_init(&transport, |_| {
            inits.fetch_add(1, Ordering::SeqCst);
            Arc::new(42usize)
        });
        let v2 = cache.get_or_init(&transport, |_| {
            inits.fetch_add(1, Ordering::SeqCst);
            Arc::new(99usize)
        });
        let v3 = cache.get_or_init(&cloned, |_| {
            inits.fetch_add(1, Ordering::SeqCst);
            Arc::new(100usize)
        });

        assert_eq!(inits.load(Ordering::SeqCst), 1);
        assert!(Arc::ptr_eq(&v1, &v2));
        assert!(Arc::ptr_eq(&v1, &v3));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_distinct_transports_are_isolated() {
        let cache = HttpTransportCache::new();
        let t1 = HttpTransporter::new(NonZstTransport(1));
        let t2 = HttpTransporter::new(NonZstTransport(2));
        let d1 = HttpTransporter::default();
        let d2 = HttpTransporter::default();
        let inits = AtomicUsize::new(0);

        let next = |_: WeakHttpTransporter| {
            let id = inits.fetch_add(1, Ordering::SeqCst);
            Arc::new(id)
        };

        let v_t1 = cache.get_or_init(&t1, next);
        let v_t2 = cache.get_or_init(&t2, next);
        let v_d1 = cache.get_or_init(&d1, next);
        let v_d2 = cache.get_or_init(&d2, next);

        assert_eq!(inits.load(Ordering::SeqCst), 4);
        assert_eq!(cache.len(), 4);

        let all = [&v_t1, &v_t2, &v_d1, &v_d2];
        for i in 0..all.len() {
            for j in (i + 1)..all.len() {
                assert!(!Arc::ptr_eq(all[i], all[j]));
            }
        }

        // Subsequent lookups must hit their own entries without re-initializing.
        assert!(Arc::ptr_eq(&v_t1, &cache.get_or_init(&t1, next)));
        assert!(Arc::ptr_eq(&v_t2, &cache.get_or_init(&t2, next)));
        assert!(Arc::ptr_eq(&v_d1, &cache.get_or_init(&d1, next)));
        assert!(Arc::ptr_eq(&v_d2, &cache.get_or_init(&d2, next)));
        assert_eq!(inits.load(Ordering::SeqCst), 4);
    }

    struct DropSentinel {
        drops: Arc<AtomicUsize>,
    }

    impl Drop for DropSentinel {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[derive(Clone)]
    struct CachedHandle {
        weak: WeakHttpTransporter,
        _sentinel: Arc<DropSentinel>,
    }

    #[test]
    fn test_dead_entries_are_pruned_on_insert() {
        let cache = HttpTransportCache::new();
        let drops = Arc::new(AtomicUsize::new(0));

        let transport_a = HttpTransporter::new(DummyTransport);
        let handle_a = cache.get_or_init(&transport_a, |weak| CachedHandle {
            weak,
            _sentinel: Arc::new(DropSentinel {
                drops: drops.clone(),
            }),
        });
        assert!(handle_a.weak.upgrade().is_some());
        let weak_a = handle_a.weak.clone();
        drop(handle_a);

        // Dropping transport A must leave no strong reference in the cache.
        drop(transport_a);
        assert!(weak_a.upgrade().is_none());

        // Inserting transport B prunes A's dead entry and drops its sentinel.
        let transport_b = HttpTransporter::new(DummyTransport);
        let _handle_b = cache.get_or_init(&transport_b, |weak| CachedHandle {
            weak,
            _sentinel: Arc::new(DropSentinel {
                drops: drops.clone(),
            }),
        });
        assert_eq!(cache.len(), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 1);

        drop(_handle_b);
        drop(transport_b);

        // 100 ephemeral transports must not accumulate in the cache.
        let loop_drops = Arc::new(AtomicUsize::new(0));
        for _ in 0..100 {
            let transport = HttpTransporter::new(DummyTransport);
            let _ = cache.get_or_init(&transport, |weak| CachedHandle {
                weak,
                _sentinel: Arc::new(DropSentinel {
                    drops: loop_drops.clone(),
                }),
            });
        }
        assert_eq!(cache.len(), 1);
        assert_eq!(loop_drops.load(Ordering::SeqCst), 99);
    }

    #[cfg(all(feature = "reqsign", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn test_weak_transport_http_send_and_dropped_error() {
        use std::sync::Mutex;

        use bytes::Bytes;
        use http::header::ACCEPT_ENCODING;
        use reqsign_core::HttpSend;

        #[derive(Clone, Default)]
        struct RecordingTransport {
            accept_encoding: Arc<Mutex<Vec<Option<String>>>>,
        }

        impl HttpTransport for RecordingTransport {
            async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
                let value = req
                    .headers()
                    .get(ACCEPT_ENCODING)
                    .map(|v| v.to_str().unwrap().to_string());
                self.accept_encoding.lock().unwrap().push(value);

                let body = Buffer::from("ok");
                let size = body.len() as u64;
                let stream = futures::stream::iter(vec![Ok(body)]);
                Ok(Response::new(HttpBody::new(stream, Some(size))))
            }
        }

        let recording = RecordingTransport::default();
        let transport = HttpTransporter::new(recording.clone());
        let weak = WeakHttpTransporter::downgrade(&transport);

        let req = Request::get("http://127.0.0.1/token")
            .body(Bytes::new())
            .unwrap();
        let resp = weak.http_send(req).await.unwrap();
        assert_eq!(resp.body().as_ref(), b"ok");
        assert_eq!(
            *recording.accept_encoding.lock().unwrap(),
            vec![Some("identity".to_string())]
        );

        drop(transport);
        assert!(weak.upgrade().is_none());

        let req = Request::get("http://127.0.0.1/token")
            .body(Bytes::new())
            .unwrap();
        let err = weak.http_send(req).await.unwrap_err();
        assert_eq!(err.kind(), reqsign_core::ErrorKind::Unexpected);
        assert!(!err.is_retryable());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn test_concurrent_get_or_init_converges_and_allows_reentrant_init() {
        use std::sync::Barrier;

        let cache = Arc::new(HttpTransportCache::new());
        let transport = HttpTransporter::new(NonZstTransport(7));
        let threads = 8;
        let barrier = Arc::new(Barrier::new(threads));

        let mut handles = Vec::with_capacity(threads);
        for i in 0..threads {
            let cache = cache.clone();
            let transport = transport.clone();
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                cache.get_or_init(&transport, |_| Arc::new(i))
            }));
        }

        let values: Vec<Arc<usize>> = handles
            .into_iter()
            .map(|h| h.join().expect("worker thread panicked"))
            .collect();
        for value in &values[1..] {
            assert!(Arc::ptr_eq(&values[0], value));
        }

        // Reentrant initialization inside `init` must not deadlock.
        let t2 = HttpTransporter::new(NonZstTransport(8));
        let reentrant = cache.get_or_init(&t2, |_| {
            let nested = cache.get_or_init(&transport, |_| Arc::new(999));
            assert!(Arc::ptr_eq(&values[0], &nested));
            Arc::new(888)
        });
        assert_eq!(*reentrant, 888);

        // Compile-time proof that `get_or_init` does not hold any lock across `.await`.
        let cache_for_task = cache.clone();
        let transport_for_task = transport.clone();
        let task_val = tokio::spawn(async move {
            let val = cache_for_task.get_or_init(&transport_for_task, |_| Arc::new(0));
            tokio::task::yield_now().await;
            val
        })
        .await
        .expect("tokio task panicked");
        assert!(Arc::ptr_eq(&values[0], &task_val));
    }
}
