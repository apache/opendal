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
use std::io;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use futures::future;
use parking_lot::Mutex;

/// StdDnsResolver uses `getaddrinfo` to query dns.
#[derive(Default)]
pub struct StdDnsResolver {
    cache: DnsCache,
}

impl ureq::Resolver for StdDnsResolver {
    fn resolve(&self, netloc: &str) -> io::Result<Vec<SocketAddr>> {
        if let Some(v) = self.cache.get(netloc) {
            return Ok(v);
        }

        ToSocketAddrs::to_socket_addrs(netloc).map(|iter| {
            let res: Vec<_> = iter.collect();
            // Insert into cache if resolved succeeded.
            self.cache.insert(netloc, res.clone());
            res
        })
    }
}

/// AsyncStdDnsResolver uses `getaddrinfo` to query dns in tokio runtime.
pub struct AsyncStdDnsResolver {
    cache: DnsCache,
    runtime: Option<tokio::runtime::Runtime>,
}

impl Default for AsyncStdDnsResolver {
    fn default() -> Self {
        let runtime = {
            let mut builder = tokio::runtime::Builder::new_current_thread();
            builder.enable_all();

            builder.build().expect("build dns runtime failed")
        };

        Self {
            cache: DnsCache::default(),
            runtime: Some(runtime),
        }
    }
}

/// Make sure runtime has been dropped in background.
impl Drop for AsyncStdDnsResolver {
    fn drop(&mut self) {
        let runtime = self.runtime.take().unwrap();
        runtime.shutdown_background();
    }
}

impl reqwest::dns::Resolve for AsyncStdDnsResolver {
    fn resolve(&self, name: hyper::client::connect::dns::Name) -> reqwest::dns::Resolving {
        if let Some(v) = self.cache.get(name.as_str()) {
            return Box::pin(future::ok(Box::new(v.into_iter()) as reqwest::dns::Addrs));
        }

        debug_assert!(self.runtime.is_some(), "runtime must be valid");
        let runtime = self.runtime.as_ref().unwrap().handle().clone();

        let cache = self.cache.clone();
        let fut = async move {
            match runtime
                .spawn_blocking(move || {
                    // hyper will set port externally, so the port here is
                    // just used to construct a correct query.
                    //
                    // Ref: <https://github.com/hyperium/hyper/blob/4d89adce6122af1650165337d9d814314e7ee409/src/client/connect/http.rs#L322-L359>
                    (name.as_str(), 0).to_socket_addrs().map(|iter| {
                        let res: Vec<_> = iter.collect();
                        // Insert into cache if resolved succeeded.
                        cache.insert(name.as_str(), res.clone());
                        res
                    })
                })
                .await
            {
                Ok(v) => v.map(|v| Box::new(v.into_iter()) as reqwest::dns::Addrs),
                Err(err) => Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("spawn dns resolving task failed: {err:?}"),
                )),
            }
            .map_err(|err| Box::new(err) as Box<_>)
        };

        Box::pin(fut)
    }
}

/// TrustDnsResolver will adopt `trust-dns` to query dns in tokio runtime.
///
/// We will only use trust dns while we have an async runtime.
#[cfg(feature = "trust-dns")]
pub struct AsyncTrustDnsResolver {
    inner: Arc<trust_dns_resolver::TokioAsyncResolver>,
}

#[cfg(feature = "trust-dns")]
impl AsyncTrustDnsResolver {
    pub fn new() -> io::Result<Self> {
        let resolver = trust_dns_resolver::TokioAsyncResolver::from_system_conf(
            trust_dns_resolver::TokioHandle,
        )?;

        Ok(Self {
            inner: Arc::new(resolver),
        })
    }
}

#[cfg(feature = "trust-dns")]
impl reqwest::dns::Resolve for AsyncTrustDnsResolver {
    fn resolve(&self, name: hyper::client::connect::dns::Name) -> reqwest::dns::Resolving {
        let resolver = self.inner.clone();

        let fut = async move {
            let lookup = resolver.lookup_ip(name.as_str()).await?;

            // hyper will set port externally, so the port here is
            // just used to construct a correct SocketAddr.
            //
            // Ref: <https://github.com/hyperium/hyper/blob/4d89adce6122af1650165337d9d814314e7ee409/src/client/connect/http.rs#L322-L359>
            Ok(Box::new(lookup.into_iter().map(|v| SocketAddr::new(v, 0))) as reqwest::dns::Addrs)
        };

        Box::pin(fut)
    }
}

/// The cache entry we maintained in memory.
#[derive(Clone)]
struct DnsCacheEntry {
    value: Vec<SocketAddr>,
    expires_in: SystemTime,
}

/// DnsCache that used for accessing different storage services.
///
/// # Notes
///
/// **DON'T USE THIS DNS CACHE OUTSIDE OPENDAL.**
///
/// This is not a general dns cache that suitable for all use-cases.
#[derive(Clone)]
struct DnsCache {
    inner: Arc<Mutex<HashMap<String, DnsCacheEntry>>>,

    /// The most cache entries that we can hold.
    ///
    /// We will use 32 as default value (the same value as trust-dns).
    limits: usize,
    /// Ideally, we need to set the expire based on resolved dns record. But
    /// rust std doesn't give us this choice, so we have to use default one.
    ///
    /// We will use 3600s as the default expire time. In the future, we will
    /// allow user to configure it.
    default_expire: Duration,
}

impl Default for DnsCache {
    fn default() -> Self {
        DnsCache {
            inner: Arc::default(),
            limits: 32,
            default_expire: Duration::from_secs(3600),
        }
    }
}

impl DnsCache {
    fn get(&self, domain: &str) -> Option<Vec<SocketAddr>> {
        let mut guard = self.inner.lock();
        match guard.get(domain) {
            None => None,
            Some(entry) => {
                let now = SystemTime::now();
                if entry.expires_in >= now {
                    Some(entry.value.clone())
                } else {
                    // Remove already expires entry.
                    guard.remove(domain);
                    None
                }
            }
        }
    }

    fn insert(&self, domain: &str, value: Vec<SocketAddr>) {
        let mut guard = self.inner.lock();

        // If we are reaching the limits of dns cache entry. We will clean the
        // entire cache to make more space.
        //
        // As described in DnsCache's doc, this limit should never be reached.
        // We expect there only few entries held in cache.
        if guard.len() >= self.limits {
            guard.clear()
        }

        guard.insert(
            domain.to_string(),
            DnsCacheEntry {
                value,
                expires_in: SystemTime::now() + self.default_expire,
            },
        );
    }
}
