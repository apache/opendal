// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::net::SocketAddr;
use std::sync::Arc;

use hyper::client::connect::dns::Name;
use once_cell::sync::OnceCell;
use reqwest::dns::Addrs;
use reqwest::dns::Resolve;
use trust_dns_resolver::TokioAsyncResolver;

/// a global resolver to reuse dns cache
///
/// # Note:
/// this structure contains nothing, only used as an entry point
/// to call from `trust_dns_resolver`
#[derive(Debug)]
pub struct TrustDnsClient {
    resolver: Arc<TokioAsyncResolver>,
}

static GLOBAL_DNS_CLIENT: OnceCell<Arc<TrustDnsClient>> = OnceCell::new();

/// Get a new async dns client.
pub fn get_trust_dns_client() -> Arc<TrustDnsClient> {
    GLOBAL_DNS_CLIENT
        .get_or_init(|| Arc::new(TrustDnsClient::new()))
        .clone()
}

impl TrustDnsClient {
    /// get a new global dns client
    pub fn new() -> Self {
        let resolver = Arc::new(
            TokioAsyncResolver::tokio_from_system_conf().expect("init trust dns resolver failure"),
        );
        Self { resolver }
    }
}

impl Resolve for TrustDnsClient {
    fn resolve(&self, name: Name) -> reqwest::dns::Resolving {
        let resolver = self.resolver.clone();
        Box::pin(async move {
            let ip = resolver.lookup_ip(name.as_str()).await?;
            let addrs: Addrs = Box::new(ip.into_iter().map(|ip| SocketAddr::new(ip, 0)));
            Ok(addrs)
        })
    }
}

#[cfg(test)]
mod resolve_test {
    #[tokio::test]
    async fn test_resolve() {
        use super::*;

        let name = "docs.rs";
        let our_resolver = get_trust_dns_client();
        let addrs = our_resolver.resolve(name.parse().unwrap()).await.unwrap();
        let got = addrs.map(|s| s.ip()).collect::<Vec<_>>();

        let trust_resolver = TokioAsyncResolver::tokio_from_system_conf().unwrap();
        let addrs = trust_resolver.lookup_ip(name).await.unwrap();
        let expected = addrs.into_iter().collect::<Vec<_>>();
        assert!(got.len() > 0);
        assert_eq!(got, expected);
    }
}
