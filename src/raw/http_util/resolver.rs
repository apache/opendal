use std::{net::SocketAddr, sync::Arc};

use hyper::client::connect::dns::Name;
use once_cell::sync::OnceCell;
use reqwest::dns::{Addrs, Resolve};
use trust_dns_resolver::TokioAsyncResolver;

/// a global resolver to reuse dns cache
///
/// # Note:
/// this structure contains nothing, only used as an entry point
/// to call from `trust_dns_resolver`
#[derive(Debug)]
pub struct DnsClient {}

static GLOBAL_DNS_CLIENT: OnceCell<Arc<TokioAsyncResolver>> = OnceCell::new();

/// Get a new async dns client.
fn get_client() -> Arc<TokioAsyncResolver> {
    GLOBAL_DNS_CLIENT
        .get_or_init(|| {
            let resolver = TokioAsyncResolver::tokio_from_system_conf().unwrap();
            Arc::new(resolver)
        })
        .clone()
}

impl DnsClient {
    /// get a new global dns client
    pub fn new() -> Self {
        DnsClient {}
    }
    /// get a new global dns client, wrapped in Arc
    pub fn new_arc() -> Arc<Self> {
        Arc::new(DnsClient::new())
    }
}

impl Resolve for DnsClient {
    fn resolve(&self, name: Name) -> reqwest::dns::Resolving {
        Box::pin(async move {
            let resolver = get_client();
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

        let our_resolver = DnsClient::new_arc();
        let addrs = our_resolver
            .resolve("datafuselabs.rs".parse().unwrap())
            .await
            .unwrap();
        let got = addrs.map(|s| s.ip()).collect::<Vec<_>>();

        let trust_resolver = TokioAsyncResolver::tokio_from_system_conf().unwrap();
        let addrs = trust_resolver.lookup_ip("datafuselabs.rs").await.unwrap();
        let expected = addrs.into_iter().collect::<Vec<_>>();
        assert!(got.len() > 0);
        assert_eq!(got, expected);
    }
}
