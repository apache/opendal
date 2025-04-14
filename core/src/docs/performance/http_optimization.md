# HTTP Optimization

All OpenDAL HTTP-based storage services use the same [HttpClient][crate::raw::HttpClient] abstraction. This design offers users a unified interface for configuring HTTP clients. The default HTTP client is [reqwest](https://crates.io/crates/reqwest), a popular and widely used HTTP client library in Rust.

Many of the services supported by OpenDAL are HTTP-based. This guide aims to provide optimization tips for using HTTP-based storage services. While these tips are also applicable to other HTTP clients, the configuration methods may vary.

Please note that the following optimizations are based on experience and may not be suitable for all scenarios. The most effective way to determine the optimal configuration is to test it in your specific environment.

## HTTP/1.1

According to benchmarks from OpenDAL users, `HTTP/1.1` is generally faster than `HTTP/2` for large-scale download and upload operations.

`reqwest` tends to maintain only a single TCP connection for `HTTP/2`, relying on its built-in multiplexing capabilities. While this works well for small files, such as web page downloads, the design is not ideal for handling large files or massive file scan OLAP workloads.

When `HTTP/2` is disabled, `reqwest` falls back to `HTTP/1.1` and utilizes its default connection pool. This approach is better suited for large files, as it allows multiple TCP connections to be opened and used concurrently, significantly improving performance for large file downloads and uploads.

If your workloads involve large files or require high throughput, and are not sensitive to latency, consider disabling `HTTP/2` in your configuration.

```rust
let client = reqwest::ClientBuilder::new()
  // Disable http2 for better performance.
  .http1_only()
  .build()
  .expect("http client must be created");

// Update the http client in the operator.
let op = op.update_http_client(|_| HttpClient::with(client));
```

## DNS Caching

`reqwest` uses the DNS resolver provided by Rust's standard library by default, which is backed by the `getaddrinfo` system call under the hood. This system call does not cache results by default, meaning that each time you make a request to a new domain, a DNS lookup will be performed.

Under high-throughput workloads, this can cause a significant performance degradation, as each request incurs the overhead of a DNS lookup. It can also negatively affect the resolver, potentially overwhelming it with the volume of requests. In extreme cases, this may result in a DoS attack on the resolver, rendering it unresponsive.

To mitigate this issue, you can enable DNS caching in `reqwest` by using the `hickory-dns` feature. This feature provides a more efficient DNS resolver that caches results.

```rust
let client = reqwest::ClientBuilder::new()
  // Enable hickory dns for dns caching and async dns resolve.
  .hickory_dns(true)
  .build()
  .expect("http client must be created");

// Update the http client in the operator.
let op = op.update_http_client(|_| HttpClient::with(client));
```

The default DNS cache settings from `hickory_dns` are generally sufficient for most workloads. However, if you have specific requirements—such as sharing the same DNS cache across multiple HTTP clients or configuring the DNS cache size—you can use the `Xuanwo/reqwest-hickory-resolver` crate to set up a custom DNS resolver.

```rust
/// Global shared hickory resolver.
static GLOBAL_HICKORY_RESOLVER: LazyLock<Arc<HickoryResolver>> = LazyLock::new(|| {
    let mut opts = ResolverOpts::default();
    // Only query for the ipv4 address.
    opts.ip_strategy = LookupIpStrategy::Ipv4Only;
    // Use larger cache size for better performance.
    opts.cache_size = 1024;
    // Positive TTL is set to 5 minutes.
    opts.positive_min_ttl = Some(Duration::from_secs(300));
    // Negative TTL is set to 1 minute.
    opts.negative_min_ttl = Some(Duration::from_secs(60));

    Arc::new(
        HickoryResolver::default()
            // Always shuffle the DNS results for better performance.
            .with_shuffle(true)
            .with_options(opts),
    )
});

let client = reqwest::ClientBuilder::new()
  // Use our global hickory resolver instead.
  .dns_resolver(GLOBAL_HICKORY_RESOLVER.clone())
  .build()
  .expect("http client must be created");

// Update the http client in the operator.
let op = op.update_http_client(|_| HttpClient::with(client));
```

The `ResolverOpts` has many options that can be configured. For a complete list of options, please refer to the [hickory_resolver documentation](https://docs.rs/hickory-resolver/latest/hickory_resolver/config/struct.ResolverOpts.html).

Here is a summary of the most commonly used options:

- `ip_strategy`: `hickory_resolver` default to use `Ipv4thenIpv6` strategy, which means it will first query for the IPv4 address and then the IPv6 address. This is generally a good strategy for most workloads. However, if you only need IPv4 addresses, you can set this option to `Ipv4Only` to avoid unnecessary DNS lookups.
- `cache_size`: This option controls the size of the DNS cache. A larger cache size can improve performance, but it may also increase memory usage. The default value is `32`.
- `positive_min_ttl` and `negative_min_ttl`: This option controls the minimum TTL for positive and negative DNS responses. A longer TTL can improve performance, but it may also increase the risk of stale DNS records. The default value is `None`. Some bad DNS servers may return a TTL of `0` even when the record is valid. In this case, you can set a longer TTL to avoid unnecessary DNS lookups.

In addition to the options mentioned above, `Xuanwo/reqwest-hickory-resolver` also offers a `shuffle` option. This setting determines whether the DNS results are shuffled before being returned. Shuffling can enhance performance by distributing the load more evenly across multiple IP addresses.

## Timeout

`reqwest` didn't set a default timeout for HTTP requests. This means that if a request hangs or takes too long to complete, it can block the entire process, leading to performance degradation or even application crashes.

It's recommended to set a connect timeout for HTTP requests to prevent this issue. 

```rust
let client = reqwest::ClientBuilder::new()
  // Set a connect timeout of 5 seconds.
  .connect_timeout(Duration::from_secs(5))
  .build()
  .expect("http client must be created");

// Update the http client in the operator.
let op = op.update_http_client(|_| HttpClient::with(client));
```

It's also recommended to use opendal's [`TimeoutLayer`][crate::layers::TimeoutLayer] to prevent slow requests hangs forever. This layer will automatically cancel the request if it takes too long to complete.

```rust
let op = op.layer(TimeoutLayer::new());
```

## Connection Pool

`reqwest` uses a connection pool to manage HTTP connections. This allows multiple requests to share the same connection, reducing the overhead of establishing new connections for each request.

By default, the connection pool is unlimited, allowing `reqwest` to open as many connections as needed. The default keep-alive timeout is 90 seconds, meaning any connection idle for longer than that will be closed.

You can tune those settings via:

- [pool_idle_timeout](https://docs.rs/reqwest/0.12.15/reqwest/struct.ClientBuilder.html#method.pool_idle_timeout): Set an optional timeout for idle sockets being kept-alive.
- [pool_max_idle_per_host](https://docs.rs/reqwest/0.12.15/reqwest/struct.ClientBuilder.html#method.pool_max_idle_per_host): Sets the maximum idle connection per host allowed in the pool.
