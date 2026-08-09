# opendal-http-transport-cyper

Cyper-based HTTP transport for [Apache OpenDAL](https://opendal.apache.org).

`CyperTransport` implements OpenDAL's `HttpTransport` trait with
[Cyper](https://crates.io/crates/cyper), an HTTP client for the Compio runtime.
It keeps one client and connection pool per runtime thread.

Poll each request future and response body on the Compio runtime thread where
polling started. Moving a partially polled request or response body to another
runtime thread is unsupported.

## TLS configuration

The default feature is `rustls`. Enable `native-tls` instead to select Cyper's
platform TLS backend.

## Use through `opendal`

Disable the default Reqwest transport, then enable the Cyper transport:

```toml
[dependencies]
opendal = { version = "0.59", default-features = false, features = [
    "auto-register-services",
    "http-transport-cyper",
    "services-http",
] }
```

Run operations that use this transport inside a Compio runtime. Applications
that use concurrent OpenDAL operations must also configure an executor that can
run those operations without moving Cyper response bodies between runtime
threads.

Use `http-transport-cyper-native-tls` instead of `http-transport-cyper` to
select the platform TLS backend.

## Use with `opendal-core`

Applications that use the split crates can attach Cyper to an operator without
installing a process-wide default transport:

```rust,ignore
use opendal_core::Builder;
use opendal_core::HttpTransporter;
use opendal_core::OperationContext;
use opendal_core::Operator;
use opendal_core::Result;
use opendal_http_transport_cyper::CyperTransport;

fn build_operator<B: Builder>(builder: B) -> Result<Operator> {
    let transport = HttpTransporter::new(CyperTransport::new());
    let context = OperationContext::new().with_http_transport(transport);

    Ok(Operator::new(builder)?.with_context(context))
}
```

Pass a configured service builder to `build_operator`, then use the resulting
operator inside a Compio runtime.

## License and Trademarks

Licensed under the Apache License, Version 2.0.

Apache OpenDAL, OpenDAL, and the OpenDAL logo are either registered trademarks
or trademarks of The Apache Software Foundation.
