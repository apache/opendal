# opendal-http-transport-cyper

Cyper-based HTTP transport for [Apache OpenDAL](https://opendal.apache.org).

`CyperTransport` implements OpenDAL's `HttpTransport` trait with
[Cyper](https://crates.io/crates/cyper), an HTTP client for the Compio runtime.
It keeps one client and connection pool per runtime thread.

## Current limitation

OpenDAL does not yet provide a Compio runtime executor. This transport can be
configured, but applications cannot use it for operations until that runtime
support is added.

## TLS configuration

The default feature is `rustls`. Enable `native-tls` instead to select Cyper's
platform TLS backend.

## Use through `opendal`

The following configuration enables the transport, but the execution limitation
above currently prevents using it for operations:

```toml
[dependencies]
opendal = { version = "0.59", default-features = false, features = [
    "auto-register-services",
    "http-transport-cyper",
    "services-http",
] }
```

Use `http-transport-cyper-native-tls` instead of `http-transport-cyper` to
select the platform TLS backend.

## Use with `opendal-core`

Applications that use the split crates can configure Cyper directly while
working on a Compio-aware execution path:

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

Pass a configured service builder to `build_operator`. The resulting operator
remains subject to the execution limitation described above.

## License and Trademarks

Licensed under the Apache License, Version 2.0.

Apache OpenDAL, OpenDAL, and the OpenDAL logo are either registered trademarks
or trademarks of The Apache Software Foundation.
