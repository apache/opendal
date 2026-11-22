# opendal-http-transport-cyper

Cyper-based HTTP transport for [Apache OpenDAL](https://opendal.apache.org).

`CyperTransport` implements OpenDAL's `HttpTransport` trait with
[Cyper](https://crates.io/crates/cyper), an HTTP client for Compio 0.19. The
transport requires Rust 1.95 or later.

## Use Cyper with an operator

Install `CyperTransport` on the operator's `OperationContext` and start the
operation inside a running Compio runtime. For example, this code reads an
object from an HTTP endpoint:

```rust,no_run
use opendal_core::{HttpTransporter, Operator};
use opendal_http_transport_cyper::CyperTransport;
use opendal_service_http::Http;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = compio::runtime::Runtime::new()?;
    let op = Operator::new(Http::default().endpoint("https://example.com"))?;
    let transport = HttpTransporter::new(CyperTransport::new());
    let context = op.base_context().with_http_transport(transport);
    let op = op.with_context(context);

    let body = runtime.block_on(op.read("object"))?;
    println!("Read {} bytes", body.len());
    Ok(())
}
```

Keep the originating Compio runtime running while consuming a streamed response.
If an operation schedules additional HTTP requests through an OpenDAL executor,
configure that executor to start the tasks on Compio as well.

With the `opendal` facade, enable `http-transport-cyper` and `services-http` and
import `opendal::CyperTransport`, `opendal::HttpTransporter`,
`opendal::Operator`, and `opendal::services::Http`. Configure the operator the
same way. The facade installs Reqwest as its process-wide transport when a
Reqwest feature is enabled.

## TLS configuration

The default `rustls` feature uses ring and platform certificate verification.
Disable default features and enable `native-tls` to use platform TLS. The
corresponding `opendal` facade features are `http-transport-cyper` and
`http-transport-cyper-native-tls`. Rustls takes precedence when both Cyper TLS
features are enabled.

## License and Trademarks

Licensed under the Apache License, Version 2.0.

Apache OpenDAL, OpenDAL, and the OpenDAL logo are either registered trademarks
or trademarks of The Apache Software Foundation.
