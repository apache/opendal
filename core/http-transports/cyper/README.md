# opendal-http-transport-cyper

Cyper-based HTTP transport for [Apache OpenDAL](https://opendal.apache.org).

`CyperTransport` runs HTTP requests on Compio. It keeps one client and
connection pool per runtime thread.

Poll each request future and response body on one Compio runtime thread. Moving
a partially polled request or response body to another runtime thread is
unsupported.

## Use through `opendal`

Disable the Tokio and reqwest defaults, then enable the Compio executor and
Cyper transport:

```toml
[dependencies]
opendal = { version = "0.58", default-features = false, features = [
    "auto-register-services",
    "executors-compio",
    "http-transport-cyper",
] }
```

`http-transport-cyper` uses Rustls and platform certificate verification. Use
`http-transport-cyper-native-tls` instead to select the platform TLS backend.

## Use with the split crates

Attach the transport and executor to an operation context when an application
manages its own Compio dispatcher:

```rust
use std::sync::Arc;

use compio::dispatcher::Dispatcher;
use opendal_core::Executor;
use opendal_core::HttpTransporter;
use opendal_core::OperationContext;
use opendal_core::Operator;
use opendal_executor_compio::CompioExecutor;
use opendal_http_transport_cyper::CyperTransport;
use opendal_service_http::Http;

# async fn read() -> Result<(), Box<dyn std::error::Error>> {
let dispatcher = Arc::new(Dispatcher::new()?);
let context = OperationContext::from_parts(
    HttpTransporter::new(CyperTransport::new()),
    Executor::with(CompioExecutor::new(dispatcher)),
);
let op = Operator::new(Http::default().endpoint("https://example.com"))?
    .with_context(context);
let content = op.read("index.html").await?;
# Ok(())
# }
```

## License

Licensed under the Apache License, Version 2.0.
