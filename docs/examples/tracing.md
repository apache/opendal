# Tracing

OpenDAL has native support for tracing.

## Tracing Layer

[TracingLayer](https://opendal.databend.rs/opendal/layers/struct.TracingLayer.html) will add tracing for OpenDAL operations.

Enable tracing layer requires enable feature `layer-tracing`:

```rust
use anyhow::Result;
use opendal::layers::TracingLayer;
use opendal::Operator;
use opendal::Scheme;

let _ = Operator::from_env(Scheme::Fs)
    .expect("must init")
    .layer(TracingLayer);
```

## Tracing Output

OpenDAL is using [`tracing`](https://docs.rs/tracing/latest/tracing/) for tracing internally.

To enable tracing output, please init one of the subscribers that `tracing` supports.

For example:

```rust
extern crate tracing;

let my_subscriber = FooSubscriber::new();
tracing::subscriber::set_global_default(my_subscriber)
    .expect("setting tracing default failed");
```

For real-world usage, please take a look at [`tracing-opentelemetry`](https://crates.io/crates/tracing-opentelemetry).
