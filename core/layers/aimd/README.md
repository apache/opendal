# Apache OpenDAL™ AIMD Layer

`AimdLayer` adapts HTTP request rates to storage-service throttling. It combines
an additive-increase/multiplicative-decrease controller with a token bucket for
each of four independent categories: read (including stat), write (including
copy, compose, create_dir, rename, and restore), delete, and list.

## Usage

Enable the facade feature `layers-aimd` and use
`opendal::layers::{AimdConfig, AimdLayer}`. With split crates, add
`opendal-layer-aimd` alongside `opendal-core`:

```rust
use opendal_core::{Operator, Result};
use opendal_layer_aimd::{AimdConfig, AimdLayer};

fn configure(operator: Operator) -> Result<Operator> {
    let aimd = AimdLayer::new(AimdConfig {
        initial_rate: 100.0,
        min_rate: 1.0,
        max_rate: 1000.0,
        additive_increment: 10.0,
        burst: 10,
        ..Default::default()
    })?;

    Ok(operator.layer(aimd))
}
```

Add AIMD before retry so it observes each attempt's errors. AIMD does not own
retries, modify errors, or replace bandwidth and concurrency limits. Place a
bandwidth throttle outside AIMD so locally generated `RateLimited` errors do
not reduce the backend budget.

Cloning a layer shares its budgets across operators. Construct separate layers
for independent quotas. Each category receives the configured rate and burst;
`max_rate` is not a combined ceiling across all four categories.

## Admission and feedback

The HTTP wrapper takes one token before forwarding each request with an OpenDAL
`Operation` extension. Pages, multipart parts, and batch-delete requests each
consume their own tokens. Response chunks and entries buffered from a page do
not consume extra tokens. Requests without this extension, presigning, and
non-HTTP I/O bypass admission. Requests hidden inside a custom transport, such
as automatic retries or redirects, are outside this boundary.

Parsed `ErrorKind::RateLimited` errors observed by the service and I/O wrappers
mark a feedback window as throttled. At the next window transition, its rate is
multiplied by `decrease_factor`, bounded by `min_rate`. Otherwise, a window with
HTTP responses adds `additive_increment`, bounded by `max_rate`. Empty windows
do not change the rate. Defaults are 2000 requests/s initially, a range of
1–5000 requests/s, a one-second window, an increment of 300 requests/s, a
multiplier of 0.5, and a burst of 100 requests per category.

HTTP completion and parsed errors occur at different boundaries. Feedback may
be delayed until an operation propagates its error, and errors consumed by an
inner layer are not visible. The controller uses the presence of a throttle
signal, not a ratio of HTTP requests to operation errors. Other errors do not
reduce the rate; an HTTP error response still establishes activity. A failed
connection alone does not establish activity.

Feedback belongs to the enclosing storage operation's budget. A composite
operation may send requests in other categories, such as stat requests during
a copy. Admission follows each request's marker, while semantic feedback follows
the enclosing operation; errors are not correlated with individual HTTP requests.

Waiters use a FIFO queue. Cancelling a waiter removes its queue position without
leaving token debt, and rate changes update pending waits. Buckets start full;
only a request admitted to the next transport consumes a token. The layer does
not spawn background tasks. Waiting uses Tokio timers by default and requires
an active Tokio runtime with time enabled. Use `AimdLayer::with_sleep` to supply
a function that accepts a `Duration` and returns a future completing after that
duration. A custom sleep allows admission to run without a Tokio runtime;
synchronization uses asyncband. Rate calculations use `std::time::Instant`,
so replacing sleep does not replace the controller's clock.

Services sign requests before transport admission. Bound operation duration
when a queue could outlive the service's signature validity period. Configure
transport wrappers to forward to their inner transport, preserving this layer.
This mechanism provides local adaptive pacing, not a distributed quota or a
promise of fair sharing between processes.

## License

Licensed under the Apache License, Version 2.0.
