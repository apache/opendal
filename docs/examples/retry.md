# Retry

OpenDAL has native support for retry.

## Retry Layer

[RetryLayer](https://opendal.databend.rs/opendal/layers/struct.RetryLayer.html) will add retry for OpenDAL operations.

```rust
use anyhow::Result;
use backon::ExponentialBackoff;
use opendal::layers::RetryLayer;
use opendal::Operator;
use opendal::Scheme;

let _ = Operator::from_env(Scheme::Fs)
    .expect("must init")
    .layer(RetryLayer::new(ExponentialBackoff::default()));
```

## Retry Logic

> For more information about retry design, please refer to [RFC-0247: Retryable Error](https://opendal.databend.rs/rfcs/0247-retryable-error.html).

Services will return `io::ErrorKind::Interrupt` if the error is retryable. And operator will retry `io::ErrorKind::Interrupt` errors until retry times reached.
