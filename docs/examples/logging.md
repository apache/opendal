# Logging

OpenDAL has native support for logging.

## Logging Layer

[LoggingLayer](https://opendal.databend.rs/opendal/layers/struct.LoggingLayer.html) will print logs for every operation with the following rules:

- OpenDAL will log in structural way.
- Every operation will start with a started log entry.
- Every operation will finish with the following status:
  - `finished`: the operation is successful.
  - `errored`: the operation returns an expected error like NotFound.
  - `failed`: the operation returns an unexpected error.

Enable logging layer is easy:

```rust
use anyhow::Result;
use opendal::layers::LoggingLayer;
use opendal::Operator;
use opendal::Scheme;

let _ = Operator::from_env(Scheme::Fs)
    .expect("must init")
    .layer(LoggingLayer::default());
```

## Logging Output

OpenDAL is using [`log`](https://docs.rs/log/latest/log/) for logging internally.

To enable logging output, please set `RUST_LOG`:

```shell
RUST_LOG=debug ./app
```

To config logging output, please refer to [Configure Logging](https://rust-lang-nursery.github.io/rust-cookbook/development_tools/debugging/config_log.html):

```shell
RUST_LOG="info,opendal::services=debug" ./app
```
