# Apache OpenDAL™ Tokio Executor

`opendal-executor-tokio` runs OpenDAL background tasks and timers on Tokio.

## Use through `opendal`

Applications should normally enable this executor through the `opendal` facade with the
`executors-tokio` feature. The feature is enabled by default.

```shell
cargo add opendal --features executors-tokio
```

The executor is available as `opendal::executors::TokioExecutor`.

## Use with `opendal-core`

Add the split crates directly:

```shell
cargo add opendal-core opendal-executor-tokio
```

Pass `TokioExecutor` to `Executor::with` when constructing an operation context.

## License

Licensed under the Apache License, Version 2.0.
