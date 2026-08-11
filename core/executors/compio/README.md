# Apache OpenDAL™ Compio Executor

`opendal-executor-compio` runs OpenDAL background tasks and timers on a Compio dispatcher.

## Use through `opendal`

Enable the executor through the `opendal` facade:

```shell
cargo add opendal --features executors-compio
```

The executor is available as `opendal::executors::CompioExecutor`.

## Use with `opendal-core`

Add the split crates directly:

```shell
cargo add opendal-core opendal-executor-compio
```

Pass `CompioExecutor` to `Executor::with` when constructing an operation context.

## License

Licensed under the Apache License, Version 2.0.
