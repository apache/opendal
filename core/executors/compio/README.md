# Apache OpenDAL™ Compio Executor

`opendal-executor-compio` runs OpenDAL background tasks on a Compio dispatcher and provides timers that
work independently of its task queue.

## Use through `opendal`

Enable the executor through the `opendal` facade:

```shell
cargo add opendal --features executors-compio
```

The executor is available as `opendal::executors::CompioExecutor` on native
targets. The Compio executor requires Rust 1.95 or later.

## Use with `opendal-core`

Add the split crates directly:

```shell
cargo add opendal-core opendal-executor-compio
cargo add compio@0.19 --features dispatcher
```

Pass `CompioExecutor` to `Executor::with` when constructing an operation context.

```rust
# fn main() -> std::io::Result<()> {
use std::sync::Arc;

use compio::dispatcher::Dispatcher;
use opendal_core::{Executor, Operator, services::Memory};
use opendal_executor_compio::CompioExecutor;

let dispatcher = Arc::new(Dispatcher::new()?);
let executor = Executor::with(CompioExecutor::new(dispatcher));
let op = Operator::new(Memory::default()).expect("memory operator");
let context = op.context().with_executor(executor);
let op = op.with_context(context);
# drop(op);
# Ok(())
# }
```

The executor schedules background work and supplies timers for `TimeoutLayer`.
The selected service, HTTP transport, and other layers keep their runtime
requirements. `Fs` and the Reqwest HTTP transport require a Tokio runtime
where their futures are polled. `RetryLayer` and `TailCutLayer` also use Tokio
for delays and deadlines on native targets.

The dispatcher is shared with every clone of the executor. Once the last
reference is dropped, the dispatcher closes its submission channel. To wait for
its workers explicitly, keep a dispatcher reference and call `Dispatcher::join`
when you have sole ownership.

## License

Licensed under the Apache License, Version 2.0.
