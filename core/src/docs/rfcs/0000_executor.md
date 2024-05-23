- Proposal Name: `executor`
- Start Date: 2024-05-23
- RFC PR: [apache/opendal#0000](https://github.com/apache/opendal/pull/0000)
- Tracking Issue: [apache/opendal#0000](https://github.com/apache/opendal/issues/0000)

# Summary

Add executor in opendal to allow running tasks concurrently in background.

# Motivation

OpenDAL offers top-tier support for concurrent execution, allowing tasks to run simultaneously in the background. Users can easily enable concurrent file read/write operations with just one line of code:

```diff
 let w = op
     .writer_with(path)
     .chunk(8 * 1024 * 1024) // 8 MiB per chunk
+    .concurrent(16) // 16 concurrent tasks
     .await?;
```

However, the execution of those tasks relies on users continuously calling `write`. They cannot run tasks concurrently in the background. (I explained the technical details in the `Rationale and alternatives` section.)

This can result in the following issues:

- Task latency may increase as tasks are not executed until the task queue is full.
- Memory usage may be high because all chunks must be held in memory until the task is completed.

I propose introducing an executor in OpenDAL to enable concurrent background task execution. The executor will autonomously manage the tasks without requiring user intervention for progress.

# Guide-level explanation

OpenDAL will add a new `Executor` struct to manage concurrent tasks.

```rust
pub struct Executor {
    ...
}

impl Executor {
    /// Create a new tokio based executor.
    pub fn new() -> Self { ... }
    
    /// Create a new executor with given execute impl.
    pub fn with(exec: Arc<dyn Execute>) -> Self { ... }

    /// Run given future in background immediately.
    pub fn execute<F>(&self, f: F) -> RemoteHandle<F::Output>
    where
        F: Future + Send + 'static,
    {
        ...
    }
}
```

The `Executor` uses the `tokio` runtime by default but users can also provide their own runtime by:

```rust
pub trait Execute {
    fn execute<F>(&self, f: BoxedFuture<()>) -> Result<()>;
}
```

Users can set executor in `OpWrite` / `OpRead` to enable concurrent background task execution:

```rust
+ let exec = Executor::new();
  let w = op
      .writer_with(path)
      .chunk(8 * 1024 * 1024) // 8 MiB per chunk
      .concurrent(16) // 16 concurrent tasks
+     .executor(exec) // Use specfied executor
      .await?;
```

Specifying an executor every time is cumbersome. Users can also set a global executor for given operator:

```rust
+ let exec = Executor::new();
+ let op = op.with_default_executor(exec);

  let w = op
      .writer_with(path)
      .chunk(8 * 1024 * 1024) // 8 MiB per chunk
      .concurrent(16) // 16 concurrent tasks
      .await?;
```

# Reference-level explanation

# Drawbacks

# Rationale and alternatives

## Why `ConcurrentFutures` doesn't work?

# Prior art

# Unresolved questions

# Future possibilities

## Blocking Executor

This proposal mainly focuses on async tasks. However, we can also consider adding blocking support to `Executor`. Users can use concurrent tasks in blocking context too:

```rust
 let w = op
     .writer_with(path)
     .chunk(8 * 1024 * 1024) // 8 MiB per chunk
+    .concurrent(16) // 16 concurrent tasks
     .do()?;
```
