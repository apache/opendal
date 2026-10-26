# Apache OpenDAL™ 3FS Service

`Hf3fs` reads and writes through the 3FS USRBIO shared-memory interface. Metadata
operations use the mounted filesystem. It requires Linux, a running 3FS FUSE
client, and a compatible `libhf3fs_api_shared.so` implementing the public
[`hf3fs_usrbio.h` API](https://github.com/deepseek-ai/3FS/blob/main/src/lib/api/hf3fs_usrbio.h).
The library is loaded at runtime; building OpenDAL does not require a 3FS SDK.

Enable `services-hf3fs` in `opendal`, or use the split crates:

```rust,no_run
use opendal_core::{Operator, Result};
use opendal_service_hf3fs::Hf3fs;

fn operator() -> Result<Operator> {
    Operator::new(Hf3fs::default()
        .root("/mnt/3fs/data")
        .library_path("/opt/3fs/lib/libhf3fs_api_shared.so"))
}
```

The root must be an existing absolute directory on 3FS. The service discovers
its mount point. By default, the dynamic loader searches for
`libhf3fs_api_shared.so`; `library_path` selects a specific installation.
Use the client library distributed with your 3FS deployment.

The service supports stat, range reads, streaming writes, create directory,
list, delete, recursive delete, and rename. Writes create parent directories
and replace existing files; `if_not_exists` rejects an existing file.
Closing a writer waits for writes and synchronizes the file. Writes are not
atomic, and failed or aborted writes can leave partial files. Append, native
copy, presign, and rollback on abort are unsupported. Operations require a
Tokio runtime. The operation context supplies the executor for concurrent writes.

## Memory and concurrency

Each operator owns a read driver and a write driver. Each driver submits and
reaps batches on its own thread. `queue_depth` defaults to 32 requests per ring;
`block_size` defaults to 1 MiB. Shared memory uses approximately
`3 * queue_depth * block_size`, plus the rings, and request queues are bounded.
Cloned operators share these resources.

Reads return shared-memory-backed buffers without copying while fewer than
`queue_depth` slots remain retained by callers. Additional completions copy to
ordinary buffers so collecting large files cannot exhaust the I/O pool and
deadlock. Buffer clones retain their slot until the last clone is dropped,
even after the operator is dropped. Writes copy their input into shared memory.

Cancelling an awaiting future does not cancel submitted native I/O. The driver
retains the file and buffer until completion. If submission or completion
collection fails fatally, the driver stops and reports errors. Since USRBIO
provides no cancellation fence, resources still involved in native I/O remain
allocated until process exit. Rebuild the operator after a driver failure.
A stalled client can also keep its driver and in-flight resources alive;
operation timeouts do not release native buffers.

## Testing on 3FS

Set `OPENDAL_HF3FS_ROOT` to an existing, writable test directory on a real 3FS
mount. Optionally set `OPENDAL_HF3FS_LIBRARY_PATH`. From `core/`, run:

```shell
cargo test -p opendal-service-hf3fs --test real_service -- --ignored
OPENDAL_TEST=hf3fs OPENDAL_DISABLE_RANDOM_ROOT=true cargo test --test behavior --features tests,services-hf3fs
```

The focused tests create and remove their own subdirectories. Use a dedicated
test root for the behavior suite. Neither test command needs a mock service.
