# 3FS behavior tests

The `single_node` setup starts a real, single-replica 3FS cluster on a hosted
Ubuntu 22.04 runner. It uses SoftRoCE, FoundationDB, and ordinary data directories;
it requires neither a physical RDMA adapter nor a dedicated disk. Ubuntu 24.04's
current Azure kernel disables RXE, so the behavior planner selects Ubuntu 22.04.

CI caches the pinned 3FS build, runs the focused USRBIO tests and the standard
behavior suite, and stops the cluster and removes the RXE device afterward.
The first build is expensive; subsequent jobs restore the compiled backend.

To test an existing Linux deployment, use a real 3FS FUSE mount and the matching
USRBIO client library.

Create a dedicated test directory on the mount. Set `OPENDAL_HF3FS_ROOT` to its
absolute path and `OPENDAL_HF3FS_LIBRARY_PATH` to `libhf3fs_api_shared.so`.
Set `OPENDAL_TEST=hf3fs` and `OPENDAL_DISABLE_RANDOM_ROOT=true`, since the service
requires its root to exist before construction.

From `core/`, run:

```shell
cargo test --test behavior --features tests,services-hf3fs
cargo test -p opendal-service-hf3fs --test real_service -- --ignored
cargo run --release -p opendal-service-hf3fs --example bench_read
```

The benchmark compares 1 MiB streaming reads at concurrency 1 and 16 against
`Fs` on the same file and mount. It warms each path and reports four measured
passes. Results depend on client configuration, caching, and cluster load.
