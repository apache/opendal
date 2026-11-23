# DeepSeek 3FS (Fire-Flyer File System) behavior tests

The `single_node` setup starts a real, single-replica DeepSeek 3FS cluster on a
hosted Ubuntu 22.04 runner. It uses SoftRoCE (`rdma_rxe`), FoundationDB, and
ordinary data directories; it requires neither a physical RDMA adapter nor a
dedicated disk.

CI caches a stripped runtime staging directory (the five 3FS executables,
`libhf3fs_api_shared.so`, staged runtime shared libraries including libfuse
3.16.2, and the `tests/fuse/` launcher and configs), runs the focused USRBIO
tests and the standard behavior suite, and stops the cluster and removes the
RXE device afterward. Build-only apt packages and the libfuse/3FS builds run
only on a cache miss, and FoundationDB downloads are verified against pinned
SHA-256 digests before installation. The job skips the common setup's
standalone RocksDB build because the Rust service does not depend on it; 3FS
builds its own bundled RocksDB.

## Runner plan beyond Ubuntu 22.04

The behavior planner selects `ubuntu-22.04` because GitHub's hosted
`ubuntu-24.04` Azure kernel currently omits `rdma_rxe`. Ubuntu 22.04 leaves
standard support in May 2027. Before that cutoff:

1. Re-check newer hosted GitHub Ubuntu runner images (such as updated
   `ubuntu-24.04` or `ubuntu-26.04` kernels) for `rdma_rxe` support and update
   `.github/scripts/test_behavior/plan.py` and
   `.github/services/hf3fs/single_node/action.yml` once a newer hosted image
   supports SoftRoCE.
2. If hosted runners still lack `rdma_rxe` ahead of Ubuntu 22.04 retirement,
   move the `hf3fs` behavior job to a self-hosted or custom-kernel Linux runner
   with RDMA/SoftRoCE enabled (or a dedicated nightly lane if PR-time hosted
   runners cannot provide RXE).

## Running locally on an existing 3FS deployment

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

The `bench_read` example compares 1 MiB streaming reads at concurrency 1 and 16
against `Fs` on the same file and mount. It warms each path and reports four
measured passes. Results depend on client configuration, caching, and cluster
load.
