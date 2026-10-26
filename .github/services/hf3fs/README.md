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

For the read/write matrix, run:

```shell
cargo run --release -p opendal-service-hf3fs --example bench_matrix > matrix.csv
```

The matrix uses 4 KiB, 64 KiB, 1 MiB, and 4 MiB application blocks with 1, 4,
16, and 64 concurrent workers. Each worker issues one application operation at
a time. Both services use the same 1 GiB input file. `Hf3fs` uses a queue depth
of 64 per direction and a native block size matching the application block.
`Fs` can split large application reads into smaller native requests.

Workloads include sequential range reads, random range reads, streaming writes,
single-block object overwrites, and mixed random reads/overwrites with 70% or
50% reads by operation count. Mixed workloads use separate input and output
files; they do not test random writes within an existing file. Overwrites include
open, truncate, write, close, and fsync. Streaming writers close and synchronize
every 64 MiB and at the end; their write latency measures API admission, while
throughput includes final synchronization. These latency columns are not device
completion latencies.

The default run preserves each service's normal cache behavior. Reopening the
file on some 3FS deployments invalidates its kernel page cache; initial prewarming
does not guarantee cache hits for new readers. Set `BENCH_WARM_READS=true` to
prewarm the whole input through an existing `Fs` reader after all workers open
the file. USRBIO bypasses that file cache. Neither profile isolates uncached
transport performance. The benchmark uses constant payloads and checks returned
lengths and sampled data. CPU usage covers the benchmark process, excluding the
FUSE process and servers.

Each case runs for 2 seconds and repeats 3 times, reversing service order on
alternate rounds. CSV rows contain counts, throughput, IOPS, latency percentiles,
and CPU usage. Set `BENCH_SECONDS`, `BENCH_ROUNDS`, `BENCH_BS` (bytes),
`BENCH_CONCURRENCY`, or `BENCH_WORKLOADS` to override the defaults; the last three
accept comma-separated lists. Short cases and shared-cluster load limit the
precision of tail latencies. Temporary files are removed after a successful run.
