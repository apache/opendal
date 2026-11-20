# 3FS behavior tests

The `single_node` setup starts a real, single-replica 3FS cluster on a hosted
Ubuntu 22.04 runner. It uses SoftRoCE, FoundationDB, and ordinary data directories;
it requires neither a physical RDMA adapter nor a dedicated disk. Ubuntu 24.04's
current Azure kernel disables RXE, so the behavior planner selects Ubuntu 22.04.

CI caches the pinned 3FS build, runs the focused USRBIO tests and the standard
behavior suite, and stops the cluster and removes the RXE device afterward.
The first build is expensive; subsequent jobs restore the compiled backend.
The job skips the common setup's standalone RocksDB build because the Rust
service does not depend on it. 3FS builds its own bundled RocksDB.

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

For the read/write matrix, build the example from `core/`. The default profile
requires direct I/O on the FUSE path. `Fs` has no direct-I/O builder option, so a
Linux preload helper adds `O_DIRECT` only to files below the dedicated benchmark
root. It also verifies each intercepted descriptor's flags. This helper is for
benchmarking on a real 3FS mount, not for application use.

```shell
cargo build --release -p opendal-service-hf3fs --example bench_matrix
cc -shared -fPIC -O2 -Wall -Wextra -Werror \
  ../.github/services/hf3fs/bench_direct.c -o /tmp/opendal-bench-direct.so -ldl -pthread
OPENDAL_BENCH_DIRECT_ROOT="$OPENDAL_HF3FS_ROOT" \
  LD_PRELOAD=/tmp/opendal-bench-direct.so \
  target/release/examples/bench_matrix > matrix.csv
```

The benchmark refuses to label a run `direct` unless the helper covers its root.
For a new client deployment, verify the data-file opens with `strace -f -e openat`
and check page residency with `mincore` before interpreting results. The helper
intercepts libc `open` and `open64`; it does not cover arbitrary direct syscalls
or relative `openat` calls. It prints the verified read/write open counts on exit.
Direct I/O bypasses the client's kernel page cache, not storage-server caches.

Both services use one Operator, the same 1 GiB input file, application block
sizes of 4 KiB, 64 KiB, 1 MiB, and 4 MiB, and 1, 4, 16, or 64 concurrent workers.
Each worker issues one application operation at a time, with read and write
concurrency set to one. Both paths split application operations into service
chunks of `min(BS, 1 MiB)`; `Hf3fs` uses that native block size and queue depth 64
per direction. Kernel and server request splitting can still differ. Internal
threads, copies, and ring scheduling remain properties of each implementation;
the benchmark does not add Operators to compensate for an implementation limit.
Each worker performs one untimed input read before the start barrier.

Workloads include sequential range reads, random range reads, streaming writes,
single-block object overwrites, and mixed random reads/overwrites with 70% or
50% reads by operation count. Mixed workloads use separate input and output
files; they do not test random writes within an existing file. Overwrites include
open, truncate, write, close, and fsync. Streaming writers close and synchronize
every 64 MiB and at the end. Both services buffer writes, so streaming write
latency measures API admission; throughput includes all writes and final
synchronization. These latency columns are not device completion latencies.

For application-level cache comparisons, run without the preload and explicitly
set `BENCH_CACHE=buffered` or `BENCH_CACHE=hot`. The latter warms the input through
an existing `Fs` reader after all workers open the file. USRBIO bypasses that
file cache in every profile. Keep buffered/hot results separate from the direct
comparison; they do not isolate data-path performance. CSV rows include the
cache profile and service chunk size to preserve these distinctions.

Each case runs for 2 seconds and repeats 4 times. Rounds must be a positive even
number so each service runs first equally often. Set `BENCH_SECONDS`,
`BENCH_ROUNDS`, `BENCH_BS` (bytes), `BENCH_CONCURRENCY`, or `BENCH_WORKLOADS` to
override the defaults; the last three accept comma-separated lists. Use longer
cases and additional rounds to confirm differences found in the matrix.

The benchmark uses constant payloads and checks returned lengths and sampled
data. CPU usage covers only the benchmark process, excluding the FUSE process
and servers. Short cases and shared-cluster load limit the precision of tail
latencies. Temporary files are removed after a successful run.
