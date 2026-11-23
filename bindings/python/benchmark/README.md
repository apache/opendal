# OpenDAL Python Bindings Benchmark

## Async S3 benchmarks

The OpenDAL and Boto3 scripts use the same seven payload sizes: 4, 8, 16, 32,
256, and 512 KiB, and 16 MiB. Each round concurrently writes a batch of objects
of one size, then concurrently reads the same objects in full. Each script
reuses one event loop and one client for the run. Boto3 uses a thread pool with
one worker per concurrent operation and keeps the pool alive across rounds.

For each size and operation, the scripts report the elapsed time for each timed
batch and the median, in milliseconds. Each duration includes scheduling the
batch and waiting for all operations to complete. Client and thread-pool setup,
warmup batches, payload generation, and verification are outside the reported
timings. Every read, including warmups, must match the expected length and
SHA-256 digest for its round.

From `bindings/python`, build the binding from the revision to benchmark:

```shell
uv sync --group benchmark
uv run --group benchmark maturin develop --release
```

Use an existing bucket with credentials that can put and get objects. Both
clients use their standard credential providers, including `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN` for temporary credentials.

| Option | Environment fallback | Default |
| --- | --- | --- |
| `--bucket` | `AWS_S3_BUCKET` | Required |
| `--region` | `AWS_REGION`, then `AWS_DEFAULT_REGION` | Required |
| `--endpoint` | `AWS_ENDPOINT`, then `AWS_ENDPOINT_URL_S3`, then `AWS_ENDPOINT_URL` | Client default |
| `--concurrency` | | 7 objects per batch |
| `--rounds` | | 3 timed batches per size |
| `--warmups` | | 1 warmup batch per size |

Run both scripts with identical settings. Each run overwrites only its own
objects under the prefix printed at startup. The objects remain in the bucket
and can be removed by prefix after the run.

### Local MinIO smoke run

Start MinIO and create a bucket:

```shell
export AWS_ACCESS_KEY_ID=opendal-bench
export AWS_SECRET_ACCESS_KEY=opendal-local-benchmark

docker run --rm -d --name opendal-benchmark-minio -p 127.0.0.1:9000:9000 \
  -e MINIO_ROOT_USER="$AWS_ACCESS_KEY_ID" \
  -e MINIO_ROOT_PASSWORD="$AWS_SECRET_ACCESS_KEY" \
  minio/minio:RELEASE.2025-01-20T14-49-07Z server /data
curl --fail --retry 10 --retry-connrefused http://127.0.0.1:9000/minio/health/ready
docker exec opendal-benchmark-minio mc alias set local http://127.0.0.1:9000 \
  "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY"
docker exec opendal-benchmark-minio mc mb local/opendal
```

From `bindings/python`, run both benchmarks:

```shell
export AWS_ENDPOINT=http://127.0.0.1:9000
export AWS_REGION=us-east-1
export AWS_S3_BUCKET=opendal

uv run --group benchmark benchmark/async_opendal_benchmark.py --concurrency 2 --rounds 2
uv run --group benchmark benchmark/async_origin_s3_benchmark_with_threads.py --concurrency 2 --rounds 2

docker stop opendal-benchmark-minio
```

## Blocking Operator GIL benchmark

`blocking_gil_benchmark.py` uses a local HTTP subprocess with a configurable
response delay, so it does not require service credentials. It reports the
median and p95 latency for one blocking read, two sequential reads, and two
reads started from independent Python threads.

From `bindings/python`, build the binding from the revision to measure, then
run:

```shell
uv run maturin develop --release
uv run benchmark/blocking_gil_benchmark.py \
  --delay-ms 50 --iterations 30 --warmups 3
```

Run the same command from an unchanged baseline checkout to compare
single-thread overhead and concurrency speedup with identical local I/O.

## File Open and Read over HTTP

The repository's nginx fixture provides a reproducible benchmark for opening a
Python file object and reading its first byte:

```shell
docker compose -f fixtures/http/docker-compose-nginx.yml up -d --wait

cd bindings/python
uv run maturin develop
uv run python benchmark/file_open_read_benchmark.py
```

The benchmark reports the per-operation latency for synchronous and asynchronous
file APIs. The nginx access log can be used to verify the corresponding HTTP
request count.
