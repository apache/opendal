# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import argparse
import asyncio
import os
from collections.abc import Awaitable, Callable
from hashlib import sha256
from statistics import median
from time import perf_counter
from uuid import uuid4

PAYLOAD_SIZES = (4096, 8192, 16384, 32768, 262144, 524288, 16777216)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark concurrent S3 object I/O.")
    parser.add_argument("--bucket", default=os.getenv("AWS_S3_BUCKET"))
    parser.add_argument(
        "--region",
        default=os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION"),
    )
    parser.add_argument(
        "--endpoint",
        default=os.getenv("AWS_ENDPOINT")
        or os.getenv("AWS_ENDPOINT_URL_S3")
        or os.getenv("AWS_ENDPOINT_URL"),
    )
    parser.add_argument("--concurrency", type=int, default=7, help="Objects per batch.")
    parser.add_argument("--rounds", type=int, default=3, help="Timed batches per size.")
    parser.add_argument(
        "--warmups", type=int, default=1, help="Warmup batches per size."
    )
    args = parser.parse_args()
    if not args.bucket:
        parser.error("set --bucket or AWS_S3_BUCKET")
    if not args.region:
        parser.error("set --region, AWS_REGION, or AWS_DEFAULT_REGION")
    for option in ("concurrency", "rounds", "warmups"):
        if getattr(args, option) < 1:
            parser.error(f"--{option} must be at least 1")
    return args


async def run_round(
    keys: list[str],
    payload: bytes,
    write: Callable[[str, bytes], Awaitable[None]],
    read: Callable[[str], Awaitable[bytes]],
) -> tuple[float, float]:
    size = len(payload)
    expected_digest = sha256(payload).hexdigest()

    started = perf_counter()
    await asyncio.gather(*(write(key, payload) for key in keys))
    write_ms = (perf_counter() - started) * 1000

    started = perf_counter()
    results = await asyncio.gather(*(read(key) for key in keys))
    read_ms = (perf_counter() - started) * 1000

    for key, result in zip(keys, results, strict=True):
        digest = sha256(result).hexdigest()
        if len(result) != size or digest != expected_digest:
            msg = (
                f"{key}: expected {size} bytes with SHA-256 {expected_digest}, "
                f"got {len(result)} bytes with SHA-256 {digest}"
            )
            raise ValueError(msg)

    return write_ms, read_ms


async def run_benchmark(
    client_name: str,
    args: argparse.Namespace,
    write: Callable[[str, bytes], Awaitable[None]],
    read: Callable[[str], Awaitable[bytes]],
) -> None:
    prefix = f"benchmark/{client_name}/{uuid4().hex}"
    print(
        f"client={client_name} prefix={prefix} objects_per_batch={args.concurrency} "
        f"rounds={args.rounds} warmups={args.warmups}",
        flush=True,
    )
    for size in PAYLOAD_SIZES:
        keys = [f"{prefix}/{size}/{index}" for index in range(args.concurrency)]
        write_times = []
        read_times = []
        for round_index in range(args.warmups + args.rounds):
            # Change the payload each round so stale data fails verification.
            marker = round_index.to_bytes(8, "little")
            payload = marker * (size // len(marker))
            write_ms, read_ms = await run_round(keys, payload, write, read)
            if round_index >= args.warmups:
                write_times.append(write_ms)
                read_times.append(read_ms)

        for operation, times in (("write", write_times), ("read", read_times)):
            samples = ", ".join(f"{elapsed:.3f}" for elapsed in times)
            print(
                f"size_bytes={size} operation={operation} "
                f"median_batch_ms={median(times):.3f} batch_ms=[{samples}]",
                flush=True,
            )
