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
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from threading import Barrier

from async_s3_benchmark import parse_args, run_benchmark
from boto3 import client as boto3_client
from botocore.config import Config


async def benchmark(args: argparse.Namespace) -> None:
    config = Config(
        max_pool_connections=args.concurrency,
        s3={"addressing_style": "path"} if args.endpoint else None,
    )
    with (
        closing(
            boto3_client(
                "s3",
                region_name=args.region,
                endpoint_url=args.endpoint,
                config=config,
            )
        ) as client,
        ThreadPoolExecutor(max_workers=args.concurrency) as executor,
    ):
        loop = asyncio.get_running_loop()
        # Start every worker before measuring, even if the warmup requests are fast.
        barrier = Barrier(args.concurrency, timeout=10)
        await asyncio.gather(
            *(
                loop.run_in_executor(executor, barrier.wait)
                for _ in range(args.concurrency)
            )
        )

        def write_object(key: str, payload: bytes) -> None:
            client.put_object(Bucket=args.bucket, Key=key, Body=payload)

        def read_object(key: str) -> bytes:
            response = client.get_object(Bucket=args.bucket, Key=key)
            with closing(response["Body"]) as body:
                return body.read()

        await run_benchmark(
            "boto3",
            args,
            lambda key, payload: loop.run_in_executor(
                executor, write_object, key, payload
            ),
            lambda key: loop.run_in_executor(executor, read_object, key),
        )


if __name__ == "__main__":
    asyncio.run(benchmark(parse_args()))
