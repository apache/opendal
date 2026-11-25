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

from async_s3_benchmark import parse_args, run_benchmark

import opendal


async def benchmark(args: argparse.Namespace) -> None:
    options = {"bucket": args.bucket, "region": args.region}
    if args.endpoint:
        options["endpoint"] = args.endpoint
    operator = opendal.AsyncOperator("s3", **options)
    await run_benchmark("opendal", args, operator.write, operator.read)


if __name__ == "__main__":
    asyncio.run(benchmark(parse_args()))
