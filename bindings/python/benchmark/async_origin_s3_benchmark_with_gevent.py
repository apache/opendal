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

import timeit

from gevent import monkey

monkey.patch_all()

import gevent
import greenify
from boto3 import client as boto3_client
from mypy_boto3_s3 import S3Client
from pydantic_settings import BaseSettings

greenify.greenify()


class Config(BaseSettings):
    aws_region: str
    aws_endpoint: str
    aws_s3_bucket: str
    aws_access_key_id: str
    aws_secret_access_key: str


SETTINGS = Config()

S3_CLIENT: S3Client = boto3_client(
    "s3",
    region_name=SETTINGS.aws_region,
    endpoint_url=SETTINGS.aws_endpoint,
    aws_access_key_id=SETTINGS.aws_access_key_id,
    aws_secret_access_key=SETTINGS.aws_secret_access_key,
)


TEST_CASE = [
    {"name": "4kb", "data": b"0" * 4096},
    {"name": "8kb", "data": b"0" * 8192},
    {"name": "16kb", "data": b"0" * 16384},
    {"name": "32kb", "data": b"0" * 32768},
    {"name": "256kb", "data": b"0" * 262144},
    {"name": "512kb", "data": b"0" * 524288},
    {"name": "16mb", "data": b"0" * 16777216},
]


def async_origin_s3_write():
    tasks = []
    for case in TEST_CASE:
        tasks.append(
            gevent.spawn(
                S3_CLIENT.put_object,
                **dict(
                    Bucket=SETTINGS.aws_s3_bucket,
                    Key=f"benchmark/async_write/{case['name']}",
                    Body=case["data"],
                ),
            )
        )
    gevent.joinall(tasks)


def async_origin_s3_read():
    tasks = []
    for case in TEST_CASE:
        tasks.append(
            gevent.spawn(
                S3_CLIENT.get_object,
                **dict(
                    Bucket=SETTINGS.aws_s3_bucket,
                    Key=f"benchmark/async_write/{case['name']}",
                ),
            )
        )
    gevent.joinall(tasks)
    read_tasks = []
    for task in tasks:
        read_tasks.append(gevent.spawn(task.value["Body"].read))
    gevent.joinall(read_tasks)


def async_s3_benchmark():
    for func in (async_origin_s3_write, async_origin_s3_read):
        print(f"{func.__name__}: {timeit.timeit(func, number=3)}")


if __name__ == "__main__":
    async_s3_benchmark()
