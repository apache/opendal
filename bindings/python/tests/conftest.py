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

# from dotenv import load_dotenv
import pytest
import os

import opendal
import pytest


# load_dotenv()
pytest_plugins = ("pytest_asyncio",)

SERVICE_TYPE_INDEX = {
    "service_memory": "memory",
    "service_s3": "s3",
    "service_fs": "fs",
}


def pytest_addoption(parser):
    parser.addoption("--service_type", action="store", default="")


def setup_config(service_name):
    prefix = f"opendal_{service_name}_"
    config = {}
    for key in os.environ.keys():
        if key.lower().startswith(prefix):
            config[key[len(prefix) :].lower()] = os.environ.get(key)

    # Check if current test be enabled.
    test_flag = config.get("test", "")
    if test_flag != "on" and test_flag != "true":
        raise ValueError(f"Service {service_name} test is not enabled.")
    return config


def operator(service_name, setup_config):
    return opendal.Operator(service_name, **setup_config)


def async_operator(service_name, setup_config):
    return opendal.AsyncOperator(service_name, **setup_config)


def pytest_generate_tests(metafunc):
    service_types = metafunc.config.getoption("service_type").split(",")
    fixtures = []
    for service_type in service_types:
        service_name = SERVICE_TYPE_INDEX.get(service_type)
        if service_name is None:
            raise ValueError(f"Service {service_type} is not supported.")
        config = setup_config(service_name)
        fixtures.append(
            (
                service_name,
                operator(service_name, config),
                async_operator(service_name, config),
            )
        )
    metafunc.parametrize("service_name, operator, async_operator", fixtures)
