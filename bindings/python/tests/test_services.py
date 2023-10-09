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

import os
from abc import ABC
from uuid import uuid4
from random import randint

import opendal
import pytest


class AbstractTestSuite(ABC):
    service_name = ""

    def setup_method(self):
        # Read arguments from envs.
        prefix = f"opendal_{self.service_name}_"
        self.config = {}
        for key in os.environ.keys():
            if key.lower().startswith(prefix):
                self.config[key[len(prefix) :].lower()] = os.environ.get(key)

        # Check if current test be enabled.
        test_flag = self.config.get("test", "")
        if test_flag != "on" and test_flag != "true":
            raise ValueError(f"Service {self.service_name} test is not enabled.")

        self.operator = opendal.Operator(self.service_name, **self.config)

    def test_sync_read(self):
        size = randint(1, 1024)
        filename = f"random_file_{str(uuid4())}"
        content = os.urandom(size)
        self.operator.write(filename, content)

        read_content = self.operator.read(filename)
        assert read_content is not None
        assert read_content == content

    def test_sync_read_stat(self):
        size = randint(1, 1024)
        filename = f"random_file_{str(uuid4())}"
        content = os.urandom(size)
        self.operator.write(filename, content)

        metadata = self.operator.stat(filename)
        assert metadata is not None
        assert metadata.content_length == len(content)
        assert metadata.mode.is_file()

    def test_sync_read_not_exists(self):
        with pytest.raises(FileNotFoundError):
            self.operator.read(str(uuid4()))

    def test_sync_write(self):
        size = randint(1, 1024)
        filename = f"test_file_{str(uuid4())}.txt"
        content = os.urandom(size)
        size = len(content)
        self.operator.write(filename, content)
        metadata = self.operator.stat(filename)
        assert metadata is not None
        assert metadata.mode.is_file()
        assert metadata.content_length == size

        self.operator.delete(filename)

    def test_sync_write_with_non_ascii_name(self):
        size = randint(1, 1024)
        filename = f"❌😱中文_{str(uuid4())}.test"
        content = os.urandom(size)
        size = len(content)
        self.operator.write(filename, content)
        metadata = self.operator.stat(filename)
        assert metadata is not None
        assert metadata.mode.is_file()
        assert metadata.content_length == size

        self.operator.delete(filename)

    def test_sync_create_dir(self):
        path = f"test_dir_{str(uuid4())}/"
        self.operator.create_dir(path)
        metadata = self.operator.stat(path)
        assert metadata is not None
        assert metadata.mode.is_dir()

        self.operator.delete(path)

    def test_sync_delete(self):
        size = randint(1, 1024)
        filename = f"test_file_{str(uuid4())}.txt"
        content = os.urandom(size)
        size = len(content)
        self.operator.write(filename, content)
        self.operator.delete(filename)
        with pytest.raises(FileNotFoundError):
            self.operator.stat(filename)


class TestS3(AbstractTestSuite):
    service_name = "s3"


class TestFS(AbstractTestSuite):
    service_name = "fs"


class TestMemory(AbstractTestSuite):
    service_name = "memory"
