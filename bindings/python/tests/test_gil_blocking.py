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

import contextlib
import threading
import time
from collections.abc import Callable
from uuid import uuid4

import pytest


def _test_gil_release_with_callback(io_func: Callable[[], None]) -> bool:
    callback_executed = threading.Event()
    io_started = threading.Event()
    io_finished = threading.Event()

    def io_thread() -> None:
        io_started.set()
        io_func()
        io_finished.set()

    def checker_thread() -> None:
        # Wait for IO to start
        io_started.wait(timeout=5.0)
        time.sleep(0.001)
        for _ in range(1000):
            if io_finished.is_set():
                break
            callback_executed.set()
            time.sleep(0.0001)

    t_io = threading.Thread(target=io_thread)
    t_checker = threading.Thread(target=checker_thread)

    t_io.start()
    t_checker.start()

    t_io.join(timeout=30.0)
    t_checker.join(timeout=5.0)

    return callback_executed.is_set()


@pytest.mark.need_capability("write", "delete")
def test_sync_file_write_gil_release(service_name, operator, async_operator):
    """Test that GIL is released during file write operations.

    Related issue: https://github.com/apache/opendal/issues/6855.
    """
    filename = f"test_gil_write_{uuid4()}.bin"
    # Use 1MB data to stay within service limits (e.g., etcd has ~10MB limit)
    # Write multiple times to ensure I/O takes measurable time
    data = b"x" * (1024 * 1024)  # 1MB

    def do_write() -> None:
        with operator.open(filename, "wb") as f:
            for _ in range(10):
                f.write(data)

    try:
        result = _test_gil_release_with_callback(do_write)
        assert result, "Main thread was blocked during write (GIL not released)"
    finally:
        with contextlib.suppress(Exception):
            operator.delete(filename)


@pytest.mark.need_capability("write", "read", "delete")
def test_sync_file_read_gil_release(service_name, operator, async_operator):
    """Test that GIL is released during file read operations.

    Related issue: https://github.com/apache/opendal/issues/6855.
    """
    filename = f"test_gil_read_{uuid4()}.bin"
    # Use 1MB data to stay within service limits (e.g., etcd has ~10MB limit)
    data = b"x" * (1024 * 1024)  # 1MB

    operator.write(filename, data)

    def do_read() -> None:
        with operator.open(filename, "rb") as f:
            for _ in range(10):
                f.seek(0)
                _ = f.read()

    try:
        result = _test_gil_release_with_callback(do_read)
        assert result, "Main thread was blocked during read (GIL not released)"
    finally:
        with contextlib.suppress(Exception):
            operator.delete(filename)
