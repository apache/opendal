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

import multiprocessing
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import opendal

SERVER_FALLBACK_SECONDS = 2
TEST_TIMEOUT_SECONDS = 6


class _CoordinatedHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        server = self.server
        with server.active_lock:
            server.active_requests += 1
            server.first_request_started.set()
            if server.active_requests == 2:
                server.two_requests_active.set()

        try:
            server.release_responses.wait(SERVER_FALLBACK_SECONDS)
            body = b"blocking I/O completed"
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        finally:
            with server.active_lock:
                server.active_requests -= 1

    def log_message(self, format_, *args: object) -> None:
        pass


def _serve_coordinated_http(
    port_sender,
    first_request_started,
    two_requests_active,
    release_responses,
) -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _CoordinatedHandler)
    server.active_lock = threading.Lock()
    server.active_requests = 0
    server.first_request_started = first_request_started
    server.two_requests_active = two_requests_active
    server.release_responses = release_responses
    port_sender.send(server.server_address[1])
    port_sender.close()
    server.serve_forever(poll_interval=0.01)


def test_blocking_operator_releases_gil(service_name, operator) -> None:
    if service_name != "memory":
        pytest.skip("run the standalone GIL regression test once")

    context = multiprocessing.get_context("spawn")
    first_request_started = context.Event()
    two_requests_active = context.Event()
    release_responses = context.Event()
    port_receiver, port_sender = context.Pipe(duplex=False)
    server_process = context.Process(
        target=_serve_coordinated_http,
        args=(
            port_sender,
            first_request_started,
            two_requests_active,
            release_responses,
        ),
    )
    server_process.start()
    port_sender.close()

    read_completed = threading.Event()
    python_thread_progressed = threading.Event()
    progress_while_read_blocked = []
    results = []
    errors = []

    try:
        assert port_receiver.poll(TEST_TIMEOUT_SECONDS), (
            "HTTP test server did not start"
        )
        port = port_receiver.recv()
        op = opendal.Operator("http", endpoint=f"http://127.0.0.1:{port}")

        def read(path) -> None:
            try:
                results.append(op.read(path))
            except Exception as error:  # noqa: BLE001
                errors.append(error)
            finally:
                read_completed.set()

        def record_python_progress() -> None:
            request_started = first_request_started.wait(TEST_TIMEOUT_SECONDS)
            progress_while_read_blocked.append(
                request_started and not read_completed.is_set()
            )
            python_thread_progressed.set()

        progress_thread = threading.Thread(target=record_python_progress)
        read_threads = [
            threading.Thread(target=read, args=("first",)),
            threading.Thread(target=read, args=("second",)),
        ]

        progress_thread.start()
        for thread in read_threads:
            thread.start()

        assert two_requests_active.wait(TEST_TIMEOUT_SECONDS), (
            "two blocking reads did not overlap"
        )
        assert python_thread_progressed.wait(TEST_TIMEOUT_SECONDS), (
            "another Python thread did not run during blocking I/O"
        )
        assert progress_while_read_blocked == [True]
        assert all(thread.is_alive() for thread in read_threads)

        release_responses.set()
        for thread in read_threads:
            thread.join(TEST_TIMEOUT_SECONDS)
        progress_thread.join(TEST_TIMEOUT_SECONDS)

        assert all(not thread.is_alive() for thread in read_threads)
        assert not progress_thread.is_alive()
        assert errors == []
        assert results == [b"blocking I/O completed"] * 2
    finally:
        release_responses.set()
        server_process.terminate()
        server_process.join(TEST_TIMEOUT_SECONDS)
        port_receiver.close()
