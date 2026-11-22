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

import asyncio
import contextlib
import multiprocessing
from collections import Counter
from collections.abc import Callable
from contextvars import Context
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from multiprocessing.managers import ListProxy
from multiprocessing.synchronize import Event as EventType
from urllib.parse import parse_qs, urlsplit

import pytest

import opendal
from opendal.options import ReadKwargs

CONTENT = b"HelloWorld"
TRUNCATED_CONTENT = b"Hell"


class RequestState:
    def __init__(
        self, requests: ListProxy, slow_started: EventType, slow_release: EventType
    ) -> None:
        self._requests = requests
        self.slow_started = slow_started
        self.slow_release = slow_release

    def _methods(self) -> list[str]:
        return [entry[0] for entry in self._requests]

    def _uploads(self) -> list[bytes]:
        return [entry[1] for entry in self._requests if entry[0] == "PUT"]

    def _reads(self) -> list[tuple[str, str | None]]:
        return [(entry[1], entry[2]) for entry in self._requests if entry[0] == "GET"]

    def _reset(self) -> None:
        self._requests[:] = []
        self.slow_started.clear()
        self.slow_release.clear()


def serve_requests(port_queue, requests, slow_started, slow_release, stop):
    class RequestHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_HEAD(self) -> None:
            requests.append(("HEAD",))
            self.send_response(200)
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", str(len(CONTENT)))
            self.end_headers()

        def do_PUT(self) -> None:
            body = self.rfile.read(int(self.headers["Content-Length"]))
            requests.append(("PUT", body))
            if self.path == "/slow":
                slow_started.set()
                slow_release.wait(timeout=5)
            uploads = [entry for entry in requests if entry[0] == "PUT"]
            status = 403 if self.path == "/retry" and len(uploads) == 1 else 201
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_GET(self) -> None:
            range_header = self.headers.get("Range")
            requests.append(("GET", self.path, range_header))
            content = TRUNCATED_CONTENT if self.path == "/truncated" else CONTENT
            body = content
            status = 200
            if range_header is not None:
                start, end = range_header.removeprefix("bytes=").split("-", 1)
                start = int(start)
                if start >= len(content):
                    body = b""
                    status = 416
                else:
                    end = int(end) if end else len(content) - 1
                    body = content[start : end + 1]
                    status = 206

            self.send_response(status)
            self.send_header("Accept-Ranges", "bytes")
            if self.path in ("/paused-chunks", "/broken-chunks") and status == 200:
                self.send_header("Transfer-Encoding", "chunked")
                self.end_headers()
                with contextlib.suppress(BrokenPipeError, ConnectionResetError):
                    if self.path == "/broken-chunks":
                        self.wfile.write(b"4\r\nHell\r\ninvalid-chunk-size\r\n")
                        self.close_connection = True
                    else:
                        self.wfile.write(b"2\r\nHe\r\n3\r\nllo\r\n")
                        slow_started.set()
                        slow_release.wait(timeout=5)
                        self.wfile.write(b"5\r\nWorld\r\n0\r\n\r\n")
                return
            self.send_header("Content-Length", str(len(body)))
            if status == 206:
                self.send_header("Content-Range", f"bytes {start}-{end}/{len(content)}")
            elif status == 416:
                self.send_header("Content-Range", f"bytes */{len(content)}")
            self.end_headers()

            if self.path == "/slow":
                slow_started.set()
                slow_release.wait(timeout=5)

            with contextlib.suppress(BrokenPipeError, ConnectionResetError):
                self.wfile.write(body)

        def log_message(self, _format: str, *args: object) -> None:
            pass

    class RequestServer(ThreadingHTTPServer):
        daemon_threads = True

    server = RequestServer(("127.0.0.1", 0), RequestHandler)
    server.timeout = 0.1
    port_queue.put(server.server_address[1])
    while not stop.is_set():
        server.handle_request()
    server.server_close()


@pytest.fixture(scope="module")
def request_server():
    context = multiprocessing.get_context("spawn")
    manager = context.Manager()
    requests = manager.list()
    slow_started = context.Event()
    slow_release = context.Event()
    stop = context.Event()
    port_queue = context.Queue()
    process = context.Process(
        target=serve_requests,
        args=(port_queue, requests, slow_started, slow_release, stop),
    )
    process.start()
    port = port_queue.get(timeout=5)
    state = RequestState(requests, slow_started, slow_release)

    yield f"http://127.0.0.1:{port}", state

    stop.set()
    state.slow_release.set()
    process.join(timeout=5)
    if process.is_alive():
        process.terminate()
        process.join(timeout=5)
    manager.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("prefetch", [None, 0])
async def test_read_forwards_response_overrides(request_server, prefetch):
    endpoint, state = request_server
    state._reset()
    config = {
        "endpoint": endpoint,
        "bucket": "fixture-bucket",
        "region": "us-west-2",
        "disable_config_load": "true",
        "access_key_id": "test-key",
        "secret_access_key": "test-secret",
    }
    operator = opendal.Operator("s3", **config)
    async_operator = opendal.AsyncOperator("s3", **config)
    options: ReadKwargs = {
        "offset": 2,
        "size": 5,
        "content_type": "text/plain",
        "cache_control": "max-age=60",
        "content_disposition": 'attachment; filename="sample.txt"',
    }

    assert operator.read("file", prefetch=prefetch, **options) == b"lloWo"
    assert await async_operator.read("file", prefetch=prefetch, **options) == b"lloWo"
    expected_query = {
        "response-content-type": ["text/plain"],
        "response-cache-control": ["max-age=60"],
        "response-content-disposition": ['attachment; filename="sample.txt"'],
    }
    assert [
        (parse_qs(urlsplit(target).query), byte_range)
        for target, byte_range in state._reads()
    ] == [(expected_query, "bytes=2-6")] * 2


@pytest.mark.asyncio
async def test_read_range_with_prefetch(request_server):
    endpoint, state = request_server
    state._reset()
    operator = opendal.Operator("http", endpoint=endpoint, root="/")
    async_operator = opendal.AsyncOperator("http", endpoint=endpoint, root="/")

    assert (
        operator.read("file", offset=2, size=5, chunk=2, concurrent=2, prefetch=1)
        == b"lloWo"
    )
    assert (
        await async_operator.read(
            "file", offset=2, size=5, chunk=2, concurrent=2, prefetch=1
        )
        == b"lloWo"
    )
    assert Counter(state._reads()) == {
        ("/file", "bytes=2-3"): 2,
        ("/file", "bytes=4-5"): 2,
        ("/file", "bytes=6-6"): 2,
    }


@pytest.mark.asyncio
async def test_read_rejects_prefetch_with_response_overrides():
    operator = opendal.Operator("memory")
    async_operator = opendal.AsyncOperator("memory")
    operator.write("file", CONTENT)
    await async_operator.write("file", CONTENT)

    message = "prefetch cannot be combined with response header overrides"
    with pytest.raises(ValueError, match=message):
        operator.read("file", prefetch=1, content_type="text/plain")
    with pytest.raises(ValueError, match=message):
        await async_operator.read("file", prefetch=1, content_type="text/plain")


def test_sync_file_sequential_read_uses_one_request(request_server):
    endpoint, state = request_server
    state._reset()
    op = opendal.Operator("http", endpoint=endpoint, root="/")

    file = op.open("file", "rb")
    assert state._methods() == []
    assert file.tell() == 0
    assert state._methods() == []

    assert file.read(5) == b"Hello"
    assert file.read() == b"World"
    assert file.read() == b""
    assert state._methods() == ["GET"]

    file.close()
    assert file.closed
    assert state._methods() == ["GET"]
    with pytest.raises(OSError, match="closed file"):
        file.read(1)


def test_sync_file_flush_keeps_small_writes_coalesced(request_server):
    endpoint, state = request_server
    state._reset()
    op = opendal.Operator("webdav", endpoint=endpoint, disable_create_dir="true")
    with op.open("file", "wb") as file:
        for _ in range(1000):
            assert file.write(b"small") == 5
        file.flush()
        file.flush()
        assert not file.closed
        assert state._uploads() == []
        assert file.write(CONTENT) == len(CONTENT)
    assert state._uploads() == [b"small" * 1000 + CONTENT]
    file.close()
    file.flush()
    assert len(state._uploads()) == 1


def test_sync_file_close_error_retains_input_for_retry(request_server):
    endpoint, state = request_server
    state._reset()
    op = opendal.Operator("webdav", endpoint=endpoint, disable_create_dir="true")
    file = op.open("retry", "wb")
    assert file.write(CONTENT) == len(CONTENT)
    file.flush()
    with pytest.raises(opendal.exceptions.PermissionDenied):
        file.close()
    assert not file.closed
    file.close()
    assert file.closed
    assert state._uploads() == [CONTENT, CONTENT]


def test_sync_file_start_and_current_seek_do_not_fetch_length(request_server):
    endpoint, state = request_server
    state._reset()
    op = opendal.Operator("http", endpoint=endpoint, root="/")

    with op.open("file", "rb") as file:
        assert file.seek(5) == 5
        assert state._methods() == []
        assert file.seek(-1, 1) == 4
        assert state._methods() == []
        assert file.read() == b"oWorld"
        assert state._methods() == ["GET"]

    state._reset()
    with op.open("file", "rb") as file:
        assert file.seek(-2, 2) == 8
        assert state._methods() == ["HEAD"]
        assert file.read() == b"ld"
        assert state._methods() == ["HEAD", "GET"]

    state._reset()
    with op.open("file", "rb") as file:
        assert file.read(2) == b"He"
        assert state._methods() == ["GET"]
        assert file.seek(0) == 0
        assert state._methods() == ["GET"]
        assert file.read(2) == b"He"
        assert state._methods() == ["GET", "GET"]


def test_sync_file_seek_past_end_reads_eof(request_server):
    endpoint, state = request_server
    state._reset()
    op = opendal.Operator("http", endpoint=endpoint, root="/")

    with op.open("file", "rb") as file:
        assert file.seek(20) == 20
        assert state._methods() == []
        assert file.read() == b""
        assert file.tell() == 20
        assert state._methods() == ["GET"]
        assert file.read() == b""
        assert state._methods() == ["GET"]

        assert file.seek(-15, 1) == 5
        assert file.read() == b"World"
        assert state._methods() == ["GET", "GET"]


def test_sync_file_offset_past_end_has_empty_logical_length(request_server):
    endpoint, state = request_server
    state._reset()
    op = opendal.Operator("http", endpoint=endpoint, root="/")

    with op.open("file", "rb", offset=20) as file:
        assert state._methods() == []
        assert file.read() == b""
        assert state._methods() == ["GET"]
        assert file.seek(0, 2) == 0
        assert state._methods() == ["GET", "HEAD"]


def test_sync_chunked_file_propagates_truncation_error(request_server):
    endpoint, state = request_server
    state._reset()
    op = opendal.Operator("http", endpoint=endpoint, root="/")

    with op.open("truncated", "rb", chunk=4) as file:
        assert state._methods() == ["HEAD"]
        with pytest.raises(OSError, match="RangeNotSatisfied"):
            file.read()
        assert state._methods() == ["HEAD", "GET", "GET"]


def test_sync_file_bounded_range_does_not_stat(request_server):
    endpoint, state = request_server
    state._reset()
    op = opendal.Operator("http", endpoint=endpoint, root="/")

    with op.open("file", "rb", offset=2, size=5) as file:
        assert state._methods() == []
        assert file.seek(-2, 2) == 3
        assert state._methods() == []
        assert file.read() == b"Wo"
        assert state._methods() == ["GET"]


@pytest.mark.asyncio
async def test_async_file_sequential_read_uses_one_request(request_server):
    endpoint, state = request_server
    state._reset()
    op = opendal.AsyncOperator("http", endpoint=endpoint, root="/")

    file = await op.open("file", "rb")
    assert state._methods() == []
    assert await file.tell() == 0
    assert state._methods() == []

    assert await file.read(5) == b"Hello"
    assert await file.read() == b"World"
    assert await file.read() == b""
    assert state._methods() == ["GET"]

    await file.close()
    assert await file.closed
    assert state._methods() == ["GET"]
    with pytest.raises(OSError, match="closed file"):
        await file.read(1)


@pytest.mark.asyncio
async def test_async_file_read_buffered_returns_ready_chunks(request_server):
    endpoint, state = request_server
    state._reset()
    op = opendal.AsyncOperator("http", endpoint=endpoint, root="/")

    async with await op.open("paused-chunks", "rb") as file:
        try:
            assert await file.read_buffered(0) == b""
            assert state._methods() == []
            prefix = await asyncio.wait_for(file.read_buffered(10), 2)
            assert prefix
            assert b"Hello".startswith(prefix)
            while len(prefix) < 5:
                chunk = await asyncio.wait_for(file.read_buffered(10), 2)
                assert chunk
                prefix += chunk
            assert prefix == b"Hello"
            assert await asyncio.to_thread(state.slow_started.wait, 2)
            assert await file.tell() == 5

            pending = file.read_buffered(10)
            done, _ = await asyncio.wait([pending], timeout=0.05)
            assert not done
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
            assert await asyncio.wait_for(file.tell(), 2) == 5
        finally:
            state.slow_release.set()

        assert await file.read_buffered(10) == b"World"
        assert await file.tell() == 10
        assert await file.read_buffered(10) == b""
        assert state._methods() == ["GET"]


@pytest.mark.asyncio
async def test_async_file_read_buffered_cancelled_before_delivery(
    request_server, monkeypatch
):
    endpoint, state = request_server
    state._reset()
    op = opendal.AsyncOperator("http", endpoint=endpoint, root="/")
    loop = asyncio.get_running_loop()
    call_soon_threadsafe = loop.call_soon_threadsafe
    delivery_scheduled = asyncio.Event()
    pending = None

    def hold_delivery(
        callback: Callable[..., object], *args: object, context: Context | None = None
    ) -> asyncio.Handle:
        if pending is not None and any(arg is pending for arg in args):
            return call_soon_threadsafe(delivery_scheduled.set)
        return call_soon_threadsafe(callback, *args, context=context)

    async with await op.open("file", "rb") as file:
        with monkeypatch.context() as patch:
            patch.setattr(loop, "call_soon_threadsafe", hold_delivery)
            pending = file.read_buffered(5)
            try:
                await asyncio.wait_for(delivery_scheduled.wait(), 2)
            finally:
                pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending

        assert await file.tell() == 0
        assert await file.read_buffered(2) == b"He"
        assert await file.tell() == 2
        assert await file.read(3) == b"llo"
        assert await file.read() == b"World"
        assert state._methods() == ["GET"]


@pytest.mark.asyncio
@pytest.mark.parametrize("next_read", ["read", "read_buffered", "seek"])
async def test_async_file_read_buffered_defers_error(request_server, next_read):
    endpoint, state = request_server
    state._reset()
    op = opendal.AsyncOperator("http", endpoint=endpoint, root="/")

    async with await op.open("broken-chunks", "rb") as file:
        assert await file.read_buffered(10) == b"Hell"
        assert await file.tell() == 4
        assert await file.read_buffered(0) == b""

        if next_read == "seek":
            assert await file.seek(1) == 1
            assert await file.read_buffered(3) == b"ell"
            assert state._methods() == ["GET", "GET"]
        else:
            with pytest.raises(OSError, match="read data from http response"):
                await getattr(file, next_read)(10)
            assert await file.tell() == 4
            assert state._methods() == ["GET"]


@pytest.mark.asyncio
async def test_async_file_start_and_current_seek_do_not_fetch_length(request_server):
    endpoint, state = request_server
    state._reset()
    op = opendal.AsyncOperator("http", endpoint=endpoint, root="/")

    async with await op.open("file", "rb") as file:
        assert await file.seek(5) == 5
        assert state._methods() == []
        assert await file.seek(-1, 1) == 4
        assert state._methods() == []
        assert await file.read() == b"oWorld"
        assert state._methods() == ["GET"]

    state._reset()
    async with await op.open("file", "rb") as file:
        assert await file.seek(-2, 2) == 8
        assert state._methods() == ["HEAD"]
        assert await file.read() == b"ld"
        assert state._methods() == ["HEAD", "GET"]

    state._reset()
    async with await op.open("file", "rb", offset=2, size=5) as file:
        assert state._methods() == []
        assert await file.seek(-2, 2) == 3
        assert state._methods() == []
        assert await file.read() == b"Wo"
        assert state._methods() == ["GET"]


@pytest.mark.asyncio
async def test_async_file_seek_past_end_reads_eof(request_server):
    endpoint, state = request_server
    state._reset()
    op = opendal.AsyncOperator("http", endpoint=endpoint, root="/")

    async with await op.open("file", "rb") as file:
        assert await file.seek(20) == 20
        assert state._methods() == []
        assert await file.read() == b""
        assert await file.tell() == 20
        assert state._methods() == ["GET"]
        assert await file.read() == b""
        assert state._methods() == ["GET"]

        assert await file.seek(-15, 1) == 5
        assert await file.read() == b"World"
        assert state._methods() == ["GET", "GET"]


@pytest.mark.asyncio
async def test_async_file_offset_past_end_has_empty_logical_length(request_server):
    endpoint, state = request_server
    state._reset()
    op = opendal.AsyncOperator("http", endpoint=endpoint, root="/")

    async with await op.open("file", "rb", offset=20) as file:
        assert state._methods() == []
        assert await file.read() == b""
        assert state._methods() == ["GET"]
        assert await file.seek(0, 2) == 0
        assert state._methods() == ["GET", "HEAD"]


@pytest.mark.asyncio
async def test_async_chunked_file_propagates_truncation_error(request_server):
    endpoint, state = request_server
    state._reset()
    op = opendal.AsyncOperator("http", endpoint=endpoint, root="/")

    async with await op.open("truncated", "rb", chunk=4) as file:
        assert state._methods() == ["HEAD"]
        with pytest.raises(OSError, match="RangeNotSatisfied"):
            await file.read()
        assert state._methods() == ["HEAD", "GET", "GET"]


@pytest.mark.asyncio
async def test_async_file_seek_reuses_read_metadata(request_server):
    endpoint, state = request_server
    state._reset()
    op = opendal.AsyncOperator("http", endpoint=endpoint, root="/")

    async with await op.open("file", "rb") as file:
        assert await file.read(2) == b"He"
        assert state._methods() == ["GET"]
        assert await file.seek(0) == 0
        assert state._methods() == ["GET"]
        assert await file.read(2) == b"He"
        assert state._methods() == ["GET", "GET"]


@pytest.mark.asyncio
async def test_async_file_cancelled_read_can_close(request_server):
    endpoint, state = request_server
    state._reset()
    op = opendal.AsyncOperator("http", endpoint=endpoint, root="/")
    file = await op.open("slow", "rb")

    pending = asyncio.ensure_future(file.read())
    assert await asyncio.to_thread(state.slow_started.wait, 2)
    assert state._methods() == ["GET"]

    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending

    await asyncio.wait_for(file.close(), timeout=1)
    assert await file.closed
    assert state._methods() == ["GET"]
    state.slow_release.set()


@pytest.mark.asyncio
async def test_async_file_coalesces_small_writes(request_server):
    endpoint, state = request_server
    state._reset()
    op = opendal.AsyncOperator(
        "webdav", endpoint=endpoint, root="/", disable_create_dir="true"
    )
    async with await op.open("file", "wb") as file:
        for _ in range(1000):
            assert await file.write(b"small") == 5
        assert state._methods() == []
    assert state._uploads() == [b"small" * 1000]


@pytest.mark.asyncio
async def test_async_file_close_error_can_retry(request_server):
    endpoint, state = request_server
    state._reset()
    op = opendal.AsyncOperator(
        "webdav", endpoint=endpoint, root="/", disable_create_dir="true"
    )
    file = await op.open("retry", "wb")
    assert await file.write(CONTENT) == len(CONTENT)
    with pytest.raises(opendal.exceptions.PermissionDenied):
        await file.close()
    assert not await file.closed
    await file.close()
    assert await file.closed
    assert state._uploads() == [CONTENT, CONTENT]


@pytest.mark.asyncio
async def test_async_file_cancelled_close_resumes_request(request_server):
    endpoint, state = request_server
    state._reset()
    op = opendal.AsyncOperator(
        "webdav", endpoint=endpoint, root="/", disable_create_dir="true"
    )
    file = await op.open("slow", "wb")
    assert await file.write(CONTENT) == len(CONTENT)
    pending = asyncio.ensure_future(file.close())
    try:
        assert await asyncio.to_thread(state.slow_started.wait, 2)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert not await file.closed
    finally:
        state.slow_release.set()
    await asyncio.wait_for(file.close(), timeout=2)
    assert await file.closed
    assert state._uploads() == [CONTENT]
