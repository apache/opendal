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

import threading
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Self

import pytest

import opendal
from opendal.exceptions import ConfigInvalid

BODY = b"proxied"


class ProxyServer:
    def __init__(self) -> None:
        self.paths: list[str] = []
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self) -> None:
                proxy.paths.append(self.path)
                self.send_response(200)
                self.send_header("Content-Length", str(len(BODY)))
                self.end_headers()
                self.wfile.write(BODY)

            def log_message(self, format: str, *args: object) -> None:
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def __enter__(self) -> Self:
        """Start accepting proxy connections."""
        self._thread.start()
        return self

    def __exit__(self, *args: object) -> None:
        """Stop the proxy server."""
        self._server.shutdown()
        self._server.server_close()
        self._thread.join()

    @property
    def url(self) -> str:
        """Return the proxy URL."""
        return f"http://{self._server.server_name}:{self._server.server_port}"


def test_read_through_proxy() -> None:
    with ProxyServer() as proxy:
        op = opendal.Operator("http", endpoint="http://opendal.invalid").layer(
            opendal.layers.HttpConfigLayer(proxy=proxy.url)
        )

        assert op.read("/hello") == BODY
        assert proxy.paths == ["http://opendal.invalid/hello"]


@pytest.mark.asyncio
async def test_async_read_through_proxy() -> None:
    with ProxyServer() as proxy:
        op = opendal.AsyncOperator("http", endpoint="http://opendal.invalid").layer(
            opendal.layers.HttpConfigLayer(
                proxy=proxy.url,
                connect_timeout=timedelta(seconds=5),
                read_timeout=timedelta(seconds=5),
                timeout=timedelta(seconds=10),
                user_agent="opendal-test/1.0",
            )
        )

        assert await op.read("/hello") == BODY
        assert proxy.paths == ["http://opendal.invalid/hello"]


def test_no_proxy_requires_proxy() -> None:
    with pytest.raises(ConfigInvalid, match="no_proxy requires proxy"):
        opendal.layers.HttpConfigLayer(no_proxy="169.254.169.254")


def test_invalid_proxy() -> None:
    with pytest.raises(ConfigInvalid, match="http proxy is invalid"):
        opendal.layers.HttpConfigLayer(proxy="http://")


def test_negative_duration() -> None:
    with pytest.raises(ConfigInvalid):
        opendal.layers.HttpConfigLayer(
            proxy="http://127.0.0.1:7890",
            connect_timeout=timedelta(seconds=-1),
        )
