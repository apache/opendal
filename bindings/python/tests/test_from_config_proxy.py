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
import os
import pickle
import subprocess
import sys
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import pytest

import opendal
from opendal.config import (
    ConcurrentLimitLayerConfig,
    ContextConfig,
    HttpConfig,
    HttpContextConfig,
    LoggingLayerConfig,
    MimeGuessLayerConfig,
    RetryLayerConfig,
    TimeoutLayerConfig,
)


def run_client(operator_kind: str, endpoint: str) -> None:
    service = HttpConfig(scheme="http", endpoint=endpoint)
    context = ContextConfig(
        http=HttpContextConfig(
            proxy=endpoint,
            no_proxy=["unrelated.invalid"],
            user_agent="opendal-typed-config-test",
        )
    )
    retry = RetryLayerConfig(
        name="retry", max_times=1, factor=1.5, min_delay=timedelta(milliseconds=1)
    )
    layers = (
        ConcurrentLimitLayerConfig(name="concurrent_limit", permits=2, http_permits=1),
        LoggingLayerConfig(name="logging"),
        MimeGuessLayerConfig(name="mime_guess"),
        TimeoutLayerConfig(name="timeout", timeout=timedelta(seconds=5)),
        retry,
    )

    factory = opendal.Operator if operator_kind == "sync" else opendal.AsyncOperator
    operator = factory.from_config(service, context=context, layers=layers)
    # The operator owns its configuration after construction.
    del context["http"]
    retry["max_times"] = 0
    restored = pickle.loads(pickle.dumps(operator))
    assert type(restored) is factory

    async def check() -> None:
        for op, path in ((operator, "original.html"), (restored, "restored.html")):
            if isinstance(op, opendal.AsyncOperator):
                metadata = await op.stat(path)
            else:
                metadata = op.stat(path)
            assert (metadata.content_length, metadata.content_type) == (7, "text/html")

    asyncio.run(check())


@pytest.mark.parametrize("operator_kind", ["sync", "async"])
def test_from_config_pickle_preserves_proxy_and_layers(operator_kind):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_HEAD(self) -> None:
            request = (self.path, self.headers.get("User-Agent"))
            retry = request in requests
            requests.append(request)
            self.send_response(200 if retry else 503)
            self.send_header("Content-Length", "7" if retry else "0")
            self.end_headers()

        def log_message(self, _format: str, *args: object) -> None:
            pass

    env = {
        key: value
        for key, value in os.environ.items()
        if key.lower() not in {"http_proxy", "https_proxy", "all_proxy", "no_proxy"}
    }
    # Use the same listener as the origin and proxy. The request target exposes
    # which route the client chose without making any external connections.
    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        endpoint = f"http://127.0.0.1:{server.server_port}"
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            subprocess.run(
                [sys.executable, __file__, operator_kind, endpoint],
                env=env,
                check=True,
                timeout=20,
            )
        finally:
            server.shutdown()
            thread.join()

    assert requests == [
        (f"{endpoint}/{path}", "opendal-typed-config-test")
        for path in ("original.html", "original.html", "restored.html", "restored.html")
    ]


if __name__ == "__main__":
    run_client(*sys.argv[1:])
