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

import json
import os
import subprocess
import sys
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from urllib.parse import urlencode

import pytest


@contextmanager
def head_server(length, *, retry_first=False):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_HEAD(self) -> None:
            requests.append(self.path)
            if retry_first and len(requests) == 1:
                self.send_response(503)
                self.send_header("Content-Length", "0")
            else:
                self.send_response(200)
                self.send_header("Content-Length", str(length))
            self.end_headers()

        def log_message(self, _format: str, *args: object) -> None:
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}", requests
        finally:
            server.shutdown()
            thread.join()


def run_client(constructor, origin, proxy):
    from opendal import Operator

    service = {
        "endpoint": origin,
        "region": "us-west-2",
        "skip_signature": "true",
        "disable_config_load": "true",
        "disable_ec2_metadata": "true",
    }
    proxy_options = {
        "context.http.proxy": proxy,
        "layers": "retry",
        "layer.retry.min_delay": "1ms",
    }
    if constructor == "map":
        proxied = Operator("s3", bucket="proxy-test", **service, **proxy_options)
        direct = Operator("s3", bucket="proxy-test", **service)
    else:
        proxied = Operator.from_uri(
            f"s3://proxy-test?{urlencode(service | proxy_options)}"
        )
        direct = Operator.from_uri(f"s3://proxy-test?{urlencode(service)}")

    print(
        json.dumps(
            [
                direct.stat("direct-before").content_length,
                proxied.stat("proxied").content_length,
                direct.stat("direct-after").content_length,
            ]
        )
    )


@pytest.mark.parametrize("constructor", ["map", "uri"])
def test_operator_proxy_and_retry_are_isolated(constructor):
    env = {
        key: value
        for key, value in os.environ.items()
        if key.lower() not in {"http_proxy", "https_proxy", "all_proxy", "no_proxy"}
    }
    # The child imports the binding with a clean proxy environment, and its
    # blocking calls leave the parent free to serve both endpoints.
    with (
        head_server(3) as (origin, origin_requests),
        head_server(7, retry_first=True) as (proxy, proxy_requests),
    ):
        result = subprocess.run(
            [sys.executable, __file__, constructor, origin, proxy],
            env=env,
            stdout=subprocess.PIPE,
            text=True,
            check=True,
            timeout=20,
        )

    assert json.loads(result.stdout) == [3, 7, 3]
    assert origin_requests == ["/proxy-test/direct-before", "/proxy-test/direct-after"]
    assert proxy_requests == [f"{origin}/proxy-test/proxied"] * 2


if __name__ == "__main__":
    run_client(*sys.argv[1:])
