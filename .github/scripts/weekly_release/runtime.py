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
from pathlib import Path
from urllib.request import Request, urlopen

from model import canonical

ROOT = Path(__file__).resolve().parents[3]


def run(*args, cwd=None, data=None, env=None):
    result = subprocess.run(
        [str(a) for a in args],
        cwd=cwd or ROOT,
        input=data,
        env=env,
        capture_output=True,
        check=False,
    )
    if result.returncode:
        # Never include argv; redact secrets in diagnostics.
        diagnostic = result.stderr.decode(errors="replace")[-4000:]
        for name, value in os.environ.items():
            if len(value) >= 8 and any(
                word in name for word in ("TOKEN", "PASSWORD", "SECRET", "PRIVATE_KEY")
            ):
                diagnostic = diagnostic.replace(value, "[REDACTED]")
        raise RuntimeError(
            f"{Path(str(args[0])).name} failed (exit {result.returncode}): {diagnostic}"
        )
    return result.stdout


def git(*args, cwd=None):
    return run("git", *args, cwd=cwd).decode().strip()


def api(endpoint, payload=None, method=None):
    args = ["gh", "api", endpoint]
    if method:
        args += ["--method", method]
    if payload is not None:
        args += ["--input", "-"]
    raw = run(*args, data=canonical(payload) if payload is not None else None)
    return json.loads(raw) if raw.strip() else None


def pages(endpoint):
    return [
        item
        for page in json.loads(run("gh", "api", "--paginate", "--slurp", endpoint))
        for item in page
    ]


def download(url):
    with urlopen(
        Request(url, headers={"User-Agent": "Apache-OpenDAL-release"}), timeout=120
    ) as response:
        return response.read()
