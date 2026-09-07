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

import base64
import json
import os
import subprocess
from pathlib import Path
from urllib.parse import quote
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
    output = []
    for page in range(1, 1001):
        part = api(
            f"{endpoint}{'&' if '?' in endpoint else '?'}per_page=100&page={page}"
        )
        if not isinstance(part, list):
            raise ValueError("expected paginated array")  # noqa: TRY004
        output.extend(part)
        if len(part) < 100:
            return output
    raise ValueError("pagination limit exceeded")


def download(url):
    with urlopen(
        Request(url, headers={"User-Agent": "Apache-OpenDAL-release"}), timeout=120
    ) as response:
        return response.read()


class Store:
    """Compare-and-swap state on the release-state branch."""

    def __init__(self, config):
        self.repo = config["repository"]
        self.branch = config["state_branch"]
        self.endpoint = f"repos/{self.repo}/contents/state.json"
        self.sha = None

    def load(self, public=False):
        if public:
            return json.loads(
                download(
                    f"https://raw.githubusercontent.com/{self.repo}/{self.branch}/state.json"
                )
            )
        entry = api(f"{self.endpoint}?ref={quote(self.branch, safe='')}")
        self.sha = entry["sha"]
        return json.loads(base64.b64decode(entry["content"]))

    def save(self, state):
        value = api(
            self.endpoint,
            {
                "message": "Update weekly release state",
                "branch": self.branch,
                "sha": self.sha,
                "content": base64.b64encode(canonical(state)).decode(),
            },
            method="PUT",
        )
        self.sha = value["content"]["sha"]


def config():
    return json.loads((ROOT / ".release/config.json").read_text())
