#!/usr/bin/env python3
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

"""Build and run the Linux Rust extension probe using Cargo's artifact paths."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile


def main():
    if sys.platform != "linux":
        raise SystemExit("This runner currently supports Linux only.")
    env = os.environ.copy()
    if "CARGO_ENCODED_RUSTFLAGS" in env:
        raise SystemExit("Unset CARGO_ENCODED_RUSTFLAGS so prefer-dynamic takes effect.")
    env["RUSTFLAGS"] = (env.get("RUSTFLAGS", "") + " -C prefer-dynamic").strip()
    root = Path(__file__).resolve().parent
    artifacts = {}
    process = subprocess.Popen(
        ["cargo", "build", "--workspace", "--locked", "--message-format=json-render-diagnostics"],
        cwd=root,
        env=env,
        stdout=subprocess.PIPE,
        text=True,
    )
    for line in process.stdout:
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            print(line, end="", flush=True)
            continue
        if message.get("reason") == "compiler-artifact":
            name = message["target"]["name"]
            if message.get("executable"):
                artifacts[name] = Path(message["executable"])
            else:
                for filename in message["filenames"]:
                    if filename.endswith(".so"):
                        artifacts[name] = Path(filename)
    if process.wait():
        raise SystemExit(process.returncode)
    host = artifacts["opendal-extension-demo-host"]
    plugin = artifacts["opendal_extension_demo_plugin"]
    runtime = artifacts["opendal_extension_demo_runtime"]
    rust_libdir = subprocess.check_output(
        ["rustc", "--print", "target-libdir"], cwd=root, text=True
    ).strip()
    env["LD_LIBRARY_PATH"] = os.pathsep.join(
        [str(runtime.parent), str(runtime.parent / "deps"), rust_libdir]
        + ([env["LD_LIBRARY_PATH"]] if env.get("LD_LIBRARY_PATH") else [])
    )

    for artifact in [host, plugin]:
        output = subprocess.check_output(["ldd", str(artifact)], env=env, text=True)
        runtime_lines = [line.strip() for line in output.splitlines() if runtime.name in line]
        if not runtime_lines or any("not found" in line for line in output.splitlines()):
            raise RuntimeError(f"Unresolved shared runtime for {artifact}:\n{output}")
        resolved = Path(runtime_lines[0].split("=>", 1)[1].split()[0]).resolve()
        if resolved != runtime.resolve():
            raise RuntimeError(f"Unexpected runtime library: {resolved}")
        print(f"LINK: {artifact.name}: {runtime_lines[0]}", flush=True)
    with tempfile.TemporaryDirectory(prefix="opendal-extension-demo-") as storage:
        subprocess.run([str(host), str(plugin), storage], env=env, check=True, timeout=30)


if __name__ == "__main__":
    main()
