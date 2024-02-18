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

import subprocess
from pathlib import Path
from constants import get_package_version, get_package_dependence, PACKAGES

ROOT_DIR = Path(__file__).parent.parent

# If this package depends on `core`, we need to update it's core version in `Cargo.toml` file.
def update_package(path):
    # skip core package.
    if path == "core":
        return
    if get_package_dependence(path) != "core":
        return

    core_version = get_package_version("core")


def archive_package(path):
    print(f"Archive package {path} started")

    version = get_package_version(path)
    name = f"apache-opendal-{str(path).replace('/', '-')}-{version}-src"

    ls_command = ["git", "ls-files", "."]
    ls_result = subprocess.run(
        ls_command, cwd=ROOT_DIR / path, capture_output=True, check=True, text=True
    )

    tar_command = [
        "tar",
        "-zcf",
        f"{ROOT_DIR}/dist/{name}.tar.gz",
        "--transform",
        f"s,^,{name}/,",
        "-T",
        "-",
    ]
    subprocess.run(
        tar_command,
        cwd=ROOT_DIR / path,
        input=ls_result.stdout.encode("utf-8"),
        check=True,
    )

    print(f"Archive package {path} to dist/{name}.tar.gz")

def generate_signature():
    for i in Path(ROOT_DIR / "dist").glob("*.tar.gz"):
        print(f"Generate signature for {i}")
        subprocess.run(
            ["gpg", "--yes", "--armor", "--output", f"{i}.asc", "--detach-sig", str(i)],
            cwd=ROOT_DIR / "dist",
            check=True,
        )

    for i in Path(ROOT_DIR / "dist").glob("*.tar.gz"):
        print(f"Check signature for {i}")
        subprocess.run(
            ["gpg", "--verify", f"{i}.asc", str(i)], cwd=ROOT_DIR / "dist", check=True
        )


def generate_checksum():
    for i in Path(ROOT_DIR / "dist").glob("*.tar.gz"):
        print(f"Generate checksum for {i}")
        subprocess.run(
            ["sha512sum", str(i.relative_to(ROOT_DIR / "dist"))],
            stdout=open(f"{i}.sha512", "w"),
            cwd=ROOT_DIR / "dist",
            check=True,
        )

    for i in Path(ROOT_DIR / "dist").glob("*.tar.gz"):
        print(f"Check checksum for {i}")
        subprocess.run(
            ["sha512sum", "--check", f"{str(i.relative_to(ROOT_DIR / 'dist'))}.sha512"],
            cwd=ROOT_DIR / "dist",
            check=True,
        )


if __name__ == "__main__":
    for v in PACKAGES:
        archive_package(v)
    generate_signature()
    generate_checksum()
