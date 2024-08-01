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
import tarfile
from pathlib import Path
from constants import get_package_version, get_package_dependence, PACKAGES

ROOT_DIR = Path(__file__).parent.parent


def archive_package(path):
    print(f"Archive package {path} started")

    core_version = get_package_version("core")
    version = get_package_version(path)
    name = f"apache-opendal-{str(path).replace('/', '-')}-{version}-src"

    # `git ls-files` handles duplicated path correctly, so we don't need to worry about it
    ls_command = [
        "git",
        "ls-files",
        # Make sure LICENSE and NOTICE files are included.
        "LICENSE",
        "NOTICE",
        # Always include core.
        "core",
        f"{path}",
        f"{get_package_dependence(path)}",
    ]
    ls_result = subprocess.run(
        ls_command, cwd=ROOT_DIR, capture_output=True, check=True, text=True
    )
    files = list(filter(lambda x: len(x) > 0, ls_result.stdout.split("\n")))

    with tarfile.open(f"{ROOT_DIR}/dist/{name}.tar.gz", "w:gz") as tar:
        for file in files:
            tar.add(f"{ROOT_DIR}/{file}", arcname=f"{name}/{file}")

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
            ["shasum", "-a", "512", str(i.relative_to(ROOT_DIR / "dist"))],
            stdout=open(f"{i}.sha512", "w"),
            cwd=ROOT_DIR / "dist",
            check=True,
        )

    for i in Path(ROOT_DIR / "dist").glob("*.tar.gz"):
        print(f"Check checksum for {i}")
        subprocess.run(
            [
                "shasum",
                "-a",
                "512",
                "-c",
                f"{str(i.relative_to(ROOT_DIR / 'dist'))}.sha512",
            ],
            cwd=ROOT_DIR / "dist",
            check=True,
        )


if __name__ == "__main__":
    (ROOT_DIR / "dist").mkdir(exist_ok=True)
    for v in PACKAGES:
        archive_package(v)
    generate_signature()
    generate_checksum()
