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


"""Verify that the Maven source release contains the standalone JNI source tree."""

import argparse
import stat
import subprocess
import tempfile
import zipfile
from pathlib import Path, PurePosixPath


def check_source_release(path: Path) -> None:
    with zipfile.ZipFile(path) as archive:
        files = {
            info.filename: info for info in archive.infolist() if not info.is_dir()
        }
        roots = {PurePosixPath(name).parts[0] for name in files}
        if len(roots) != 1:
            raise ValueError("expected one source release root directory")
        root = roots.pop()
        required = (
            "LICENSE",
            "NOTICE",
            "bindings/java/pom.xml",
            "bindings/java/Cargo.toml",
            "bindings/java/mvnw",
            "bindings/java/.mvn/wrapper/maven-wrapper.properties",
            "bindings/java/tools/build.py",
            "core/Cargo.toml",
            "core/core/Cargo.toml",
        )
        missing = [name for name in required if f"{root}/{name}" not in files]
        if missing:
            raise ValueError(f"missing standalone build inputs: {missing}")
        for name in files:
            parts = PurePosixPath(name).parts
            if PurePosixPath(name).is_absolute() or ".." in parts:
                raise ValueError(f"invalid archive path: {name}")
            if (
                "target" in parts
                or "local-staging" in parts
                or name.endswith(
                    (".jar", ".class", ".so", ".dylib", ".dll", ".a", ".rlib")
                )
            ):
                raise ValueError(f"compiled build output in source release: {name}")
        mode = files[f"{root}/bindings/java/mvnw"].external_attr >> 16
        if not mode & stat.S_IXUSR:
            raise ValueError("the Maven wrapper must remain executable")
        with tempfile.TemporaryDirectory() as tmpdir:
            archive.extractall(tmpdir)
            subprocess.run(
                ["cargo", "metadata", "--format-version", "1", "--no-deps"],
                cwd=Path(tmpdir) / root / "bindings/java",
                check=True,
                stdout=subprocess.DEVNULL,
            )
    print(f"Verified standalone source layout and Cargo manifest: {path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    check_source_release(parser.parse_args().archive)
