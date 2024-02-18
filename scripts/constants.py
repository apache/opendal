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

from pathlib import Path
import tomllib

ROOT_DIR = Path(__file__).parent.parent


def list_packages():
    packages = ["core"]

    for dir in ["bin", "bindings", "integrations"]:
        for path in (ROOT_DIR / dir).iterdir():
            if path.is_dir():
                packages.append(path.relative_to(ROOT_DIR))
    return packages


PACKAGES = list_packages()

# package dependencies is used to maintain the dependencies between packages.
#
# All packages are depend on `core` by default, so we should only maintain the exceptions in list.
PACKAGE_DEPENDENCIES = {
    Path("bindings/go"): "bindings/c",
    Path("bindings/swift"): "bindings/c",
    Path("bindings/zig"): "bindings/c",
}


# fetch the package dependence, return `core` if not listed in `PACKAGE_DEPENDENCIES`.
def get_package_dependence(package: Path) -> str:
    return PACKAGE_DEPENDENCIES.get(package, "core")


# input: Path to a Rust package like `core` and `bindings/python`.
def get_rust_package_version(path):
    with open(ROOT_DIR / path / "Cargo.toml", "rb") as f:
        data = tomllib.load(f)
        version = data["package"]["version"]
    return version


# get the package version by package name.
#
# For examples:
# core: `0.45.0`
# packages depends on core: `0.1.0+core.0.45.0`
# packages depends on bindings/c: `0.1.0+bindings_c.0.1.0`
def get_package_version(package):
    if package == "core":
        return get_rust_package_version("core")

    # NOTE: for now, all dependence package must be rust package.
    dependence = get_package_dependence(package)
    dependence_version = get_rust_package_version(dependence)

    cargo_toml = ROOT_DIR / package / "Cargo.toml"
    # cargo_toml exists, we can get the version from Cargo.toml.
    if cargo_toml.exists():
        package_version = get_rust_package_version(package)
        return f"{package_version}+{dependence.replace('/', '_')}.{dependence_version}"

    # cargo_toml not exists, we should handle case by case ideally.
    #
    # However, those packages are not mature enough, it's much easier for us to always return `0.0.0` instead.
    return f"0.0.0+{dependence.replace('/', '_')}.{dependence_version}"


if __name__ == "__main__":
    for v in PACKAGES:
        print(
            f"{v}: depends={get_package_dependence(v)}, version={get_package_version(v)}"
        )
