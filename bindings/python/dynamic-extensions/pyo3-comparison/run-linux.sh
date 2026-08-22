#!/usr/bin/env sh
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eu

prototype_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
target_root="$prototype_dir/target"
stage="$target_root/python-stage"
audit="$prototype_dir/../audit-elf-exports.sh"

build_package() {
  package=$1
  target=$2
  CARGO_TARGET_DIR="$target_root/$target" cargo build --release --locked --offline \
    --manifest-path "$prototype_dir/Cargo.toml" --package "$package"
}

build_package pyo3-comparison-base base
build_package pyo3-comparison-fs-capsule fs-capsule
build_package pyo3-comparison-fs-direct fs-direct
build_package pyo3-comparison-mime-capsule mime-capsule
build_package pyo3-comparison-mime-direct mime-direct

mkdir -p "$stage"
cp "$target_root/base/release/libbase.so" "$stage/opendal_poc.so"
cp "$target_root/fs-capsule/release/libfs_capsule.so" "$stage/opendal_fs_capsule.so"
cp "$target_root/fs-direct/release/libfs_direct.so" "$stage/opendal_fs_direct.so"
cp "$target_root/mime-capsule/release/libmime_capsule.so" "$stage/opendal_mime_capsule.so"
cp "$target_root/mime-direct/release/libmime_direct.so" "$stage/opendal_mime_direct.so"

"$audit" "$stage/opendal_poc.so" PyInit_opendal_poc
"$audit" "$stage/opendal_fs_capsule.so" PyInit_opendal_fs_capsule
"$audit" "$stage/opendal_fs_direct.so" PyInit_opendal_fs_direct
"$audit" "$stage/opendal_mime_capsule.so" PyInit_opendal_mime_capsule
"$audit" "$stage/opendal_mime_direct.so" PyInit_opendal_mime_direct

if [ "$#" -eq 0 ]; then
  set -- --all
fi

PYTHONPATH="$stage" python3 "$prototype_dir/compare.py" "$@"
