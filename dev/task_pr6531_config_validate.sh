#!/usr/bin/env bash
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

set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
log_dir="${LOG_DIR:?new log directory required}"
base_head="${BASE_HEAD:?restore head required}"
product_head=cbf8db813a6670356ca5cbda4fac27f3360660d5
export PATH="/home/xuanwo/.cargo/bin:$PATH"
export CARGO_BUILD_JOBS=4
test ! -e "$log_dir"
mkdir -p "$log_dir"
cd "$repo_root"
exec 9>/tmp/opendal-pr-delivery-20261031-build.lock
flock 9
finish() {
    local result=$? restore_status
    trap - EXIT
    set +e
    git rev-parse HEAD HEAD^{tree} > "$log_dir/final-source-head.txt"
    git status --porcelain --untracked-files=all > "$log_dir/final-source-status.txt"
    if [ ! -s "$log_dir/final-source-status.txt" ]; then
        git switch --detach "$base_head" > "$log_dir/restore.log" 2>&1
        restore_status=$?
        printf '%s\n' "$restore_status" > "$log_dir/restore.status"
        if [ "$restore_status" -ne 0 ]; then result=2; fi
    else
        printf 'not attempted: preserve uncommitted task output\n' > "$log_dir/restore.status"
        result=2
    fi
    git rev-parse HEAD HEAD^{tree} > "$log_dir/restored-head.txt"
    git status --porcelain --untracked-files=all > "$log_dir/restored-status.txt"
    printf '%s\n' "$result" > "$log_dir/driver.exit"
    exit "$result"
}
trap finish EXIT
run_stage() {
    local stage="$1"
    shift
    set +e
    "$@" > "$log_dir/$stage.log" 2>&1
    local stage_status=$?
    set -e
    printf '%s\n' "$stage_status" > "$log_dir/$stage.status"
    test "$stage_status" -eq 0
}
git rev-parse HEAD HEAD^{tree} > "$log_dir/carrier-head.txt"
test "$(git rev-parse HEAD^)" = "$product_head"
sha256sum dev/task_pr6531_config_validate.sh > "$log_dir/driver.sha256"
git status --porcelain --untracked-files=all > "$log_dir/pre-status.txt"
test ! -s "$log_dir/pre-status.txt"
git switch --detach "$product_head" > "$log_dir/product-checkout.log" 2>&1
cargo --version > "$log_dir/toolchain.txt"
rustc --version >> "$log_dir/toolchain.txt"
for language in java python dotnet docs; do
    run_stage "generate-$language" cargo run --quiet --manifest-path dev/Cargo.toml -- generate -l "$language"
done
run_stage fmt-dotnet cargo fmt --manifest-path bindings/dotnet/Cargo.toml
# The generator emits this unchanged Rust metadata with noncanonical wrapping.
test "$(git hash-object bindings/dotnet/src/metadata.rs)" = "$(git rev-parse 58aec67cd76b1d450c2491d807c4229e09e1e49f:bindings/dotnet/src/metadata.rs)"
run_stage diff-check git diff --check
git diff --name-only > "$log_dir/generated-paths.txt"
git diff --stat > "$log_dir/generated-diff-stat.txt"
while IFS= read -r path; do
    case "$path" in
        bindings/java/src/main/java/org/apache/opendal/ServiceConfig.java|bindings/python/python/opendal/config.py|bindings/dotnet/OpenDAL/ServiceConfig/S3ServiceConfig.cs|bindings/dotnet/src/metadata.rs|website/data/services.json) ;;
        *) exit 3 ;;
    esac
done < "$log_dir/generated-paths.txt"
git add -- bindings/java/src/main/java/org/apache/opendal/ServiceConfig.java bindings/python/python/opendal/config.py bindings/dotnet/OpenDAL/ServiceConfig/S3ServiceConfig.cs bindings/dotnet/src/metadata.rs website/data/services.json
git -c commit.gpgsign=true commit -m 'docs: normalize generated S3 configuration descriptions' > "$log_dir/generated-commit.log" 2>&1
git branch xuanwo/pr-6531-validated-result HEAD
git rev-parse HEAD HEAD^{tree} HEAD^ > "$log_dir/validated-head.txt"
run_stage verify-signature git verify-commit HEAD
git diff --quiet "$product_head" -- core/services/s3
cd core
run_stage unit env -u AWS_REGION -u AWS_DEFAULT_REGION cargo test --offline --locked -p opendal-service-s3 --lib
run_stage region-both env AWS_REGION=eu-west-1 AWS_DEFAULT_REGION=us-east-2 cargo test --offline --locked -p opendal-service-s3 --lib backend::tests::test_default_region_presign_scope -- --exact
run_stage region-default env -u AWS_REGION AWS_DEFAULT_REGION=us-east-2 cargo test --offline --locked -p opendal-service-s3 --lib backend::tests::test_default_region_presign_scope -- --exact
run_stage clippy cargo clippy --offline --locked -p opendal-service-s3 --all-targets -- -D warnings
run_stage doc cargo test --offline --locked -p opendal-service-s3 --doc
run_stage fmt-core cargo fmt --all -- --check
cd "$repo_root"
run_stage fmt-dotnet-check cargo fmt --manifest-path bindings/dotnet/Cargo.toml -- --check
run_stage final-diff-check git diff --check
git status --porcelain --untracked-files=all > "$log_dir/validated-status.txt"
test ! -s "$log_dir/validated-status.txt"
