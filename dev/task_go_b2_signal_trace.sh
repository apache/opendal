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

task_signal_dir="${RUNNER_TEMP:?}/opendal-go-b2-signal"
mkdir -p "$task_signal_dir"
go env -json GOVERSION GOOS GOARCH CGO_ENABLED > "$task_signal_dir/go-environment.json"
cat "$task_signal_dir/go-environment.json"
test "$(go env CGO_ENABLED)" = 0
strace --version > "$task_signal_dir/strace-version.txt"
git rev-parse HEAD > "$task_signal_dir/checkout-head.txt"
git -C opendal rev-parse HEAD > "$task_signal_dir/native-source-head.txt"
git -C opendal-go-services rev-parse HEAD > "$task_signal_dir/go-services-head.txt"
sha256sum opendal/bindings/c/target/debug/libopendal_c.so > "$task_signal_dir/native-library.sha256"
sha256sum opendal-go-services/b2/libopendal_c.linux.amd64.so.zst > "$task_signal_dir/embedded-library.sha256"
set +e
cargo tree --offline --locked --manifest-path opendal/bindings/c/Cargo.toml --features opendal/services-b2 > "$task_signal_dir/resolved-native-tree.log" 2>&1
task_tree_status=$?
set -e
printf '%s\n' "$task_tree_status" > "$task_signal_dir/resolved-native-tree.status"

task_root_suffix="task-sigpipe-${GITHUB_RUN_ID:?}"
test "${OPENDAL_TEST:?}" = b2
task_base_root="${OPENDAL_B2_ROOT:-/}"
export OPENDAL_B2_ROOT="${task_base_root%/}/$task_root_suffix/"
printf '%s\n' "$task_root_suffix" > "$task_signal_dir/root-suffix.txt"

# String limit zero suppresses buffer contents. Trace no reads, exec arguments,
# stack memory, or payload dumps; retain signal and descriptor metadata only.
set +e
strace -ff -ttt -s 0 -y -Y   -e trace=rt_sigaction,rt_sigprocmask,tgkill,write,writev,sendto,sendmsg   -e signal=SIGPIPE -o "$task_signal_dir/trace" -- "$@"
task_test_status=$?
printf '%s\n' "$task_test_status" > "$task_signal_dir/test-command.status"
exit "$task_test_status"
