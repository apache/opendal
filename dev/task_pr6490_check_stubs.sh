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
test "$#" -eq 2
task_repo=$1
task_logs=$2
mkdir -p "$task_logs"
cd "$task_repo"
git rev-parse HEAD > "$task_logs/head.before"
git diff --quiet b509fa920283a02277e780d2346ba57e30744aad HEAD -- bindings/python
git diff --cached --quiet
git diff --name-only > "$task_logs/generated.before"
if rg -qv '^bindings/python/python/opendal/[^/]+\.pyi$' "$task_logs/generated.before"; then
    exit 3
fi

# Use the formatter version required by ci_bindings_python.yml. Keep the
# executable with this task's artifacts, without replacing a global tool.
task_tool_dir="$task_logs/hawkeye-6.5.1"
task_archive=hawkeye-x86_64-unknown-linux-gnu.tar.xz
mkdir -p "$task_tool_dir"
gh release download v6.5.1 --repo korandoru/hawkeye --pattern "$task_archive" --dir "$task_tool_dir" --skip-existing > "$task_logs/download.log" 2>&1
printf '%s  %s\n' d6eb0505a45a15244f4f789158aafe5e3f1a7dc86c9dc1d7651f3cb1e1b321e0 "$task_tool_dir/$task_archive" | sha256sum --check > "$task_logs/archive-check.log"
tar -xJf "$task_tool_dir/$task_archive" -C "$task_tool_dir"
mapfile -t task_binaries < <(rg --files "$task_tool_dir" -g hawkeye)
test "${#task_binaries[@]}" -eq 1
task_hawkeye=${task_binaries[0]}
"$task_hawkeye" --version > "$task_logs/formatter.version"
rg -q '^version: 6\.5\.1$' "$task_logs/formatter.version"
sha256sum "$task_hawkeye" > "$task_logs/formatter.sha256"

cd bindings/python
"$task_hawkeye" format --config ../../licenserc.toml --fail-if-updated false > "$task_logs/license.log" 2>&1
cd "$task_repo"
git diff --check > "$task_logs/diff-check.log" 2>&1
git diff --quiet
git status --porcelain > "$task_logs/status.after"
git rev-parse HEAD > "$task_logs/head.after"
printf '0\n' > "$task_logs/canonical.status"
