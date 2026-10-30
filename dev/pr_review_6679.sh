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

task_worktree=$1
task_logs=$2
task_expected_sha=$3
task_native_sha=$4
mkdir -p "$task_logs"
task_status=0
"$task_worktree/bindings/python/.venv/bin/python" \
    "$task_logs/pr_review_6679.py" "$task_worktree" "$task_expected_sha" \
    "$task_native_sha" > "$task_logs/probe.log" 2>&1 || task_status=$?
printf '%s\n' "$task_status" > "$task_logs/done.status.tmp"
mv "$task_logs/done.status.tmp" "$task_logs/done.status"
exit "$task_status"
