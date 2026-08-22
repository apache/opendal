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

if [ "$#" -lt 2 ]; then
  echo "usage: $0 ARTIFACT EXPECTED_SYMBOL..." >&2
  exit 2
fi

artifact=$1
shift
expected=$(printf '%s\n' "$@" | LC_ALL=C sort -u)
actual=$(
  nm --dynamic --defined-only --extern-only --format=posix "$artifact" \
    | awk '{ print $1 }' \
    | sed 's/@.*//' \
    | LC_ALL=C sort -u
)

if [ "$actual" != "$expected" ]; then
  echo "unexpected exports in $artifact" >&2
  echo "expected: $expected" >&2
  echo "actual:" >&2
  echo "$actual" >&2
  exit 1
fi

echo "$artifact exports only the expected symbols"
