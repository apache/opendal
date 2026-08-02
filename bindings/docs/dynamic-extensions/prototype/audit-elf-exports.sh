#!/usr/bin/env sh
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0.

set -eu

if [ "$#" -ne 2 ]; then
  echo "usage: $0 ARTIFACT EXPECTED_SYMBOL" >&2
  exit 2
fi

artifact=$1
expected=$2
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

echo "$artifact exports only $expected"
