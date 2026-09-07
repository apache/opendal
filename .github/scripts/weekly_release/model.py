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

"""Deterministic release identities and version plans."""

import datetime as dt
import json
import re

UTC = dt.timezone.utc
PACKAGE_PATTERN = re.compile(r'make_package\("([^"\n]+)", "(\d+\.\d+\.\d+)"')


def canonical(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode()


def timestamp(value):
    result = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("timestamp must have a timezone")
    return result.astimezone(UTC)


def cutoff(now):
    """Most recent Friday 00:00 UTC, independent of runner start delays."""
    local = now.astimezone(UTC)
    friday = local.replace(hour=0, minute=0, second=0, microsecond=0)
    friday -= dt.timedelta(days=(local.weekday() - 4) % 7)
    return friday.astimezone(UTC)


def versions(source):
    result = dict(PACKAGE_PATTERN.findall(source))
    if "core" not in result:
        raise ValueError("source package inventory has no core")
    return result


def version_tuple(value):
    if not re.fullmatch(r"\d+\.\d+\.\d+", value):
        raise ValueError(f"invalid stable version: {value}")
    return tuple(map(int, value.split(".")))


def plan_versions(baseline, inventory):
    if set(baseline) != set(inventory):
        raise ValueError("package inventory changed; review the release baseline")
    targets = {}
    for package, value in baseline.items():
        major, minor, patch = version_tuple(value)
        targets[package] = max(
            f"{major}.{minor}.{patch + 1}", inventory[package], key=version_tuple
        )
    return targets
