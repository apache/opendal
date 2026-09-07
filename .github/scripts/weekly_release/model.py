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
import hashlib
import json
import re

UTC = dt.timezone.utc
TERMINAL = {"dry-run-complete", "released", "withdrawn", "failed", "skipped"}
PACKAGE_PATTERN = re.compile(r'make_package\("([^"\n]+)", "(\d+\.\d+\.\d+)"')


def canonical(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode()


def digest(value):
    return hashlib.sha512(value).hexdigest()


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


def cycle_id(when):
    return when.astimezone(UTC).strftime("%G-W%V")


def snapshot_at(snapshots, when):
    eligible = [s for s in snapshots if timestamp(s["at"]) <= when]
    if not eligible:
        raise ValueError("no recorded main snapshot before cutoff; wait for next cycle")
    return max(eligible, key=lambda s: timestamp(s["at"]))["sha"]


def versions(source):
    result = dict(PACKAGE_PATTERN.findall(source))
    if "core" not in result:
        raise ValueError("source package inventory has no core")
    return result


def version_tuple(value):
    if not re.fullmatch(r"\d+\.\d+\.\d+", value):
        raise ValueError(f"invalid stable version: {value}")
    return tuple(map(int, value.split(".")))


def bump(value, change):
    major, minor, patch = version_tuple(value)
    if change == "breaking":
        return f"{major + 1}.0.0" if major else f"0.{minor + 1}.0"
    if change == "feature" and major:
        return f"{major}.{minor + 1}.0"
    if change in {"feature", "patch"}:
        return f"{major}.{minor}.{patch + 1}"
    raise ValueError(f"unknown change type: {change}")


def validate_change(change, packages):
    if set(change) != {"summary", "packages"}:
        raise ValueError("change must contain summary and packages only")
    if not isinstance(change["summary"], str) or not change["summary"].strip():
        raise ValueError("change summary must be nonempty")
    if not isinstance(change["packages"], dict):
        raise ValueError(  # noqa: TRY004
            "packages must be an object (empty for release-neutral changes)"
        )
    for package, kind in change["packages"].items():
        if package not in packages or kind not in {"patch", "feature", "breaking"}:
            raise ValueError(f"invalid package/change: {package}: {kind}")


def plan_versions(baseline, inventory, changes):
    """Release the existing full source/package matrix as one coordinated train.

    Unchanged packages get a patch for the coordinated release. This keeps the
    existing all-package publishers from attempting an occupied stable version.
    """
    levels = {"patch": 0, "feature": 1, "breaking": 2}
    selected = {}
    for change in changes:
        validate_change(change, inventory)
        for package, kind in change["packages"].items():
            if levels[kind] > levels.get(selected.get(package), -1):
                selected[package] = kind
    if not selected:
        return {}
    if set(baseline) != set(inventory):
        raise ValueError(
            "package inventory changed; initialize its reviewed baseline before weekly release"
        )
    if selected.get("core") == "breaking":
        # These integrations expose OpenDAL in their public Rust contracts.
        for package in inventory:
            if package.startswith("integrations/"):
                selected[package] = "breaking"
    return {p: bump(baseline[p], selected.get(p, "patch")) for p in inventory}


def next_rc(version, tags):
    pattern = re.compile(rf"v{re.escape(version)}-rc\.(\d+)")
    numbers = [int(m[1]) for t in tags if (m := pattern.fullmatch(t))]
    return f"v{version}-rc.{max(numbers, default=0) + 1}"
