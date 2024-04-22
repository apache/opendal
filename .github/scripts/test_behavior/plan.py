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

import json
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# The path for current script.
SCRIPT_PATH = Path(__file__).parent.absolute()
# The path for `.github` dir.
GITHUB_DIR = SCRIPT_PATH.parent.parent
# The project dir for opendal.
PROJECT_DIR = GITHUB_DIR.parent

LANGUAGE_BINDING = ["java", "python", "nodejs"]

BIN = ["ofs"]

def provided_cases() -> list[dict[str, str]]:
    root_dir = f"{GITHUB_DIR}/services"

    cases = [
        {
            "service": service,
            "setup": setup,
            "feature": "services-{}".format(service.replace("_", "-")),
            "content": Path(
                os.path.join(root_dir, service, setup, "action.yml")
            ).read_text(),
        }
        for service in os.listdir(root_dir)
        for setup in os.listdir(os.path.join(root_dir, service))
        if os.path.exists(os.path.join(root_dir, service, setup, "action.yml"))
    ]

    # Check if this workflow needs to read secrets.
    #
    # We will check if pattern `op://services` exist in content.
    if not os.getenv("GITHUB_HAS_SECRETS") == "true":
        cases[:] = [v for v in cases if "op://services" not in v["content"]]

    # Remove content from cases.
    cases = [
        {
            "setup": v["setup"],
            "service": v["service"],
            "feature": v["feature"],
        }
        for v in cases
    ]

    # Make sure the order is stable.
    sorted_cases = sorted(cases, key=lambda x: (x["service"], x["setup"]))
    return sorted_cases


@dataclass
class Hint:
    # Is core affected?
    core: bool = field(default=False, init=False)
    # Is binding java affected?
    binding_java: bool = field(default=False, init=False)
    # Is binding python affected?
    binding_python: bool = field(default=False, init=False)
    # Is binding nodejs affected?
    binding_nodejs: bool = field(default=False, init=False)
    # Is bin ofs affected?
    bin_ofs: bool = field(default=False, init=False)

    # Should we run all services test?
    all_service: bool = field(default=False, init=False)
    # affected services set.
    services: set = field(default_factory=set, init=False)


def calculate_hint(changed_files: list[str]) -> Hint:
    hint = Hint()

    # Remove all files that ends with `.md`
    changed_files = [f for f in changed_files if not f.endswith(".md")]

    for p in changed_files:
        # workflow behavior tests affected
        if p == ".github/workflows/test_behavior.yml":
            hint.core = True
            for language in LANGUAGE_BINDING:
                setattr(hint, f"binding_{language}", True)
            hint.all_service = True

        if p == ".github/workflows/test_behavior_core.yml":
            hint.core = True
            hint.all_service = True

        for language in LANGUAGE_BINDING:
            if p == f".github/workflows/test_behavior_binding_{language}.yml":
                setattr(hint, f"binding_{language}", True)
                hint.all_service = True
        for bin in BIN:
            if p == f".github/workflows/test_behavior_bin_{bin}.yml":
                setattr(hint, f"bin_{bin}", True)
                hint.all_service = True

        # core affected
        if (
            p.startswith("core/")
            and not p.startswith("core/benches/")
            and not p.startswith("core/edge/")
            and not p.startswith("core/fuzz/")
            and not p.startswith("core/src/services/")
        ):
            hint.core = True
            hint.binding_java = True
            hint.binding_python = True
            hint.binding_nodejs = True
            hint.bin_ofs = True
            hint.all_service = True

        # language binding affected
        for language in LANGUAGE_BINDING:
            if p.startswith(f"bindings/{language}/"):
                setattr(hint, f"binding_{language}", True)
                hint.all_service = True

        # bin affected
        for bin in BIN:
            if p.startswith(f"bin/{bin}"):
                setattr(hint, f"bin_{bin}", True)
                hint.all_service = True

        # core service affected
        match = re.search(r"core/src/services/([^/]+)/", p)
        if match:
            hint.core = True
            for language in LANGUAGE_BINDING:
                setattr(hint, f"binding_{language}", True)
            for bin in BIN:
                setattr(hint, f"bin_{bin}", True)
            hint.services.add(match.group(1))

        # core test affected
        match = re.search(r".github/services/([^/]+)/", p)
        if match:
            hint.core = True
            for language in LANGUAGE_BINDING:
                setattr(hint, f"binding_{language}", True)
            for bin in BIN:
                setattr(hint, f"bin_{bin}", True)
            hint.services.add(match.group(1))

        # fixture affected
        match = re.search(r"fixtures/([^/]+)/", p)
        if match:
            hint.core = True
            for language in LANGUAGE_BINDING:
                setattr(hint, f"binding_{language}", True)
            for bin in BIN:
                setattr(hint, f"bin_{bin}", True)
            hint.services.add(match.group(1))

    return hint


# unique_cases is used to only one setup for each service.
#
# We need this because we have multiple setup for each service and they have already been
# tested by `core` workflow. So we can only test unique setup for each service for bindings.
#
# We make sure that we return the first setup for each service in alphabet order.
def unique_cases(cases):
    ucases = {}
    for case in cases:
        service = case["service"]
        if service not in ucases:
            ucases[service] = case

    # Convert the dictionary back to a list if needed
    return list(ucases.values())


def generate_core_cases(
    cases: list[dict[str, str]], hint: Hint
) -> list[dict[str, str]]:
    # Always run all tests if it is a push event.
    if os.getenv("GITHUB_IS_PUSH") == "true":
        return cases

    # Return empty if core is False
    if not hint.core:
        return []

    # Return all services if all_service is True
    if hint.all_service:
        return cases

    # Filter all cases that not shown un in changed files
    cases = [v for v in cases if v["service"] in hint.services]
    return cases


def generate_language_binding_cases(
    cases: list[dict[str, str]], hint: Hint, language: str
) -> list[dict[str, str]]:
    cases = unique_cases(cases)

    # Remove specified services cases for java.
    excluded_services = {"hdfs", "hdfs-native"}  # Use a set for faster lookups
    if language == "java":
        cases = [v for v in cases if v["service"] not in excluded_services]

    if os.getenv("GITHUB_IS_PUSH") == "true":
        return cases

    # Return empty if this binding is False
    if not getattr(hint, f"binding_{language}"):
        return []

    # Return all services if all_service is True
    if hint.all_service:
        return cases

    # Filter all cases that not shown un in changed files
    cases = [v for v in cases if v["service"] in hint.services]
    return cases

def generate_bin_cases(
    cases: list[dict[str, str]], hint: Hint, bin: str
) -> list[dict[str, str]]:
    # Return empty if this bin is False
    if not getattr(hint, f"bin_{bin}"):
        return []
    
    cases = unique_cases(cases)

    if bin == "ofs":
        supported_services = ["fs", "s3"]
        cases = [v for v in cases if v["service"] in supported_services]

    # Return all services if all_service is True
    if hint.all_service:
        return cases

    # Filter all cases that not shown un in changed files
    cases = [v for v in cases if v["service"] in hint.services]

    return cases


def plan(changed_files: list[str]) -> dict[str, Any]:
    cases = provided_cases()
    hint = calculate_hint(changed_files)

    core_cases = generate_core_cases(cases, hint)

    jobs = {
        "components": {
            "core": False,
        },
        "core": [],
    }

    if len(core_cases) > 0:
        jobs["components"]["core"] = True
        jobs["core"].append({"os": "ubuntu-latest", "cases": core_cases})

        # fs is the only services need to run upon windows, let's hard code it here.
        if "fs" in [v["service"] for v in core_cases]:
            jobs["core"].append(
                {
                    "os": "windows-latest",
                    "cases": [
                        {"setup": "local_fs", "service": "fs", "feature": "services-fs"}
                    ],
                }
            )
    for language in LANGUAGE_BINDING:
        jobs[f"binding_{language}"] = []
        jobs["components"][f"binding_{language}"] = False
        language_cases = generate_language_binding_cases(cases, hint, language)
        if len(language_cases) > 0:
            jobs["components"][f"binding_{language}"] = True
            jobs[f"binding_{language}"].append({"os": "ubuntu-latest", "cases": language_cases})

    for bin in BIN:
        jobs[f"bin_{bin}"] = []
        jobs["components"][f"bin_{bin}"] = False
        bin_cases = generate_bin_cases(cases, hint, bin)
        if len(bin_cases) > 0:
            jobs["components"][f"bin_{bin}"] = True
            jobs[f"bin_{bin}"].append({"os": "ubuntu-latest", "cases": bin_cases})
    return jobs


if __name__ == "__main__":
    changed_files = sys.argv[1:]
    result = plan(changed_files)
    print(json.dumps(result))
