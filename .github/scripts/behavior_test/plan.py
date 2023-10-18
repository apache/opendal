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

import sys
import json
import os
from pathlib import Path

# The path for current script.
SCRIPT_PATH = Path(__file__).parent.absolute()
# The path for `.github` dir.
GITHUB_DIR = SCRIPT_PATH.parent.parent
# The project dir for opendal.
PROJECT_DIR = GITHUB_DIR.parent


def get_provided_cases():
    root_dir = f"{GITHUB_DIR}/services"

    cases = [
        {
            "service": service,
            "setup": setup,
            "content": Path(
                os.path.join(root_dir, service, setup, "action.yml")
            ).read_text(),
        }
        for service in os.listdir(root_dir)
        for setup in os.listdir(os.path.join(root_dir, service))
    ]

    # Check if this workflow needs to read secrets.
    #
    # We will check if pattern `secrets.XXX` exist in content.
    if not os.getenv("GITHUB_HAS_SECRETS") == "true":
        cases[:] = [v for v in cases if "secrets" not in v["content"]]

    return cases


def calculate_core_cases(cases, changed_files):
    # If any of the core workflow changed, we will run all cases.
    for p in [
        ".github/workflows/core_test.yml",
        ".github/workflows/test_planner.yml",
        ".github/actions/test-core",
    ]:
        if p in changed_files:
            return cases

    # Always run all tests if it is a push event.
    if os.getenv("GITHUB_IS_PUSH") == "true":
        return cases

    # If any of the core files changed, we will run all cases.
    if any(
        p.startswith("core/")
        and not p.startswith("core/src/services/")
        and not p.endswith(".md")
        for p in changed_files
    ):
        return cases
    if any(p.startswith("core/tests/") for p in changed_files):
        return cases

    # Filter all cases that not shown un in changed files
    cases = [v for v in cases if any(v["service"] in p for p in changed_files)]
    return cases


# Context is the github context: https://docs.github.com/en/actions/learn-github-actions/contexts#github-context
def plan(changed_files):
    # TODO: add bindings/java, bindings/python in the future.
    components = ["core"]
    cases = get_provided_cases()

    core_cases = calculate_core_cases(cases, changed_files)

    jobs = {}

    if len(core_cases) > 0:
        jobs["components"] = {"core": True}
        jobs["core"] = [
            {
                "os": "ubuntu-latest",
                "features": ",".join(
                    set([f"services-{v['service']}" for v in core_cases])
                ),
                "cases": [
                    {"setup": v["setup"], "service": v["service"]} for v in core_cases
                ],
            },
        ]

        # fs is the only services need to run upon windows, let's hard code it here.
        if "fs" in [v["service"] for v in core_cases]:
            jobs["core"].append(
                {
                    "os": "windows-latest",
                    "features": "services-fs",
                    "cases": [{"setup": "local-fs", "service": "fs"}],
                }
            )

    return jobs


# For quick test:
#
# ./scripts/workflow_planner.py PATH
if __name__ == "__main__":
    changed_files = sys.argv[1:]
    result = plan(changed_files)
    print(json.dumps(result))
