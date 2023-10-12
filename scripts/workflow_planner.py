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


# Check if we can access secrets based on github context.
def check_secrets(github):
    if (
        github["event_name"] == "push"
        and github["repository"] == "apache/incubator-opendal"
    ):
        return True
    if (
        github["event_name"] == "pull_request"
        and github["event"]["pull_request"]["head"]["full_name"]
        == "apache/incubator-opendal"
        and not github["event"]["pull_request"]["head"]["repo"]["fork"]
    ):
        return True
    return False


def get_provided_cases(github):
    root_dir = ".github/services"

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
    if not check_secrets(github):
        cases[:] = [v for v in cases if "secrets." in v["content"]]

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

    # If any of the core files changed, we will run all cases.
    if any(
        p.startswith("core/src/") and not p.startswith("core/src/services")
        for p in changed_files
    ):
        return cases
    if any(p.startswith("core/tests/") for p in changed_files):
        return cases

    # Filter all cases that not shown un in changed files
    cases = [v for v in cases if any(v["service"] in p for p in changed_files)]
    return cases


# Context is the github context: https://docs.github.com/en/actions/learn-github-actions/contexts#github-context
def plan(github, changed_files):
    # TODO: add bindings/java, bindings/python in the future.
    components = ["core"]
    cases = get_provided_cases(github)

    core_cases = calculate_core_cases(cases, changed_files)

    jobs = {}

    if len(core_cases)> 0:
        jobs["components"] = {"core": True}
        jobs["core"] = [
            {
                "os": "ubuntu-latest",
                "features": ",".join(set([f"services-{v['service']}" for v in core_cases])),
                "cases": [{"setup": v["setup"], 'service': v['service']} for v in core_cases],
            },
            # fs is the only services need to run upon windows, let's hard code it here.
            {
                "os": "windows-latest",
                "features": "services-fs",
                "cases": [{"setup": "local-fs", "service": "fs"}],
            }
        ]

    return json.dumps(jobs)


# For quick test:
#
# `./scripts/workflow_planner.py '{"event_name": "push", "repository": "apache/incubator-opendal"}' PATH`
if __name__ == "__main__":
    github = json.loads(sys.argv[1])
    changed_files = sys.argv[2:]
    result = plan(github, changed_files)
    print(result)
