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

import argparse
import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# The path for current script.
SCRIPT_PATH = Path(__file__).parent.absolute()
# The path for `.github` dir.
GITHUB_DIR = SCRIPT_PATH.parent.parent
# The project dir for opendal.
PROJECT_DIR = GITHUB_DIR.parent

LANGUAGE_BINDING = ["java", "python", "ruby", "nodejs", "go", "c", "cpp", "dotnet"]

INTEGRATIONS = ["object_store"]

SHARED_SERVICE_SCHEMES = {
    "s3": {"s3", "minio"},
}

VALID_MODES = ("pr", "nightly-local", "nightly-live")

# Setups that access external network services without 1Password secrets.
ONLINE_SETUPS = {
    ("github", "read_only"),
}

# Deterministic local smoke setups for shared core / workflow PRs.
CORE_PR_SMOKE_SETUPS = {
    ("memory", "memory"),
    ("fs", "local_fs"),
    ("s3", "0_silo_s3"),
    ("azblob", "azurite_azblob"),
    ("http", "nginx"),
}

# Deterministic local smoke setups for binding PRs.
BINDING_PR_SMOKE_SETUPS = {
    ("memory", "memory"),
    ("fs", "local_fs"),
    ("s3", "0_silo_s3"),
}

# Helper crates under core/services/ that are not standalone OpenDAL services.
SERVICE_HELPER_CRATES = {"azure_common"}

# Direct service crate dependency edges (verified from core/services/*/Cargo.toml).
SERVICE_DEPENDENTS = {
    "azure_common": ["azblob", "azdls", "azfile", "ghac"],
    "azblob": ["ghac"],
}


def is_live_setup(service: str, setup: str, content: str) -> bool:
    return "op://services" in content or (service, setup) in ONLINE_SETUPS


def _all_service_cases() -> list[dict[str, Any]]:
    root_dir = f"{GITHUB_DIR}/services"
    cases = [
        {
            "service": service,
            "setup": setup,
            "feature": "services-{}".format(service.replace("_", "-")),
            "live": is_live_setup(
                service,
                setup,
                Path(os.path.join(root_dir, service, setup, "action.yml")).read_text(),
            ),
        }
        for service in os.listdir(root_dir)
        for setup in os.listdir(os.path.join(root_dir, service))
        if os.path.exists(os.path.join(root_dir, service, setup, "action.yml"))
    ]
    return sorted(cases, key=lambda x: (x["service"], x["setup"]))


def provided_cases(mode: str = "pr") -> list[dict[str, str]]:
    if mode not in VALID_MODES:
        raise ValueError(f"unsupported planner mode: {mode}")

    cases = _all_service_cases()
    if mode in ("pr", "nightly-local"):
        cases = [v for v in cases if not v["live"]]
    elif mode == "nightly-live":
        cases = [v for v in cases if v["live"]]

    return [
        {
            "setup": v["setup"],
            "service": v["service"],
            "feature": v["feature"],
        }
        for v in cases
    ]


@dataclass
class Hint:
    core_smoke: bool = field(default=False, init=False)
    core_services: set[str] = field(default_factory=set, init=False)
    binding_smoke: set[str] = field(default_factory=set, init=False)
    integration_smoke: set[str] = field(default_factory=set, init=False)


def _expand_service_dependents(service: str) -> set[str]:
    norm = service.replace("-", "_")
    affected: set[str] = set()
    queue = [norm]
    while queue:
        cur = queue.pop()
        if cur in affected:
            continue
        affected.add(cur)
        for dep in SERVICE_DEPENDENTS.get(cur, []):
            if dep not in affected:
                queue.append(dep)
    return affected


def calculate_hint(changed_files: list[str]) -> Hint:
    hint = Hint()

    # Ignore documentation-only markdown files.
    changed_files = [f for f in changed_files if not f.endswith(".md")]

    def mark_service_affected(service: str) -> None:
        for svc in _expand_service_dependents(service):
            if svc not in SERVICE_HELPER_CRATES:
                hint.core_services.update(SHARED_SERVICE_SCHEMES.get(svc, {svc}))

    for p in changed_files:
        # Behavior workflow / planner wiring changes.
        if p == ".github/workflows/test_behavior.yml":
            hint.core_smoke = True
            hint.binding_smoke.add("c")
            hint.integration_smoke.add("object_store")

        if (
            p == ".github/workflows/test_behavior_core.yml"
            or p.startswith(".github/actions/test_behavior_core/")
            or p.startswith(".github/scripts/test_behavior/")
        ):
            hint.core_smoke = True

        for language in LANGUAGE_BINDING:
            if p == f".github/workflows/test_behavior_binding_{language}.yml" or p.startswith(
                f".github/actions/test_behavior_binding_{language}/"
            ):
                hint.binding_smoke.add(language)

        for integration in INTEGRATIONS:
            if (
                p == f".github/workflows/test_behavior_integration_{integration}.yml"
                or p.startswith(
                    f".github/actions/test_behavior_integration_{integration}/"
                )
            ):
                hint.integration_smoke.add(integration)

        # Shared core runtime, facade, layers, HTTP transports, and testkit.
        if (
            p.startswith("core/")
            and not p.startswith("core/benches/")
            and not p.startswith("core/edge/")
            and not p.startswith("core/fuzz/")
            and not p.startswith("core/src/services/")
            and not p.startswith("core/core/src/services/")
            and not p.startswith("core/services/")
            and not p.startswith("core/src/docs/")
            and not p.startswith("core/core/src/docs/")
        ):
            hint.core_smoke = True

        # Language bindings (C ABI changes also verify Go runtime).
        for language in LANGUAGE_BINDING:
            if p.startswith(f"bindings/{language}/"):
                hint.binding_smoke.add(language)

        if p.startswith("bindings/c/"):
            hint.binding_smoke.add("c")
            hint.binding_smoke.add("go")

        if p.startswith(".github/scripts/test_go_binding/"):
            hint.binding_smoke.add("go")

        # Integrations.
        for integration in INTEGRATIONS:
            if p.startswith(f"integrations/{integration}/"):
                hint.integration_smoke.add(integration)

        # Service-specific paths (core only; no cross-binding fan-out on PR).
        for pattern in (
            r"core/src/services/([^/]+)/",
            r"core/services/([^/]+)/",
            r"core/core/src/services/([^/]+)/",
            r"\.github/services/([^/]+)/",
            r"fixtures/([^/]+)/",
        ):
            match = re.search(pattern, p)
            if match:
                mark_service_affected(match.group(1))

    return hint


# `unique_cases` picks one setup for each service (first alphabetically).
def unique_cases(cases: list[dict[str, str]]) -> list[dict[str, str]]:
    ucases = {}
    for case in cases:
        service = case["service"]
        if service not in ucases:
            ucases[service] = case
    return list(ucases.values())


def group_cases_by_service(cases: list[dict[str, str]]) -> list[dict[str, Any]]:
    grouped_cases = {}
    for case in cases:
        service = case["service"]
        if service not in grouped_cases:
            grouped_cases[service] = {
                "service": service,
                "feature": case["feature"],
                "setups": [],
            }
        grouped_cases[service]["setups"].append(case["setup"])

    return list(grouped_cases.values())


def generate_core_cases(
    cases: list[dict[str, str]], hint: Hint, mode: str = "pr"
) -> list[dict[str, str]]:
    if mode in ("nightly-local", "nightly-live"):
        return cases

    selected = []
    for case in cases:
        in_service = case["service"] in hint.core_services
        in_smoke = hint.core_smoke and (
            case["service"],
            case["setup"],
        ) in CORE_PR_SMOKE_SETUPS
        if in_service or in_smoke:
            selected.append(case)
    return selected


def _filter_binding_cases(
    cases: list[dict[str, str]], language: str
) -> list[dict[str, str]]:
    cases = unique_cases(cases)

    # Disable aliyun_drive case for every language due to upstream rate limits.
    cases = [v for v in cases if v["service"] != "aliyun_drive"]

    # These fixtures target core services without enabling bindings.
    cases = [v for v in cases if v["service"] not in ["github", "hf3fs"]]

    # The default Java and .NET native libraries omit the services-ftp feature.
    if language in ("java", "dotnet"):
        cases = [v for v in cases if v["service"] != "ftp"]

    if language == "java":
        cases = [
            v
            for v in cases
            if v["service"]
            not in [
                "compfs",
                # FIXME: Re-enable hf after https://github.com/apache/opendal/issues/7367 is fixed.
                "hf",
                "hdfs",
                "hdfs_native",
                "monoiofs",
                "rocksdb",
            ]
        ]

    if language == "dotnet":
        cases = [
            v
            for v in cases
            if v["service"]
            not in [
                "hdfs",
                "hdfs_native",
                "rocksdb",
            ]
        ]

    if language == "go":
        cases = [
            v
            for v in cases
            if v["service"]
            not in [
                # opendal-go-services does not provide these services yet.
                "tos",
                "minio",
            ]
        ]

    # Ruby binding only integrates a subset of services (see bindings/ruby/Cargo.toml).
    if language == "ruby":
        cases = [
            v
            for v in cases
            if v["service"]
            in [
                "azblob",
                "azdls",
                "cos",
                "fs",
                "gcs",
                "ghac",
                "http",
                "ipmfs",
                "memory",
                "obs",
                "oss",
                "minio",
                "s3",
                "webdav",
                "webhdfs",
                "azfile",
            ]
        ]

    return cases


def generate_language_binding_cases(
    cases: list[dict[str, str]], hint: Hint, language: str, mode: str = "pr"
) -> list[dict[str, str]]:
    cases = _filter_binding_cases(cases, language)

    if mode in ("nightly-local", "nightly-live"):
        return cases

    if language not in hint.binding_smoke:
        return []

    return [
        v for v in cases if (v["service"], v["setup"]) in BINDING_PR_SMOKE_SETUPS
    ]


def generate_integration_cases(
    cases: list[dict[str, str]], hint: Hint, integration: str, mode: str = "pr"
) -> list[dict[str, str]]:
    cases = unique_cases(cases)

    if integration == "object_store":
        supported_services = ["fs", "s3"]
        cases = [v for v in cases if v["service"] in supported_services]

    if mode in ("nightly-local", "nightly-live"):
        return cases

    if integration not in hint.integration_smoke:
        return []

    return cases


def validate_nightly_plan(jobs: dict[str, Any], mode: str) -> None:
    if mode == "nightly-local":
        if not jobs["components"]["core"]:
            raise RuntimeError("nightly-local plan must include core")
        core_services = {
            case["service"] for target in jobs["core"] for case in target["cases"]
        }
        missing_core = {"memory", "fs", "s3", "azblob", "http"} - core_services
        if missing_core:
            raise RuntimeError(
                f"nightly-local plan missing core services: {sorted(missing_core)}"
            )
        for language in LANGUAGE_BINDING:
            if not jobs["components"][f"binding_{language}"]:
                raise RuntimeError(
                    f"nightly-local plan must include binding_{language}"
                )
        for integration in INTEGRATIONS:
            if not jobs["components"][f"integration_{integration}"]:
                raise RuntimeError(
                    f"nightly-local plan must include integration_{integration}"
                )
    elif mode == "nightly-live":
        if not jobs["components"]["core"]:
            raise RuntimeError("nightly-live plan must include core")
        core_services = {
            case["service"] for target in jobs["core"] for case in target["cases"]
        }
        if "github" not in core_services:
            raise RuntimeError("nightly-live plan must include github/read_only")
        if not jobs["components"]["integration_object_store"]:
            raise RuntimeError(
                "nightly-live plan must include integration_object_store"
            )


def _pr_coverage_notes(hint: Hint, local_cases: list[dict[str, str]]) -> list[str]:
    if not hint.core_services:
        return []
    local_services = {c["service"] for c in local_cases}
    live_services = {c["service"] for c in _all_service_cases() if c["live"]}
    notes: list[str] = []
    for svc in sorted(hint.core_services):
        if svc in local_services:
            continue
        if svc == "ghac":
            notes.append(
                "Service 'ghac' has no local behavior fixture here; it is verified via service_test_ghac.yml and Nightly Tier B."
            )
        elif svc in live_services:
            notes.append(
                f"Service '{svc}' only has live/online behavior fixtures; PR runs local deterministic tests only. Live verification runs in Nightly Tier B or via manual workflow_dispatch (mode=nightly-live)."
            )
        else:
            notes.append(
                f"Service '{svc}' has no configured behavior test fixture under .github/services/."
            )
    return notes


def plan(changed_files: list[str], mode: str | None = None) -> dict[str, Any]:
    resolved_mode = mode or os.getenv("BEHAVIOR_PLAN_MODE", "pr")
    cases = provided_cases(mode=resolved_mode)
    hint = calculate_hint(changed_files)

    if resolved_mode == "nightly-live":
        core_cases = [
            {
                "service": c["service"],
                "feature": c["feature"],
                "setups": [c["setup"]],
                "label": f"{c['service']} / {c['setup']}",
            }
            for c in generate_core_cases(cases, hint, mode=resolved_mode)
        ]
    else:
        core_cases = group_cases_by_service(
            generate_core_cases(cases, hint, mode=resolved_mode)
        )

    jobs: dict[str, Any] = {
        "components": {
            "core": False,
        },
        "core": [],
        "notes": _pr_coverage_notes(hint, cases) if resolved_mode == "pr" else [],
    }

    if len(core_cases) > 0:
        jobs["components"]["core"] = True
        linux_cases = [v for v in core_cases if v["service"] != "hf3fs"]
        hf3fs_cases = [v for v in core_cases if v["service"] == "hf3fs"]
        if linux_cases:
            jobs["core"].append({"os": "ubuntu-latest", "cases": linux_cases})
        if hf3fs_cases:
            # Ubuntu 24.04 Azure runners currently omit rdma_rxe; see
            # .github/services/hf3fs/README.md for the transition plan before
            # Ubuntu 22.04 leaves standard support in May 2027.
            jobs["core"].append({"os": "ubuntu-22.04", "cases": hf3fs_cases})

        # fs is the only service tested on Windows core runners.
        if "fs" in [v["service"] for v in core_cases]:
            jobs["core"].append(
                {
                    "os": "windows-latest",
                    "cases": [
                        {
                            "setups": ["local_fs"],
                            "service": "fs",
                            "feature": "services-fs",
                        }
                    ],
                }
            )
    for language in LANGUAGE_BINDING:
        jobs[f"binding_{language}"] = []
        jobs["components"][f"binding_{language}"] = False
        language_cases = generate_language_binding_cases(
            cases, hint, language, mode=resolved_mode
        )
        if len(language_cases) > 0:
            jobs["components"][f"binding_{language}"] = True
            jobs[f"binding_{language}"].append(
                {"os": "ubuntu-latest", "cases": language_cases}
            )
            if language == "go" and "fs" in [v["service"] for v in language_cases]:
                # Add fs service to ensure the go binding works on Windows and macOS.
                jobs[f"binding_{language}"].append(
                    {
                        "os": "windows-latest",
                        "cases": [
                            {
                                "setup": "local_fs",
                                "service": "fs",
                                "feature": "services-fs",
                            }
                        ],
                    }
                )
                jobs[f"binding_{language}"].append(
                    {
                        "os": "macos-latest",
                        "cases": [
                            {
                                "setup": "local_fs",
                                "service": "fs",
                                "feature": "services-fs",
                            }
                        ],
                    }
                )

    for integration in INTEGRATIONS:
        jobs[f"integration_{integration}"] = []
        jobs["components"][f"integration_{integration}"] = False
        integration_cases = generate_integration_cases(
            cases, hint, integration, mode=resolved_mode
        )
        if len(integration_cases) > 0:
            jobs["components"][f"integration_{integration}"] = True
            jobs[f"integration_{integration}"].append(
                {"os": "ubuntu-latest", "cases": integration_cases}
            )

    if resolved_mode in ("nightly-local", "nightly-live"):
        validate_nightly_plan(jobs, resolved_mode)

    return jobs


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Plan OpenDAL behavior tests")
    parser.add_argument(
        "--mode",
        choices=VALID_MODES,
        default=os.getenv("BEHAVIOR_PLAN_MODE", "pr"),
        help="Planning mode: pr, nightly-local, or nightly-live",
    )
    parser.add_argument("changed_files", nargs="*")
    args = parser.parse_args(argv)
    result = plan(args.changed_files, mode=args.mode)
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
