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

import unittest
from pathlib import Path

from plan import (
    BINDING_PR_SMOKE_SETUPS,
    CORE_PR_SMOKE_SETUPS,
    INTEGRATIONS,
    LANGUAGE_BINDING,
    group_cases_by_service,
    plan,
    provided_cases,
    validate_nightly_plan,
)


class BehaviorTestPlan(unittest.TestCase):
    def test_empty(self):
        result = plan([])
        self.assertEqual(result["components"]["core"], False)
        self.assertEqual(result["components"]["binding_java"], False)
        self.assertEqual(len(result["core"]), 0)
        self.assertEqual(len(result["binding_java"]), 0)
        self.assertEqual(result["notes"], [])

    def test_shared_core_change_runs_local_smoke_only_without_binding_fanout(self):
        result = plan(["core/Cargo.toml"])
        self.assertTrue(result["components"]["core"])
        ubuntu_cases = result["core"][0]["cases"]
        actual_setups = {
            (c["service"], s) for c in ubuntu_cases for s in c["setups"]
        }
        self.assertEqual(actual_setups, CORE_PR_SMOKE_SETUPS)
        # Windows fs smoke is included because fs is in the core smoke set.
        windows = next(v for v in result["core"] if v["os"] == "windows-latest")
        self.assertEqual(windows["cases"][0]["setups"], ["local_fs"])
        # Shared core changes do not fan out to all 8 bindings on PR.
        for language in LANGUAGE_BINDING:
            self.assertFalse(result["components"][f"binding_{language}"], language)
        self.assertFalse(result["components"]["integration_object_store"])

    def test_single_service_change_is_core_only_without_binding_fanout(self):
        result = plan(["core/services/fs/src/lib.rs"])
        self.assertTrue(result["components"]["core"])
        cases = [v["service"] for v in result["core"][0]["cases"]]
        self.assertEqual(cases, ["fs"])
        for language in LANGUAGE_BINDING:
            self.assertFalse(result["components"][f"binding_{language}"], language)
        self.assertFalse(result["components"]["integration_object_store"])

    def test_azure_common_expands_to_dependent_services(self):
        result = plan(["core/services/azure-common/src/lib.rs"])
        self.assertTrue(result["components"]["core"])
        core_services = {c["service"] for c in result["core"][0]["cases"]}
        # azblob has a local Azurite fixture; azdls/azfile/ghac emit explicit notes.
        self.assertEqual(core_services, {"azblob"})
        notes_text = "\n".join(result["notes"])
        self.assertIn("azdls", notes_text)
        self.assertIn("azfile", notes_text)
        self.assertIn("ghac", notes_text)

    def test_live_only_service_emits_explicit_note_without_live_fixture(self):
        result = plan(["core/services/gcs/src/backend.rs"])
        self.assertFalse(result["components"]["core"])
        self.assertTrue(
            any("gcs" in note and "Nightly Tier B" in note for note in result["notes"])
        )

    def test_github_fixture_is_live_only_and_excluded_from_pr(self):
        result = plan([".github/services/github/read_only/action.yml"])
        self.assertFalse(result["components"]["core"])
        self.assertTrue(any("github" in note for note in result["notes"]))

    def test_smb_fixture_is_core_only(self):
        result = plan([
            ".github/services/smb/samba/action.yml",
            "core/services/fs/src/lib.rs",
        ])
        core = {
            case["service"]
            for target in result["core"]
            for case in target["cases"]
        }
        self.assertEqual(core, {"smb", "fs"})
        for language in LANGUAGE_BINDING:
            self.assertFalse(result["components"][f"binding_{language}"], language)

    def test_core_services_hdfs_native_mapping(self):
        result = plan(["core/services/hdfs-native/src/lib.rs"])
        self.assertTrue(result["components"]["core"])
        cases = [v["service"] for v in result["core"][0]["cases"]]
        self.assertEqual(cases, ["hdfs_native"])

    def test_group_core_cases_by_service(self):
        cases = [
            {
                "service": "webdav",
                "setup": "nginx_with_empty_password",
                "feature": "services-webdav",
            },
            {
                "service": "webdav",
                "setup": "nginx_with_password",
                "feature": "services-webdav",
            },
            {
                "service": "memory",
                "setup": "memory",
                "feature": "services-memory",
            },
        ]

        self.assertEqual(
            group_cases_by_service(cases),
            [
                {
                    "service": "webdav",
                    "feature": "services-webdav",
                    "setups": [
                        "nginx_with_empty_password",
                        "nginx_with_password",
                    ],
                },
                {
                    "service": "memory",
                    "feature": "services-memory",
                    "setups": ["memory"],
                },
            ],
        )

    def test_core_groups_multiple_setups(self):
        result = plan(["core/services/webdav/src/lib.rs"])
        cases = result["core"][0]["cases"]
        webdav = next(v for v in cases if v["service"] == "webdav")

        self.assertIn("nginx_with_empty_password", webdav["setups"])
        self.assertIn("nginx_with_password", webdav["setups"])
        self.assertIn("nginx_with_redirect", webdav["setups"])
        self.assertEqual(
            len([v for v in cases if v["service"] == "webdav"]),
            1,
        )

    def test_core_action_and_planner_changes_trigger_core_smoke(self):
        for changed in (
            ".github/actions/test_behavior_core/action.yaml",
            ".github/scripts/test_behavior/plan.py",
        ):
            with self.subTest(changed=changed):
                result = plan([changed])
                self.assertTrue(result["components"]["core"])
                actual_setups = {
                    (c["service"], s)
                    for c in result["core"][0]["cases"]
                    for s in c["setups"]
                }
                self.assertEqual(actual_setups, CORE_PR_SMOKE_SETUPS)

    def test_service_setups_do_not_checkout(self):
        service_dir = Path(__file__).parents[2] / "services"
        setup_actions = service_dir.glob("*/*/action.yml")

        for action in setup_actions:
            self.assertNotIn("actions/checkout@", action.read_text(), str(action))

    def test_binding_java_runs_local_smoke_set_only(self):
        result = plan(["bindings/java/pom.xml"])
        self.assertFalse(result["components"]["core"])
        self.assertTrue(result["components"]["binding_java"])
        actual = {
            (c["service"], c["setup"])
            for target in result["binding_java"]
            for c in target["cases"]
        }
        self.assertEqual(actual, BINDING_PR_SMOKE_SETUPS)

    def test_binding_c_change_triggers_c_and_go_smoke(self):
        result = plan(["bindings/c/src/lib.rs"])
        self.assertTrue(result["components"]["binding_c"])
        self.assertTrue(result["components"]["binding_go"])
        self.assertFalse(result["components"]["binding_java"])
        go_oses = {target["os"] for target in result["binding_go"]}
        self.assertEqual(go_oses, {"ubuntu-latest", "windows-latest", "macos-latest"})

    def test_dependency_union_combines_core_and_service_and_binding(self):
        result = plan([
            "core/core/src/types/operator/operator.rs",
            "core/services/webdav/src/backend.rs",
            "bindings/python/src/lib.rs",
        ])
        core_services = {c["service"] for c in result["core"][0]["cases"]}
        self.assertTrue({"memory", "fs", "s3", "azblob", "http", "webdav"} <= core_services)
        self.assertTrue(result["components"]["binding_python"])
        self.assertFalse(result["components"]["binding_java"])

    def test_integration_object_store(self):
        result = plan(["integrations/object_store/Cargo.toml"])
        self.assertTrue(result["components"]["integration_object_store"])
        services = {
            v["service"] for v in result["integration_object_store"][0]["cases"]
        }
        self.assertEqual(services, {"fs", "s3"})

    def test_nightly_local_and_live_partition_all_setups(self):
        service_dir = Path(__file__).parents[2] / "services"
        all_setups = {
            (action.parents[1].name, action.parent.name)
            for action in service_dir.glob("*/*/action.yml")
        }
        local_setups = {
            (c["service"], c["setup"]) for c in provided_cases(mode="nightly-local")
        }
        live_setups = {
            (c["service"], c["setup"]) for c in provided_cases(mode="nightly-live")
        }

        self.assertEqual(local_setups & live_setups, set())
        self.assertEqual(local_setups | live_setups, all_setups)
        self.assertNotIn(("github", "read_only"), local_setups)
        self.assertIn(("github", "read_only"), live_setups)

    def test_nightly_local_ignores_empty_diff_and_covers_all_components(self):
        result = plan([], mode="nightly-local")
        self.assertTrue(result["components"]["core"])
        for language in LANGUAGE_BINDING:
            self.assertTrue(result["components"][f"binding_{language}"], language)
        for integration in INTEGRATIONS:
            self.assertTrue(
                result["components"][f"integration_{integration}"], integration
            )

        core_setups = {
            (case["service"], setup)
            for target in result["core"]
            for case in target["cases"]
            for setup in case["setups"]
        }
        self.assertIn(("memory", "memory"), core_setups)
        self.assertIn(("fs", "local_fs"), core_setups)
        self.assertNotIn(("github", "read_only"), core_setups)
        self.assertNotIn(("s3", "aws_s3"), core_setups)

        obj_services = {
            case["service"]
            for target in result["integration_object_store"]
            for case in target["cases"]
        }
        self.assertEqual(obj_services, {"fs", "s3"})

        go_oses = {target["os"] for target in result["binding_go"]}
        self.assertEqual(go_oses, {"ubuntu-latest", "windows-latest", "macos-latest"})

    def test_nightly_live_ignores_empty_diff_and_excludes_local_setups(self):
        result = plan([], mode="nightly-live")
        self.assertTrue(result["components"]["core"])
        self.assertTrue(result["components"]["integration_object_store"])

        core_cases = result["core"][0]["cases"]
        # Each live setup runs as an independent core job for per-setup reporting.
        self.assertTrue(all(len(c["setups"]) == 1 for c in core_cases))
        self.assertTrue(all("label" in c for c in core_cases))
        core_setups = {
            (case["service"], setup)
            for target in result["core"]
            for case in target["cases"]
            for setup in case["setups"]
        }
        self.assertIn(("github", "read_only"), core_setups)
        self.assertIn(("s3", "aws_s3"), core_setups)
        self.assertNotIn(("fs", "local_fs"), core_setups)
        self.assertNotIn(("memory", "memory"), core_setups)

        go_oses = {target["os"] for target in result["binding_go"]}
        self.assertEqual(go_oses, {"ubuntu-latest"})

    def test_validate_nightly_plan_rejects_empty_components(self):
        empty = {"components": {"core": False}, "core": []}
        with self.assertRaises(RuntimeError):
            validate_nightly_plan(empty, "nightly-local")
        with self.assertRaises(RuntimeError):
            validate_nightly_plan(empty, "nightly-live")


if __name__ == "__main__":
    unittest.main()
