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
from unittest.mock import patch

from plan import plan


class BehaviorTestPlan(unittest.TestCase):
    def test_empty(self):
        result = plan([])
        self.assertEqual(result["components"]["core"], False)
        self.assertEqual(result["components"]["binding_java"], False)
        self.assertEqual(len(result["core"]), 0)
        self.assertEqual(len(result["binding_java"]), 0)

    def test_core_cargo_toml(self):
        result = plan(["core/Cargo.toml"])
        self.assertTrue(result["components"]["core"])

    def test_core_services_fs(self):
        result = plan(["core/services/fs/src/lib.rs"])
        self.assertTrue(result["components"]["core"])
        self.assertTrue(len(result["core"]) > 0)

        cases = [v["service"] for v in result["core"][0]["cases"]]
        # Should contain fs
        self.assertTrue("fs" in cases)
        # Should not contain s3
        self.assertFalse("s3" in cases)

    def test_core_services_hdfs_native_mapping(self):
        result = plan(["core/services/hdfs-native/src/lib.rs"])
        self.assertTrue(result["components"]["core"])
        self.assertTrue(len(result["core"]) > 0)

        cases = [v["service"] for v in result["core"][0]["cases"]]
        self.assertTrue("hdfs_native" in cases)
        self.assertFalse("fs" in cases)

    @patch.dict("os.environ", {"GITHUB_HAS_SECRETS": "true"})
    def test_s3_crate_schedules_provider_cases(self):
        result = plan(["core/services/s3/src/lib.rs"])
        core_cases = {
            (case["service"], case["feature"])
            for target in result["core"]
            for case in target["cases"]
        }
        self.assertEqual(
            core_cases,
            {
                ("s3", "services-s3"),
                ("minio", "services-minio"),
                ("r2", "services-r2"),
            },
        )

        go_services = {
            case["service"]
            for target in result["binding_go"]
            for case in target["cases"]
        }
        ruby_services = {
            case["service"]
            for target in result["binding_ruby"]
            for case in target["cases"]
        }
        self.assertTrue({"minio", "r2"}.isdisjoint(go_services))
        self.assertTrue({"minio", "r2"} <= ruby_services)

    def test_binding_java(self):
        result = plan(["bindings/java/pom.xml"])
        self.assertFalse(result["components"]["core"])
        self.assertTrue(len(result["core"]) == 0)
        self.assertTrue(result["components"]["binding_java"])
        self.assertTrue(len(result["binding_java"]) > 0)

    def test_binding_java_excludes_hf(self):
        result = plan(["bindings/java/pom.xml"])
        cases = [v["service"] for target in result["binding_java"] for v in target["cases"]]
        self.assertFalse("hf" in cases)

    def test_integration_object_store(self):
        result = plan(["integrations/object_store/Cargo.toml"])
        self.assertTrue(result["components"]["integration_object_store"])
        self.assertTrue(len(result["integration_object_store"]) > 0)

        result = plan(["core/services/fs/src/lib.rs"])
        cases = [v["service"] for v in result["integration_object_store"][0]["cases"]]
        # Should contain fs
        self.assertTrue("fs" in cases)

if __name__ == "__main__":
    unittest.main()
