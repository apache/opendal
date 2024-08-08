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
        result = plan(["core/src/services/fs/mod.rs"])
        self.assertTrue(result["components"]["core"])
        self.assertTrue(len(result["core"]) > 0)

        cases = [v["service"] for v in result["core"][0]["cases"]]
        # Should not contain fs
        self.assertTrue("fs" in cases)
        # Should not contain s3
        self.assertFalse("s3" in cases)

    def test_binding_java(self):
        result = plan(["bindings/java/pom.xml"])
        self.assertFalse(result["components"]["core"])
        self.assertTrue(len(result["core"]) == 0)
        self.assertTrue(result["components"]["binding_java"])
        self.assertTrue(len(result["binding_java"]) > 0)

    def test_bin_ofs(self):
        result = plan(["bin/ofs/Cargo.toml"])
        self.assertTrue(result["components"]["bin_ofs"])
        self.assertTrue(len(result["bin_ofs"][0]["cases"]) > 0)

        result = plan(["core/src/services/fs/mod.rs"])
        cases = [v["service"] for v in result["bin_ofs"][0]["cases"]]
        # Should contain ofs
        self.assertTrue("fs" in cases)
    
    def test_integration_cloudfilter(self):
        result = plan(["integrations/cloudfilter/Cargo.toml"])
        self.assertTrue(result["components"]["integration_cloudfilter"])
        self.assertEqual(result["integration_cloudfilter"][0]["cases"], 
                        [{"service": "fs", "setup": "fixture_data", "feature": "services-fs"}])

        result = plan(["core/src/services/fs/mod.rs"])
        self.assertTrue(result["components"]["integration_cloudfilter"])
        self.assertEqual(result["integration_cloudfilter"][0]["cases"], 
                        [{"service": "fs", "setup": "fixture_data", "feature": "services-fs"}])
        
        result = plan(["core/src/services/s3/mod.rs"])
        self.assertTrue(result["components"]["integration_cloudfilter"])
        self.assertEqual(result["integration_cloudfilter"][0]["cases"], 
                        [{"service": "fs", "setup": "fixture_data", "feature": "services-fs"}])
        
        result = plan(["bindings/java/pom.xml"])
        self.assertFalse(result["components"]["integration_cloudfilter"])
        self.assertTrue(len(result["integration_cloudfilter"]) == 0)


if __name__ == "__main__":
    unittest.main()
