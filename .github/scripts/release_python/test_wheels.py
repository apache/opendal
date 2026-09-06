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

import base64
import csv
import hashlib
import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from wheels import LICENSE_EXPRESSION, LICENSE_FILES, RUNTIME_NOTICE, complete_wheel

DIST_INFO = "opendal-0.47.8rc1.dist-info"
RUNTIME = "opendal.libs/libgcc_s-12345678.so.1"
EXTENSION = "opendal/_opendal.abi3.so"
SBOM = f"{DIST_INFO}/sboms/opendal-python.cyclonedx.json"


class WheelRuntimeTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.wheel = Path(self.directory.name) / "opendal.whl"

    def write_wheel(self, runtimes=(RUNTIME,), sbom=True):
        with zipfile.ZipFile(
            self.wheel, "w", compression=zipfile.ZIP_DEFLATED
        ) as archive:
            archive.writestr(EXTENSION, b"extension bytes")
            for runtime in runtimes:
                archive.writestr(runtime, b"runtime bytes: " + runtime.encode())
            archive.writestr(
                f"{DIST_INFO}/METADATA",
                (
                    "Metadata-Version: 2.4\nName: opendal\nVersion: 0.47.8rc1\n"
                    "License-Expression: Apache-2.0\nLicense-File: LICENSE\n\n"
                    "Original project description.\n"
                ),
            )
            if sbom:
                archive.writestr(
                    SBOM,
                    json.dumps(
                        {
                            "bomFormat": "CycloneDX",
                            "specVersion": "1.5",
                            "version": 1,
                            "metadata": {
                                "component": {"bom-ref": "root", "name": "opendal"}
                            },
                            "components": [
                                {"type": "library", "bom-ref": "rust", "name": "tokio"}
                            ],
                            "dependencies": [{"ref": "root", "dependsOn": ["rust"]}],
                        }
                    ),
                )
            archive.writestr(f"{DIST_INFO}/RECORD", "")

    def test_identifies_runtime_preserves_payload_and_repairs_record(self):
        self.write_wheel()
        self.assertTrue(complete_wheel(self.wheel))
        with zipfile.ZipFile(self.wheel) as archive:
            self.assertEqual(archive.read(EXTENSION), b"extension bytes")
            runtime = archive.read(RUNTIME)
            self.assertEqual(runtime, b"runtime bytes: " + RUNTIME.encode())
            sbom = json.loads(archive.read(SBOM))
            self.assertEqual(sbom["components"][0]["name"], "tokio")
            gcc = next(c for c in sbom["components"] if c["name"] == "libgcc_s")
            self.assertEqual(gcc["licenses"], [{"expression": LICENSE_EXPRESSION}])
            self.assertEqual(
                gcc["hashes"],
                [{"alg": "SHA-256", "content": hashlib.sha256(runtime).hexdigest()}],
            )
            self.assertEqual(gcc["properties"][0]["value"], RUNTIME)
            self.assertEqual(
                sbom["dependencies"][0]["dependsOn"], ["rust", gcc["bom-ref"]]
            )
            metadata = archive.read(f"{DIST_INFO}/METADATA").decode()
            self.assertIn("Original project description.", metadata)
            self.assertIn(RUNTIME_NOTICE, metadata)
            self.assertIn(
                f"License-Expression: (Apache-2.0) AND ({LICENSE_EXPRESSION})", metadata
            )
            for name in (*LICENSE_FILES, "LIBGCC-NOTICE"):
                self.assertIn(f"License-File: {name}\n", metadata)
                self.assertTrue(archive.read(f"{DIST_INFO}/licenses/{name}"))
            records = list(
                csv.reader(io.StringIO(archive.read(f"{DIST_INFO}/RECORD").decode()))
            )
            self.assertEqual({row[0] for row in records}, set(archive.namelist()))
            for name, digest, size in records:
                if name.endswith("/RECORD"):
                    self.assertEqual((digest, size), ("", ""))
                else:
                    content = archive.read(name)
                    expected = (
                        base64.urlsafe_b64encode(hashlib.sha256(content).digest())
                        .rstrip(b"=")
                        .decode()
                    )
                    self.assertEqual(digest, "sha256=" + expected)
                    self.assertEqual(int(size), len(content))
        first = self.wheel.read_bytes()
        self.assertTrue(complete_wheel(self.wheel))
        self.assertEqual(self.wheel.read_bytes(), first)

    def test_wheel_without_bundled_libraries_is_unchanged(self):
        self.write_wheel(runtimes=())
        before = self.wheel.read_bytes()
        self.assertFalse(complete_wheel(self.wheel))
        self.assertEqual(self.wheel.read_bytes(), before)

    def test_every_bundled_runtime_is_identified(self):
        self.write_wheel(runtimes=(RUNTIME, "opendal.libs/libgcc_s-abcdef.so.1"))
        complete_wheel(self.wheel)
        with zipfile.ZipFile(self.wheel) as archive:
            sbom = json.loads(archive.read(SBOM))
            self.assertEqual(
                sum(c["name"] == "libgcc_s" for c in sbom["components"]), 2
            )
            self.assertEqual(len(sbom["dependencies"][0]["dependsOn"]), 3)

    def test_missing_sbom_does_not_rewrite_wheel(self):
        self.write_wheel(sbom=False)
        before = self.wheel.read_bytes()
        with self.assertRaises(KeyError):
            complete_wheel(self.wheel)
        self.assertEqual(self.wheel.read_bytes(), before)

    def test_unknown_library_does_not_receive_gcc_license(self):
        self.write_wheel(runtimes=("opendal.libs/libother.so.1",))
        before = self.wheel.read_bytes()
        with self.assertRaisesRegex(ValueError, "unrecognized bundled libraries"):
            complete_wheel(self.wheel)
        self.assertEqual(self.wheel.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
