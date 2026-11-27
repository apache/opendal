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
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import check_docsrs


class CheckDocsRsTests(unittest.TestCase):
    def test_load_docsrs_metadata_from_core_manifests(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        for rel in ("core/Cargo.toml", "core/services/rocksdb/Cargo.toml"):
            meta = check_docsrs.load_docsrs_metadata(repo_root / rel)
            self.assertTrue(meta.all_features)
            self.assertIn("--config", meta.cargo_args)
            self.assertIn('env.ROCKSDB_LIB_DIR="/usr/lib"', meta.cargo_args)

    def test_filter_metadata_cargo_args_strips_scrape_examples(self) -> None:
        args = (
            "-Zrustdoc-scrape-examples",
            "-Z",
            "rustdoc-scrape-examples",
            "--config",
            'env.ROCKSDB_LIB_DIR="/usr/lib"',
            "-Zbuild-std",
        )
        self.assertEqual(
            check_docsrs.filter_metadata_cargo_args(args),
            ["--config", 'env.ROCKSDB_LIB_DIR="/usr/lib"', "-Zbuild-std"],
        )

    def test_build_cargo_rustdoc_command_matches_docsrs_order(self) -> None:
        meta = check_docsrs.DocsRsMetadata(
            package_name="opendal",
            all_features=True,
            rustc_args=("--cfg", "custom_rustc"),
            rustdoc_args=("--cfg", "custom_doc"),
            cargo_args=("-Zrustdoc-scrape-examples", "--config", 'env.ROCKSDB_LIB_DIR="/usr/lib"'),
        )

        html_cmd = check_docsrs.build_cargo_rustdoc_command(
            cargo_bin="/opt/rustwide/cargo-home/bin/cargo",
            toolchain="nightly",
            manifest_path="/opt/rustwide/workdir/core/Cargo.toml",
            metadata=meta,
            target="x86_64-unknown-linux-gnu",
            jobs=12,
            mode="html",
        )
        self.assertIn("--all-features", html_cmd)
        self.assertIn("-j12", html_cmd)
        self.assertIn("-Zrustdoc-scrape-examples", html_cmd)
        self.assertEqual(
            html_cmd[-2:],
            ["--config", 'env.ROCKSDB_LIB_DIR="/usr/lib"'],
        )
        rustdoc_cfg = next(a for a in html_cmd if a.startswith("build.rustdocflags="))
        rustdoc_flags = json.loads(rustdoc_cfg.split("=", 1)[1])
        self.assertEqual(rustdoc_flags[:4], ["--cfg", "docsrs", "--cfg", "custom_doc"])
        self.assertIn("--emit=html-non-static-files", rustdoc_flags)

        json_cmd = check_docsrs.build_cargo_rustdoc_command(
            cargo_bin="/opt/rustwide/cargo-home/bin/cargo",
            toolchain="nightly",
            manifest_path="/opt/rustwide/workdir/core/Cargo.toml",
            metadata=meta,
            target="x86_64-unknown-linux-gnu",
            jobs=12,
            mode="json",
        )
        self.assertNotIn("-Zrustdoc-scrape-examples", json_cmd)
        json_cfg = next(a for a in json_cmd if a.startswith("build.rustdocflags="))
        json_flags = json.loads(json_cfg.split("=", 1)[1])
        self.assertIn("--output-format", json_flags)
        self.assertIn("json", json_flags)

    def test_load_docsrs_metadata_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manifest = Path(tmp) / "Cargo.toml"
            manifest.write_text('[package]\nname = "demo"\nversion = "0.1.0"\n')
            meta = check_docsrs.load_docsrs_metadata(manifest)
            self.assertEqual(meta.package_name, "demo")
            self.assertFalse(meta.all_features)
            self.assertEqual(meta.cargo_args, ())


if __name__ == "__main__":
    unittest.main()
