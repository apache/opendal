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

import hashlib
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import compose_atr as compose
import release_lifecycle as lifecycle


class ComposeTests(unittest.TestCase):
    def test_second_upload_replaces_first_revision_including_sidecars(self):
        first = lifecycle.Candidate("0.60.0-rc.1", "a" * 40)
        second = lifecycle.Candidate("0.60.0-rc.2", "b" * 40)
        core = "apache-opendal-core-0.60.0-src.tar.gz"
        old = "apache-opendal-bindings-c-1.0.0-src.tar.gz"
        new = "apache-opendal-bindings-c-1.0.1-src.tar.gz"
        removed = "apache-opendal-bindings-test-0.1.0-src.tar.gz"
        real_run = subprocess.run
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            remote = root / "atr"
            remote.mkdir()
            registered = []
            numbers = iter(["00004", "00009"])
            revisions = {}
            release = {}

            def register(url, payload):
                self.assertEqual(url, f"{lifecycle.ATR}/api/publisher/ssh/register")
                registered.append(payload["source_commit"])

            def transfer(argv, **options):
                if argv[0] == "ssh-keygen":
                    return real_run(argv, **options)
                self.assertEqual(
                    argv[-1], "github@releases.apache.org:/opendal/0.60.0/"
                )
                real_run(
                    [*argv[:-1], str(remote) + "/"], capture_output=True, **options
                )
                number = next(numbers)
                revisions[number] = {
                    path.name: "sha3-256:"
                    + hashlib.sha3_256(path.read_bytes()).hexdigest()
                    for path in remote.iterdir()
                }
                release.update(
                    phase="release_candidate_draft",
                    latest_revision_number=number,
                    commit_hash=registered[-1],
                )

            def snapshot(version, *, optional=False):
                if version == first.version:
                    return release or None
                return None

            with (
                patch.object(
                    compose,
                    "ref_sha",
                    side_effect=lambda kind, name: {
                        "v" + first.rc: first.sha,
                        "v" + second.rc: second.sha,
                    }.get(name),
                ),
                patch.object(compose, "candidates", return_value=[first.rc, second.rc]),
                patch.object(compose, "atr_release", side_effect=snapshot),
                patch.object(
                    compose,
                    "atr_manifest",
                    side_effect=lambda version, number: revisions[number],
                ),
                patch.object(compose, "atr_jwt", return_value="token"),
                patch.object(compose, "request_json", side_effect=register),
                patch.object(compose.subprocess, "run", side_effect=transfer),
                patch.object(lifecycle, "pages", return_value=[]),
                patch.object(compose, "api") as status,
            ):
                for candidate, names, number in [
                    (first, (core, old, removed), "00004"),
                    (second, (core, new), "00009"),
                ]:
                    bundle = root / candidate.rc
                    bundle.mkdir()
                    for name in names:
                        for suffix in ("", ".asc", ".sha512"):
                            path = bundle / (name + suffix)
                            path.write_text(name + suffix + candidate.sha)
                            os.utime(
                                path,
                                (1700001000, 1700001000)
                                if candidate == first
                                else (1700002000, 1700002000),
                            )
                    compose.compose(candidate, bundle)
                    self.assertEqual(registered[-1], candidate.sha)
                    self.assertEqual(
                        set(revisions[number]), {p.name for p in bundle.iterdir()}
                    )
                    self.assertEqual(
                        {p.name: p.read_bytes() for p in remote.iterdir()},
                        {p.name: p.read_bytes() for p in bundle.iterdir()},
                    )
                    status.assert_called_with(
                        f"repos/{lifecycle.REPO}/statuses/{candidate.sha}",
                        candidate.revision_status(number, revisions[number]),
                    )
                    self.assertEqual(
                        status.call_args.args[1]["target_url"],
                        f"{lifecycle.ATR}/api/release/manifest/opendal/0.60.0/{number}",
                    )

    def test_first_shared_release_waits_for_existing_long_version_vote(self):
        candidate = lifecycle.Candidate("0.60.0-rc.2", "b" * 40)
        with (
            patch.object(compose, "ref_sha", side_effect=[candidate.sha, None]),
            patch.object(
                compose, "signed_manifest", return_value={"archive": "digest"}
            ),
            patch.object(
                compose,
                "atr_release",
                side_effect=[None, None, {"phase": "release_candidate"}],
            ),
            patch.object(
                compose, "candidates", return_value=["0.60.0-rc.1", candidate.rc]
            ),
            patch.object(compose, "upload") as upload,
        ):
            with self.assertRaisesRegex(ValueError, "existing ATR release 0.60.0-rc.1"):
                compose.compose(candidate, Path("bundle"))
            upload.assert_not_called()

    def test_retry_records_already_accepted_revision_without_another_upload(self):
        candidate = lifecycle.Candidate("0.60.0-rc.2", "b" * 40)
        files = {"archive.asc": "digest"}
        accepted = {
            "phase": "release_candidate_draft",
            "commit_hash": candidate.sha,
            "latest_revision_number": "00007",
        }
        with (
            patch.object(compose, "ref_sha", return_value=candidate.sha),
            patch.object(compose, "signed_manifest", return_value=files),
            patch.object(compose, "atr_release", side_effect=[None, accepted]),
            patch.object(compose, "atr_manifest", return_value=files),
            patch.object(lifecycle, "pages", return_value=[]),
            patch.object(compose, "upload") as upload,
            patch.object(compose, "api") as status,
        ):
            compose.compose(candidate, Path("bundle"))
            upload.assert_not_called()
            status.assert_called_once_with(
                f"repos/{lifecycle.REPO}/statuses/{candidate.sha}",
                candidate.revision_status("00007", files),
            )

    def test_unverified_upload_never_gets_a_commit_status(self):
        candidate = lifecycle.Candidate("0.60.0-rc.2", "b" * 40)
        files = {"package.asc": "new"}
        old = {
            "phase": "release_candidate_draft",
            "latest_revision_number": "00004",
            "commit_hash": "a" * 40,
        }
        accepted = old | {
            "latest_revision_number": "00009",
            "commit_hash": candidate.sha,
        }
        with (
            patch.object(compose, "ref_sha", return_value=candidate.sha),
            patch.object(compose, "signed_manifest", return_value=files),
            patch.object(
                compose, "atr_release", side_effect=[None, old, old, accepted]
            ),
            patch.object(compose, "upload"),
            patch.object(compose.time, "sleep") as wait,
            patch.object(
                compose,
                "atr_manifest",
                return_value=files | {"withdrawn-package.asc": "old"},
            ),
            patch.object(compose, "api") as status,
        ):
            with self.assertRaisesRegex(ValueError, "withdrawn-package.asc"):
                compose.compose(candidate, Path("bundle"))
            wait.assert_called_once()
            status.assert_not_called()


if __name__ == "__main__":
    unittest.main()
