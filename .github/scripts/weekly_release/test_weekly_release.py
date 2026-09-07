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

"""Test fresh attempts, platform facts and the real command entry point."""

import copy
import hashlib
import io
import json
import os
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import Mock, patch

import artifacts
import atr
import controller as c
import model
import prepare

SHA = "a" * 40
RC = "0.59.1-rc.12001"


def release(phase="release_candidate_draft", rc=RC):
    return {
        "version": rc,
        "phase": phase,
        "latest_revision_number": "00001",
        "vote_mode": "trusted",
        "vote_started": "2026-09-04T01:00:00Z",
    }


class EntryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.cwd = Path.cwd()
        os.chdir(self.root)
        self.addCleanup(os.chdir, self.cwd)
        self.env = {
            "GITHUB_REPOSITORY": c.REPO,
            "GITHUB_RUN_ID": "12",
            "GITHUB_RUN_ATTEMPT": "1",
            "GITHUB_OUTPUT": str(self.root / "outputs"),
            "GITHUB_EVENT_PATH": str(self.root / "event.json"),
        }
        self.remote = Mock(spec=atr.ATR)
        self.remote.release.return_value = None
        self.remote.request.return_value = {"releases": [], "count": 0}
        self.remote.checks.return_value = []

    def invoke(self, *args):
        with (
            patch.dict(os.environ, self.env),
            patch("sys.argv", ["controller.py", *args]),
            patch.object(c, "ATR", return_value=self.remote),
        ):
            c.main()

    def test_new_attempt_after_lost_pr_response_uses_new_rc(self):
        created = []

        def create(record, cfg):
            created.append(copy.deepcopy(record))
            if len(created) == 1:
                raise TimeoutError("PR created; response lost")
            return {"html_url": "new-pr"}

        with (
            patch.object(c, "baseline", return_value=("v0.59.0", {"core": "0.59.0"})),
            patch.object(c, "cutoff_commit", return_value=SHA),
            patch.object(c, "git", return_value='make_package("core", "0.59.0")'),
            patch.object(c, "ref", return_value=None),
            patch.object(c, "candidate", side_effect=create),
        ):
            with self.assertRaises(TimeoutError):
                self.invoke("prepare")
            self.env["GITHUB_RUN_ATTEMPT"] = "2"
            self.invoke("prepare")
        self.assertNotEqual(created[0]["rc"], created[1]["rc"])
        self.assertNotEqual(created[0]["branch"], created[1]["branch"])
        self.assertEqual(created[0]["versions"], created[1]["versions"])
        self.assertEqual(created[0]["baseline_tag"], "v0.59.0")
        self.assertFalse((self.root / "state.json").exists())

    def test_open_vote_or_publication_prevents_new_candidate(self):
        for phase in ("release_candidate", "release_preview"):
            self.remote.request.return_value = {
                "releases": [release(phase)],
                "count": 1,
            }
            with patch.object(c, "baseline") as baseline, self.assertRaises(ValueError):
                self.invoke("prepare")
            baseline.assert_not_called()

    def test_drafts_can_be_abandoned(self):
        c.require_idle([release()])

    def test_handoff_is_manual_and_rehearsal_disables_publication(self):
        self.remote.release.return_value = release()
        self.remote.checks.return_value = [{"status": "concern", "checker": "rat"}]
        self.invoke("handoff", "--rc", RC, "--sha", SHA, "--rehearsal")
        request = json.loads(Path("vote-request.json").read_text())
        self.assertFalse(request["automatic_publish_when_resolved"])
        self.assertTrue(request["automatic_resolve_when_finished"])
        self.assertEqual(request["vote_duration"], 72)
        self.assertEqual(request["concerns_noted"], ["rat"])
        self.assertEqual(request["revision"], "00001")
        self.assertFalse(hasattr(self.remote, "post"))

    def test_merged_candidate_rejects_changed_branch_or_existing_upload(self):
        event = {
            "pull_request": {
                "merged": True,
                "base": {"ref": "release-candidates/" + RC},
                "head": {
                    "ref": "release-candidates/" + RC + "-bump",
                    "repo": {"full_name": c.REPO},
                },
                "merge_commit_sha": SHA,
            }
        }
        Path(self.env["GITHUB_EVENT_PATH"]).write_text(json.dumps(event))
        with (
            patch.object(c, "ref", return_value="b" * 40),
            self.assertRaises(ValueError),
        ):
            self.invoke("merged")
        self.remote.release.return_value = release()
        with patch.object(c, "ref", return_value=SHA), self.assertRaises(ValueError):
            self.invoke("merged")
        self.remote.release.return_value = None
        with (
            patch.object(c, "ref", return_value=SHA),
            patch.object(c, "git", return_value='make_package("core", "0.59.1")'),
            patch.object(c, "ensure_ref") as tag,
        ):
            self.invoke("merged")
        tag.assert_called_once_with(c.REPO, "tags/v" + RC, SHA)
        self.assertIn("candidate=" + SHA, Path(self.env["GITHUB_OUTPUT"]).read_text())

    def test_followup_uses_atr_phase_without_journal(self):
        self.remote.request.return_value = {
            "releases": [release("release_preview")],
            "count": 1,
        }

        def ref(repo, name):
            return None if name == "tags/v0.59.1" else SHA

        with (
            patch.object(c, "ref", side_effect=ref),
            patch.object(c, "git", return_value='make_package("core", "0.59.1")'),
            patch.object(
                c,
                "api",
                return_value={
                    "workflow_runs": [
                        {"id": 12, "run_attempt": 1, "conclusion": "success"}
                    ]
                },
            ),
            patch.object(c, "signed_checksums", return_value={"file": "digest"}),
            patch.object(c, "published_files") as verify,
            patch.object(c, "ensure_ref") as tag,
            patch.object(c, "sync_main") as sync,
        ):
            self.invoke("follow-up")
            verify.assert_called_once()
            tag.assert_not_called()
            self.assertIn("announce=true", Path(self.env["GITHUB_OUTPUT"]).read_text())
            self.remote.request.return_value = {
                "releases": [release("release")],
                "count": 1,
            }
            Path(self.env["GITHUB_OUTPUT"]).write_text("")
            self.invoke("follow-up")
            tag.assert_called_once_with(c.REPO, "tags/v0.59.1", SHA)
            sync.assert_called_once()
            self.assertNotIn(
                "announce=true", Path(self.env["GITHUB_OUTPUT"]).read_text()
            )

    def test_followup_never_advances_on_wrong_published_bytes(self):
        self.remote.request.return_value = {
            "releases": [release("release_preview")],
            "count": 1,
        }
        with (
            patch.object(
                c,
                "ref",
                side_effect=lambda repo, name: None if name == "tags/v0.59.1" else SHA,
            ),
            patch.object(c, "git", return_value='make_package("core", "0.59.1")'),
            patch.object(
                c,
                "api",
                return_value={
                    "workflow_runs": [
                        {"id": 12, "run_attempt": 1, "conclusion": "success"}
                    ]
                },
            ),
            patch.object(c, "signed_checksums", return_value={"file": "digest"}),
            patch.object(c, "published_files", side_effect=ValueError("wrong bytes")),
            patch.object(c, "ensure_ref") as tag,
            self.assertRaises(ValueError),
        ):
            self.invoke("follow-up")
        tag.assert_not_called()
        self.assertFalse(Path(self.env["GITHUB_OUTPUT"]).exists())


class ContractTests(unittest.TestCase):
    def test_cutoff_ignores_later_push(self):
        when = model.timestamp("2026-09-07T10:00:00+08:00")
        self.assertEqual(model.cutoff(when).isoformat(), "2026-09-04T00:00:00+00:00")
        rows = [
            {"created_at": "2026-09-04T00:01:00Z", "id": 2, "head_sha": "late"},
            {"created_at": "2026-09-03T23:59:00Z", "id": 1, "head_sha": SHA},
        ]
        with patch.object(c, "api", return_value={"workflow_runs": rows}):
            self.assertEqual(c.cutoff_commit(model.cutoff(when)), SHA)

    def test_versions_preserve_reviewed_bumps_and_reject_inventory_drift(self):
        self.assertEqual(
            model.plan_versions({"core": "0.59.0"}, {"core": "0.60.0"}),
            {"core": "0.60.0"},
        )
        self.assertEqual(
            model.plan_versions({"core": "0.59.0"}, {"core": "0.59.0"}),
            {"core": "0.59.1"},
        )
        with self.assertRaises(ValueError):
            model.plan_versions({"core": "0.59.0"}, {"core": "0.59.0", "new": "0.1.0"})

    def test_baseline_requires_actual_publication(self):
        with (
            patch.object(
                c,
                "git",
                side_effect=["v0.59.0\nv0.60.0", 'make_package("core", "0.59.0")'],
            ),
            patch.object(c, "download", return_value=b'<a href="0.59.0/">'),
        ):
            self.assertEqual(c.baseline([]), ("v0.59.0", {"core": "0.59.0"}))

    def test_atr_publication_precedes_final_tag_and_sync_pr(self):
        with (
            patch.object(
                c, "git", side_effect=["v0.59.0", 'make_package("core", "0.59.1")']
            ),
            patch.object(c, "ref", return_value=SHA),
            patch.object(c, "download", return_value=b'<a href="0.59.1/">'),
        ):
            self.assertEqual(c.baseline([release("release")])[0], "v" + RC)

    def test_signed_inventory_and_published_bytes(self):
        record = {
            "version": "0.59.1",
            "versions": {"core": "0.59.1"},
            "compose_run": 12,
            "compose_attempt": 1,
        }
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as archive:
            for path in artifacts.expected_paths(record):
                archive.writestr(path.split("/", 1)[1], b"source")
        with (
            patch.object(
                artifacts,
                "api",
                return_value={
                    "artifacts": [
                        {"id": 1, "name": "signed-source-12-1", "expired": False}
                    ]
                },
            ),
            patch.object(artifacts, "run", return_value=buf.getvalue()),
        ):
            record["checksums"] = artifacts.signed_checksums(record)
        self.assertEqual(
            set(record["checksums"].values()), {hashlib.sha512(b"source").hexdigest()}
        )
        with patch.object(artifacts, "download", return_value=b"source"):
            artifacts.published_files(record)
        with (
            patch.object(artifacts, "download", return_value=b"changed"),
            self.assertRaises(ValueError),
        ):
            artifacts.published_files(record)

    def test_build_subprocess_does_not_receive_credentials(self):
        with patch.dict(
            os.environ,
            {"GH_TOKEN": "secret", "ATR_PAT": "secret", "GIT_CONFIG_VALUE_0": "secret"},
        ):
            env = prepare.build_env()
        self.assertNotIn("GH_TOKEN", env)
        self.assertNotIn("ATR_PAT", env)
        self.assertNotIn("GIT_CONFIG_VALUE_0", env)


if __name__ == "__main__":
    unittest.main()
