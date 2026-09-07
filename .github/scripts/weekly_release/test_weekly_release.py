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

"""Exercise release lifecycle, retry boundaries and immutable artifact identities."""

import hashlib
import io
import os
import subprocess
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import Mock, patch

import atr
import controller as c
import model
import prepare

CFG = {
    "repository": "apache/opendal",
    "validation_workflows": ["ci_odev.yml", "ci_check.yml"],
}
SHA = "a" * 40


def record(phase="staged", dry=False):
    return {
        "id": "2026-W37",
        "phase": phase,
        "dry_run": dry,
        "candidate_sha": SHA,
        "cutoff_sha": "b" * 40,
        "branch": "release-candidates/2026-W37",
        "version": "0.59.2",
        "versions": {"core": "0.59.2"},
        "rc": "v0.59.2-rc.1",
        "atr_version": "0.59.2-rc.1",
        "revision": "00001",
        "dispatches": {},
        "compose_run": 123,
        "compose_attempt": 1,
    }


def release(phase="release_candidate", resolved=None):
    return {
        "phase": phase,
        "vote_started": "2026-09-04T01:00:00Z",
        "vote_resolved": resolved,
        "latest_revision_number": "00001",
        "vote_mode": "trusted",
    }


class ModelTests(unittest.TestCase):
    def test_friday_cutoff_is_utc_and_stable_for_delayed_schedule(self):
        now = model.timestamp("2026-09-07T10:00:00+08:00")
        self.assertEqual(model.cutoff(now).isoformat(), "2026-09-04T00:00:00+00:00")
        self.assertEqual(model.cycle_id(model.cutoff(now)), "2026-W36")

    def test_failed_candidate_does_not_consume_stable_versions(self):
        base = {"core": "0.59.0"}
        patch_change = [{"summary": "Fix", "packages": {"core": "patch"}}]
        self.assertEqual(
            model.plan_versions(base, base, patch_change), {"core": "0.59.1"}
        )
        self.assertEqual(
            model.next_rc("0.59.1", ["v0.59.1-rc.1", "v0.59.1-rc.999"]),
            "v0.59.1-rc.1000",
        )
        breaking = [{"summary": "New contract", "packages": {"core": "breaking"}}]
        self.assertEqual(model.plan_versions(base, base, breaking), {"core": "0.60.0"})

    def test_package_baselines_and_compatibility_propagation(self):
        base = {
            "core": "0.59.0",
            "integrations/object_store": "0.60.0",
            "bindings/python": "0.47.0",
        }
        plan = model.plan_versions(
            base, base, [{"summary": "Breaking", "packages": {"core": "breaking"}}]
        )
        self.assertEqual(
            plan,
            {
                "core": "0.60.0",
                "integrations/object_store": "0.61.0",
                "bindings/python": "0.47.1",
            },
        )
        with self.assertRaises(ValueError):
            model.plan_versions(
                base,
                {"core": "0.59.0"},
                [{"summary": "Change", "packages": {"core": "patch"}}],
            )

    def test_cutoff_uses_run_timestamp_not_current_main(self):
        runs = [
            {"created_at": "2026-09-04T00:01:00Z", "id": 2, "head_sha": "new"},
            {"created_at": "2026-09-03T23:59:00Z", "id": 1, "head_sha": "cutoff"},
        ]
        with patch.object(c, "api", return_value={"workflow_runs": runs}):
            self.assertEqual(
                c.cutoff_commit(CFG, model.timestamp("2026-09-04T00:00:00Z")), "cutoff"
            )
        with (
            patch.object(c, "api", return_value={"workflow_runs": []}),
            self.assertRaises(ValueError),
        ):
            c.cutoff_commit(CFG, model.timestamp("2026-09-04T00:00:00Z"))


class WorkflowTests(unittest.TestCase):
    def test_dispatch_persisted_before_network_and_not_duplicated(self):
        r = record("composing")
        events = []

        def api(path, payload=None):
            if payload is None:
                return {"workflow_runs": []}
            events.append("dispatch")
            raise TimeoutError("response lost")

        with (
            patch.object(c, "api", side_effect=api),
            patch.object(c, "ref", return_value=SHA),
        ):
            with self.assertRaises(TimeoutError):
                c.workflow(
                    r, CFG, "release-compose.yml", {}, lambda: events.append("save")
                )
            with self.assertRaises(ValueError):
                c.workflow(
                    r, CFG, "release-compose.yml", {}, lambda: events.append("save")
                )
        self.assertEqual(events, ["save", "dispatch"])

    def test_dispatch_adopts_only_frozen_sha_branch_and_completed_success(self):
        r = record()
        r["dispatches"]["test.yml"] = "2026-09-04T00:00:00Z"
        run = {
            "id": 1,
            "head_sha": SHA,
            "head_branch": r["branch"],
            "created_at": "2026-09-04T00:00:00Z",
            "run_attempt": 1,
            "status": "completed",
            "conclusion": "success",
            "html_url": "run",
        }
        with patch.object(c, "api", return_value={"workflow_runs": [run]}):
            self.assertEqual(c.workflow(r, CFG, "test.yml", {}, Mock()), run)
            run["head_sha"] = "c" * 40
            with self.assertRaises(ValueError):
                c.workflow(r, CFG, "test.yml", {}, Mock())

    def test_compose_uses_same_run_instead_of_bot_dispatch(self):
        r = record("composing")
        r.pop("compose_run")
        with (
            patch.dict(os.environ, {"GITHUB_RUN_ID": "321"}),
            patch.object(c, "ref", return_value=SHA),
            patch.object(c, "output") as output,
            patch.object(c, "api") as api,
        ):
            c.reconcile(r, {}, CFG, Mock(), Mock())
            self.assertEqual(r["compose_run"], 321)
            output.assert_called_once_with(
                compose="true", candidate=SHA, rc=r["atr_version"]
            )
            api.assert_not_called()

    def test_compose_adopts_completed_reusable_workflow(self):
        r = record("composing")
        with (
            patch.dict(os.environ, {"GITHUB_RUN_ID": "321"}),
            patch.object(
                c,
                "api",
                return_value={
                    "status": "completed",
                    "conclusion": "success",
                    "run_attempt": 2,
                },
            ),
        ):
            c.reconcile(r, {}, CFG, Mock(), Mock())
        self.assertEqual(r["phase"], "staged")
        self.assertEqual(r["compose_attempt"], 2)

    def test_candidate_build_environment_has_no_credentials(self):
        with patch.dict(
            os.environ,
            {
                "GH_TOKEN": "secret",
                "ATR_PAT": "secret",
                "GITHUB_TOKEN": "secret",
                "GIT_CONFIG_VALUE_0": "secret",
                "PATH": "/bin",
            },
        ):
            env = prepare.build_env()
        self.assertNotIn("GH_TOKEN", env)
        self.assertNotIn("ATR_PAT", env)
        self.assertNotIn("GIT_CONFIG_VALUE_0", env)
        self.assertEqual(env["PATH"], "/bin")


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.atr = Mock(spec=atr.ATR)
        self.persist = Mock()
        self.state = {"baseline": {"tag": "v0.59.0"}}

    def test_failed_tag_creation_is_retried_before_validation(self):
        r = record("checking")
        with (
            patch.object(c, "ensure_ref", side_effect=[TimeoutError(), None]) as tag,
            patch.object(c, "workflow", return_value=None) as workflow,
        ):
            with self.assertRaises(TimeoutError):
                c.reconcile(r, self.state, CFG, self.atr, self.persist)
            self.assertEqual(r["phase"], "checking")
            workflow.assert_not_called()
            c.reconcile(r, self.state, CFG, self.atr, self.persist)
            self.assertEqual(tag.call_count, 2)
            workflow.assert_called_once()

    def test_announcement_uses_oidc_action_and_waits_for_release(self):
        r = record("announcing")
        self.atr.release.return_value = release("release_preview")
        with patch.object(c, "output") as output:
            c.reconcile(r, self.state, CFG, self.atr, self.persist)
            self.assertEqual(r["phase"], "announcing")
            self.persist.assert_called_once()
            self.assertTrue(output.call_args.kwargs["announce"])
        self.atr.release.return_value = release("release")
        c.reconcile(r, self.state, CFG, self.atr, self.persist)
        self.assertEqual(r["phase"], "syncing")

    def test_rehearsal_opens_trusted_vote_without_publication(self):
        r = record(dry=True)
        payload = atr.vote_payload(r, "00001", ["rat", "rat"])
        self.assertTrue(payload["automatic_resolve_when_finished"])
        self.assertFalse(payload["automatic_publish_when_resolved"])
        self.assertEqual(payload["vote_duration"], 72)
        self.assertEqual(payload["concerns_noted"], ["rat"])
        self.assertIn("REHEARSAL", payload["subject"])
        self.assertTrue(
            atr.vote_payload(record(), "00001", [])["automatic_publish_when_resolved"]
        )

    def test_staged_prepares_handoff_without_starting_vote(self):
        r = record()
        self.atr.release.return_value = release("release_candidate_draft")
        self.atr.request.side_effect = [
            {"ongoing": 0},
            {"policy_vote_mode": "trusted"},
            {"rel_paths": list(c.expected_paths(r))},
        ]
        self.atr.checks.return_value = [
            {"status": "concern", "checker": "rat"},
            {"status": "suggestion", "checker": "sbom"},
        ]

        with patch.object(c, "signed_checksums", return_value={}):
            c.reconcile(r, self.state, CFG, self.atr, self.persist)
        self.assertEqual(r["phase"], "awaiting-vote")
        self.assertEqual(r["vote_request"]["concerns_noted"], ["rat"])

    def test_blocker_does_not_start_vote(self):
        r = record()
        self.atr.release.return_value = release("release_candidate_draft")
        self.atr.request.return_value = {"ongoing": 0}
        self.atr.checks.return_value = [{"status": "blocker", "checker": "archive"}]
        with self.assertRaises(ValueError):
            c.reconcile(r, self.state, CFG, self.atr, self.persist)

    def test_manual_vote_is_observed_without_credentials_or_post(self):
        r = record("awaiting-vote")
        self.atr.release.return_value = release("release_candidate_draft")
        c.reconcile(r, self.state, CFG, self.atr, self.persist)
        self.assertEqual(r["phase"], "awaiting-vote")
        self.atr.release.return_value = release()
        c.reconcile(r, self.state, CFG, self.atr, self.persist)
        self.assertEqual(r["phase"], "voting")

    def test_manual_vote_cannot_change_revision_or_vote_mode(self):
        for key, value in [
            ("latest_revision_number", "00002"),
            ("vote_mode", "manual"),
        ]:
            r = record("awaiting-vote")
            remote = release()
            remote[key] = value
            self.atr.release.return_value = remote
            with self.assertRaises(ValueError):
                c.reconcile(r, self.state, CFG, self.atr, self.persist)
            self.assertEqual(r["phase"], "awaiting-vote")

    def test_vote_timeout_waits_for_rm_and_preserves_baseline(self):
        r = record("voting")
        r.update(discussion="url", verified_vote_files=True)
        self.atr.release.return_value = release()
        with patch.object(
            c, "now", return_value=model.timestamp("2026-09-08T00:00:00Z")
        ):
            c.reconcile(r, self.state, CFG, self.atr, self.persist)
        self.assertEqual(r["phase"], "voting")
        self.assertIn("RM", r["attention"])
        self.assertEqual(self.state["baseline"], {"tag": "v0.59.0"})

    def test_cancelled_vote_and_rehearsal_never_advance_baseline(self):
        for dry, phase, expected in [
            (False, "release_candidate_draft", "cancelled"),
            (True, "release_preview", "rehearsed"),
        ]:
            r = record("voting", dry=dry)
            r["discussion"] = "url"
            self.atr.release.return_value = release(
                phase, resolved="2026-09-07T01:00:00Z"
            )
            c.reconcile(r, self.state, CFG, self.atr, self.persist)
            self.assertEqual(r["phase"], expected)
            self.assertEqual(self.state["baseline"], {"tag": "v0.59.0"})

    def test_passing_vote_is_not_yet_a_published_release(self):
        r = record("voting")
        r.update(discussion="url", verified_vote_files=True)
        self.atr.release.return_value = release(
            "release_preview", resolved="2026-09-07T01:00:00Z"
        )
        c.reconcile(r, self.state, CFG, self.atr, self.persist)
        self.assertEqual(r["phase"], "publishing")
        self.assertEqual(self.state["baseline"], {"tag": "v0.59.0"})
        with (
            patch.object(
                c, "published_files", side_effect=ValueError("not propagated")
            ),
            self.assertRaises(ValueError),
        ):
            c.reconcile(r, self.state, CFG, self.atr, self.persist)
        self.assertEqual(self.state["baseline"], {"tag": "v0.59.0"})
        with patch.object(c, "published_files"):
            c.reconcile(r, self.state, CFG, self.atr, self.persist)
        self.assertEqual(self.state["baseline"]["sha"], SHA)
        self.assertEqual(r["phase"], "announcing")

    def test_uncertain_announcement_is_not_repeated(self):
        r = record("announcing")
        self.atr.release.return_value = release("release_preview")
        with (
            patch.object(c, "output", side_effect=TimeoutError()),
            self.assertRaises(TimeoutError),
        ):
            c.reconcile(r, self.state, CFG, self.atr, self.persist)
        with patch.object(c, "output") as output:
            with self.assertRaises(ValueError):
                c.reconcile(r, self.state, CFG, self.atr, self.persist)
            output.assert_not_called()
        self.atr.release.return_value = release("release")
        c.reconcile(r, self.state, CFG, self.atr, self.persist)
        self.assertEqual(r["phase"], "syncing")


class ArtifactTests(unittest.TestCase):
    def test_immutable_actions_zip_inventory(self):
        r = record()
        files = {p.split("/", 1)[1]: b"content" for p in c.expected_paths(r)}
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as z:
            for name, data in files.items():
                z.writestr(name, data)
        response = {
            "artifacts": [{"id": 1, "name": "signed-source-123-1", "expired": False}]
        }
        with (
            patch.object(c, "api", return_value=response),
            patch.object(c, "run", return_value=buf.getvalue()),
        ):
            checksums = c.signed_checksums(r, CFG)
        self.assertEqual(set(checksums), c.expected_paths(r))
        self.assertEqual(
            set(checksums.values()), {hashlib.sha512(b"content").hexdigest()}
        )
        r["versions"]["bindings/python"] = "0.47.1"
        with (
            patch.object(c, "api", return_value=response),
            patch.object(c, "run", return_value=buf.getvalue()),
            self.assertRaises(ValueError),
        ):
            c.signed_checksums(r, CFG)

    def test_changed_atr_bytes_require_rm_cancellation(self):
        r = record("voting")
        r["checksums"] = {
            p: hashlib.sha512(b"original").hexdigest() for p in c.expected_paths(r)
        }
        remote = Mock(spec=atr.ATR)
        remote.request.return_value = {"rel_paths": list(c.expected_paths(r))}
        remote.release.return_value = release()
        with (
            patch.object(c, "download", return_value=b"modified"),
            self.assertRaises(ValueError),
        ):
            c.capture_files(r, remote)

    def test_publication_compares_every_signed_file(self):
        r = record("publishing")
        r["checksums"] = {
            p: hashlib.sha512(b"content").hexdigest() for p in c.expected_paths(r)
        }
        with patch.object(c, "download", return_value=b"content") as download:
            c.published_files(r)
            self.assertEqual(download.call_count, 3)
        with (
            patch.object(c, "download", return_value=b"different"),
            self.assertRaises(ValueError),
        ):
            c.published_files(r)


class PlanningGitTests(unittest.TestCase):
    def test_sync_merge_alone_does_not_create_a_release(self):
        with tempfile.TemporaryDirectory() as tmp:

            def git(*args):
                return (
                    subprocess.check_output(
                        ["git", "-C", tmp, *args], stderr=subprocess.DEVNULL
                    )
                    .decode()
                    .strip()
                )

            git("init", "-b", "main")
            git("config", "user.name", "Test")
            git("config", "user.email", "test@example.org")
            git("config", "commit.gpgsign", "false")
            p = Path(tmp) / "core"
            p.mkdir()
            (p / "Cargo.toml").write_text('version = "0.59.0"')
            git("add", ".")
            git("commit", "-m", "Baseline")
            base = git("rev-parse", "HEAD")
            (p / "Cargo.toml").write_text('version = "0.59.1"')
            git("commit", "-am", "Sync released version")
            sync = git("rev-parse", "HEAD")
            state = {
                "baseline": {"sha": base},
                "candidates": {"old": {"sync_number": 1}},
            }
            with (
                patch.object(c, "git", side_effect=git),
                patch.object(c, "cutoff_commit", return_value=sync),
                patch.object(
                    c, "api", return_value={"merged": True, "merge_commit_sha": sync}
                ),
            ):
                c.plan(state, CFG)
            self.assertEqual(
                [r["phase"] for k, r in state["candidates"].items() if k != "old"],
                ["skipped"],
            )


if __name__ == "__main__":
    unittest.main()
