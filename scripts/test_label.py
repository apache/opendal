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

"""Label taxonomy, model boundary and event application contracts."""

import copy
import io
import unittest
import urllib.error
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import label

REPOSITORY_ID = 101
HEAD_REPOSITORY_ID = 202
HEAD_BRANCH = "topic/feature+fix"
HEAD_SHA = "f" * 40


def workflow_run_event():
    return {
        "workflow_run": {
            "conclusion": "success",
            "event": "pull_request",
            "path": ".github/workflows/label_pr_signal.yml",
            "repository": {"id": REPOSITORY_ID},
            "head_repository": {"id": HEAD_REPOSITORY_ID, "owner": {"login": "fork"}},
            "head_branch": HEAD_BRANCH,
            "head_sha": HEAD_SHA,
            "pull_requests": [],
        }
    }


def pull_request(number=1, base_ref="main"):
    return {
        "number": number,
        "state": "open",
        "base": {"repo": {"id": REPOSITORY_ID}, "ref": base_ref},
        "head": {
            "repo": {"id": HEAD_REPOSITORY_ID},
            "ref": HEAD_BRANCH,
            "sha": HEAD_SHA,
        },
    }


class ResolveTests(unittest.TestCase):
    def test_fork_lookup_paginates_and_deduplicates(self):
        matching = pull_request(42)
        earlier_page = [{**pull_request(), "state": "closed"}] * 100
        with (
            patch.dict("os.environ", {"GH_TOKEN": "test"}),
            patch.object(
                label, "request", side_effect=[earlier_page, [matching, matching]]
            ) as request,
        ):
            github = label.GitHub("apache/opendal")
            self.assertEqual(
                label.resolve_pr(github, workflow_run_event(), REPOSITORY_ID), 42
            )
        urls = [urlsplit(call.args[0]) for call in request.call_args_list]
        self.assertEqual(
            [url.path for url in urls], ["/repos/apache/opendal/pulls"] * 2
        )
        self.assertEqual(
            [parse_qs(url.query) for url in urls],
            [
                {
                    "state": ["open"],
                    "head": ["fork:topic/feature+fix"],
                    "per_page": ["100"],
                    "page": [page],
                }
                for page in ("1", "2")
            ],
        )

    def test_attached_pr_is_validated_using_its_live_details(self):
        event = workflow_run_event()
        event["workflow_run"]["pull_requests"] = [{"number": 42}]
        github = unittest.mock.Mock()
        github.call.return_value = pull_request(42)
        self.assertEqual(label.resolve_pr(github, event, REPOSITORY_ID), 42)
        github.call.assert_called_once_with("/pulls/42")
        github.pages.assert_not_called()

    def test_closed_or_mismatched_prs_are_skipped(self):
        for changed in (
            {"state": "closed"},
            {"base": {"repo": {"id": 999}}},
            {"head": {**pull_request()["head"], "repo": {"id": 999}}},
            {"head": {**pull_request()["head"], "repo": None}},
            {"head": {**pull_request()["head"], "ref": "different"}},
            {"head": {**pull_request()["head"], "sha": "a" * 40}},
        ):
            for attached in ([], [{"number": 1}]):
                with (
                    self.subTest(changed=changed, attached=attached),
                    patch("builtins.print"),
                ):
                    event = workflow_run_event()
                    event["workflow_run"]["pull_requests"] = attached
                    github = unittest.mock.Mock()
                    github.call.return_value = {**pull_request(), **changed}
                    github.pages.return_value = [github.call.return_value]
                    self.assertIsNone(label.resolve_pr(github, event, REPOSITORY_ID))

    def test_ambiguous_fork_lookup_between_base_branches_is_skipped(self):
        github = unittest.mock.Mock()
        github.pages.return_value = [
            pull_request(1),
            pull_request(2, base_ref="release"),
        ]
        with patch("builtins.print") as log:
            self.assertIsNone(
                label.resolve_pr(github, workflow_run_event(), REPOSITORY_ID)
            )
        self.assertIn("Multiple open PRs", log.call_args.args[0])

    def test_rejects_other_workflow_paths_repositories_events_and_conclusions(self):
        for field, value in (
            ("path", ".github/workflows/another_signal.yml"),
            ("repository", {"id": 999}),
            ("event", "push"),
            ("conclusion", "failure"),
        ):
            with self.subTest(field=field):
                event = workflow_run_event()
                event["workflow_run"][field] = value
                github = unittest.mock.Mock()
                self.assertIsNone(label.resolve_pr(github, event, REPOSITORY_ID))
                self.assertEqual(github.mock_calls, [])

    def test_missing_head_repository_is_skipped(self):
        for present in (False, True):
            with self.subTest(present=present):
                event = workflow_run_event()
                event["workflow_run"]["pull_requests"] = [{"number": 1}]
                del event["workflow_run"]["head_repository"]
                if present:
                    event["workflow_run"]["head_repository"] = None
                github = unittest.mock.Mock()
                self.assertIsNone(label.resolve_pr(github, event, REPOSITORY_ID))
                self.assertEqual(github.mock_calls, [])

    def test_resolve_cli_sets_output_only_for_a_positive_pr_number(self):
        import json
        import tempfile
        from pathlib import Path

        for number, expected in (
            (42, "pr_number=42\n"),
            (0, ""),
            ("42", ""),
            (True, ""),
        ):
            with (
                self.subTest(number=number),
                tempfile.TemporaryDirectory() as directory,
            ):
                event = workflow_run_event()
                event["workflow_run"]["pull_requests"] = [{"number": number}]
                event_path = Path(directory) / "event.json"
                event_path.write_text(json.dumps(event))
                output_path = Path(directory) / "output"
                output_path.touch()
                with (
                    patch("sys.argv", ["label.py", "resolve"]),
                    patch.dict(
                        "os.environ",
                        {
                            "GH_TOKEN": "test",
                            "GITHUB_REPOSITORY": "apache/opendal",
                            "GITHUB_REPOSITORY_ID": str(REPOSITORY_ID),
                            "GITHUB_EVENT_PATH": str(event_path),
                            "GITHUB_OUTPUT": str(output_path),
                        },
                    ),
                    patch.object(label, "request", return_value=pull_request(42)),
                    patch("builtins.print"),
                ):
                    label.main()
                self.assertEqual(output_path.read_text(), expected)


class LabelTests(unittest.TestCase):
    def item(self, pr=True):
        result = {
            "number": 1,
            "state": "open",
            "title": "fix: repair writes",
            "body": "A bug fix.",
            "labels": [],
        }
        if pr:
            result["pull_request"] = {}
        return result

    def test_catalog_covers_services_bindings_and_integrations(self):
        catalog = label.catalog()
        for name in (
            "services/goosefs",
            "services/huggingface",
            "services/memory",
            "services/cloudflare_kv",
            "bindings/zig",
            "bindings/moonbit",
            "integrations/parquet",
            "ci/nightly",
            "ci/live-service",
            "ci/deploy-docs",
        ):
            self.assertIn(name, catalog)
        self.assertNotIn("services/hf", catalog)
        self.assertNotIn("services/azure_common", catalog)

    def test_schema_separates_issue_and_pr_categories(self):
        issue = label.model_request(self.item(False), [])
        pr = label.model_request(self.item(), [])
        issue_schema = issue["response_format"]["json_schema"]["schema"]
        pr_format = pr["response_format"]
        pr_schema = pr_format["json_schema"]["schema"]
        self.assertNotIn("fix", issue_schema["properties"]["category"]["enum"])
        self.assertNotIn("bug", pr_schema["properties"]["category"]["enum"])
        self.assertEqual(pr_format["type"], "json_schema")
        self.assertEqual(pr_format["json_schema"]["name"], "label_classification")
        self.assertEqual(pr["max_completion_tokens"], 512)
        self.assertIs(pr["chat_template_kwargs"]["enable_thinking"], False)

    def test_invalid_model_shapes_never_reach_github(self):
        for result in (
            {
                "category": "fix",
                "components": "services/goosefs",
                "breaking_change": False,
            },
            {
                "category": "fix",
                "components": [1],
                "breaking_change": False,
            },
            {"category": "fix", "components": [], "breaking_change": "false"},
            {
                "category": "fix",
                "components": [],
                "breaking_change": False,
                "extra": "lgtm",
            },
        ):
            item = self.item()
            github = FakeGitHub(item)
            with self.subTest(result=result), self.assertRaises(ValueError):
                label.label_pr(
                    github,
                    workflow_run_event(),
                    REPOSITORY_ID,
                    1,
                    lambda _, result=result: result,
                    apply=True,
                )
            self.assertEqual(github.writes, [])

    def test_component_proposals_only_apply_trusted_catalog_labels(self):
        item = self.item()
        github = FakeGitHub(item)
        with patch("builtins.print"):
            label.label_pr(
                github,
                workflow_run_event(),
                REPOSITORY_ID,
                1,
                lambda _: {
                    "category": "fix",
                    "components": [
                        "services/goosefs",
                        "services/invented",
                        "run-with-secrets",
                    ],
                    "breaking_change": False,
                },
                apply=True,
            )
        self.assertEqual(
            github.writes,
            [
                (
                    "/issues/1/labels",
                    "POST",
                    {"labels": ["releases-note/fix", "services/goosefs", "size:XS"]},
                ),
            ],
        )

    def test_issue_cannot_receive_pr_category_or_breaking_flag(self):
        with self.assertRaises(ValueError):
            label.selected_labels(
                {"category": "fix", "components": []}, self.item(False), []
            )
        with self.assertRaises(ValueError):
            label.selected_labels(
                {"category": "bug", "components": [], "breaking_change": True},
                self.item(False),
                [],
            )

    def test_preserves_category_and_deduplicates_components(self):
        item = self.item()
        item["labels"] = [{"name": "releases-note/feat"}, {"name": "lgtm"}]
        labels = label.selected_labels(
            {
                "category": "fix",
                "components": ["services/goosefs"] * 2,
                "breaking_change": False,
            },
            item,
            [],
        )
        self.assertEqual(labels, {"services/goosefs", "size:XS"})

    def test_size_boundaries_exclude_lockfiles(self):
        for lines, size in [
            (9, "XS"),
            (10, "S"),
            (30, "M"),
            (100, "L"),
            (500, "XL"),
            (1000, "XXL"),
        ]:
            files = [
                {"filename": "core/src/lib.rs", "additions": lines, "deletions": 0},
                {"filename": "core/Cargo.lock", "additions": 10000, "deletions": 0},
            ]
            labels = label.selected_labels(
                {"category": "fix", "components": [], "breaking_change": False},
                self.item(),
                files,
            )
            self.assertIn("size:" + size, labels)

    def test_pr_applies_only_current_item_and_replaces_size(self):
        item = self.item()
        item["labels"] = [
            {"name": "lgtm"},
            {"name": "releases-note/docs"},
            {"name": "size:XL"},
        ]
        github = FakeGitHub(item)
        with patch("builtins.print"):
            label.label_pr(
                github,
                workflow_run_event(),
                REPOSITORY_ID,
                1,
                lambda _: {
                    "category": "fix",
                    "components": ["services/goosefs"],
                    "breaking_change": False,
                },
                apply=True,
            )
        self.assertEqual(
            github.writes,
            [
                (
                    "/issues/1/labels",
                    "POST",
                    {"labels": ["services/goosefs", "size:XS"]},
                ),
                ("/issues/1/labels/size%3AXL", "DELETE", None),
            ],
        )

    def test_dry_run_makes_no_writes(self):
        item = self.item(False)
        github = FakeGitHub(item)
        with patch("builtins.print"):
            label.label_event(
                github,
                {"issue": item},
                lambda _: {"category": "bug", "components": ["core"]},
            )
        self.assertEqual(github.writes, [])

    def test_open_issue_is_classified(self):
        item = self.item(False)
        github = FakeGitHub(item)
        with patch("builtins.print"):
            label.label_event(
                github,
                {"issue": item},
                lambda _: {"category": "bug", "components": ["core"]},
                apply=True,
            )
        self.assertEqual(
            github.writes,
            [("/issues/1/labels", "POST", {"labels": ["bug", "core"]})],
        )

    def test_pr_uses_live_title_body_and_files_after_resolution(self):
        import json

        github = FakeGitHub(self.item())
        github.files = [
            {"filename": "website/index.md", "additions": 10, "deletions": 0}
        ]
        event = workflow_run_event()
        event["workflow_run"]["pull_requests"] = [{"number": 1}]
        number = label.resolve_pr(github, event, REPOSITORY_ID)
        self.assertEqual(number, 1)
        github.item.update(title="docs: document writes", body="An edited description.")

        def infer(request):
            data = json.loads(request["messages"][1]["content"])
            self.assertEqual(data["title"], "docs: document writes")
            self.assertEqual(data["body"], "An edited description.")
            self.assertEqual(data["files"], ["website/index.md"])
            return {
                "category": "docs",
                "components": ["website"],
                "breaking_change": False,
            }

        with patch("builtins.print"):
            label.label_pr(github, event, REPOSITORY_ID, number, infer, apply=True)
        self.assertEqual(
            github.writes,
            [
                (
                    "/issues/1/labels",
                    "POST",
                    {"labels": ["releases-note/docs", "size:S", "website"]},
                )
            ],
        )

    def test_pr_skips_a_head_change_between_resolution_and_classification(self):
        github = FakeGitHub(self.item())
        github.pull["head"]["sha"] = "b" * 40
        with patch("builtins.print"):
            label.label_pr(
                github,
                workflow_run_event(),
                REPOSITORY_ID,
                1,
                lambda _: self.fail("unexpected inference"),
                apply=True,
            )
        self.assertEqual(github.reads, ["/issues/1", "/pulls/1"])
        self.assertEqual(github.writes, [])

    def test_stale_head_title_or_body_cannot_apply_results(self):
        for changed in ("title", "body", "head"):
            github = FakeGitHub(self.item())

            def infer(_, changed=changed, github=github):
                if changed == "head":
                    github.pull["head"]["sha"] = "b" * 40
                else:
                    github.item[changed] = "Updated purpose"
                return {"category": "fix", "components": [], "breaking_change": False}

            with self.subTest(changed=changed), patch("builtins.print"):
                label.label_pr(
                    github, workflow_run_event(), REPOSITORY_ID, 1, infer, apply=True
                )
                self.assertEqual(github.writes, [])

    def test_pr_skips_closed_items_and_ordinary_issues(self):
        for item in ({**self.item(), "state": "closed"}, self.item(False)):
            with self.subTest(item=item):
                github = FakeGitHub(item)
                label.label_pr(
                    github,
                    workflow_run_event(),
                    REPOSITORY_ID,
                    1,
                    lambda _: self.fail("unexpected inference"),
                    apply=True,
                )
                self.assertEqual(github.reads, ["/issues/1"])
                self.assertEqual(github.writes, [])

    def test_sync_only_creates_missing_labels(self):
        github = FakeGitHub(self.item())
        github.labels = [
            {"name": name} for name in label.catalog() if name != "services/goosefs"
        ]
        with patch("builtins.print"):
            label.sync_labels(github)
        self.assertEqual(len(github.writes), 1)
        self.assertEqual(github.writes[0][2]["name"], "services/goosefs")

    def test_http_error_reports_a_bounded_body_without_credentials(self):
        url = "https://api.cloudflare.com/example"
        token = "example-token-do-not-log"
        body = (
            f'{{"errors":[{{"code":5028,"message":"Model deprecated for {token}"}}]}}\n'
            "##[error]" + "x" * 600
        )
        error = urllib.error.HTTPError(
            url,
            410,
            "Gone",
            {"Authorization": "Bearer header-value"},
            io.BytesIO(body.encode()),
        )
        with (
            patch("urllib.request.urlopen", side_effect=error),
            self.assertRaisesRegex(
                RuntimeError, r"HTTP 410: .*5028.*Model deprecated"
            ) as caught,
        ):
            label.request(url, token, "POST", {})
        detail = str(caught.exception).partition(": ")[2]
        self.assertLessEqual(len(detail), 500)
        self.assertNotIn(token, detail)
        self.assertNotIn("header-value", detail)
        self.assertNotIn("\n", detail)
        self.assertNotIn("##[", detail)

    def test_cloudflare_response_is_parsed_and_truncation_rejected(self):
        response = {
            "success": True,
            "result": {
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {"content": '{"category":"bug","components":[]}'},
                    }
                ]
            },
        }
        with (
            patch.dict(
                "os.environ",
                {"CLOUDFLARE_ACCOUNT_ID": "a" * 32, "CLOUDFLARE_API_TOKEN": "test"},
            ),
            patch.object(label, "request", return_value=response) as request,
            patch("builtins.print"),
        ):
            self.assertEqual(label.classify({}), {"category": "bug", "components": []})
            request.assert_called_once_with(
                f"https://api.cloudflare.com/client/v4/accounts/{'a' * 32}"
                "/ai/run/@cf/qwen/qwen3.8-27b",
                "test",
                "POST",
                {},
            )
            choice = response["result"]["choices"][0]
            choice["finish_reason"] = "length"
            with self.assertRaisesRegex(ValueError, "Incomplete model output"):
                label.classify({})
            choice["finish_reason"] = "stop"
            choice["message"]["content"] = None
            with self.assertRaisesRegex(ValueError, "no classification content"):
                label.classify({})


class FakeGitHub:
    def __init__(self, item):
        self.item = copy.deepcopy(item)
        self.pull = pull_request()
        self.files = []
        self.reads = []
        self.writes = []
        self.labels = []

    def call(self, path, method="GET", body=None):
        if method != "GET":
            self.writes.append((path, method, body))
            return None
        self.reads.append(path)
        if path.startswith("/pulls/"):
            return copy.deepcopy(self.pull)
        return copy.deepcopy(self.item)

    def pages(self, path):
        self.reads.append(path)
        if path == "/labels":
            return self.labels
        if path == "/pulls/1/files":
            return self.files
        raise AssertionError(f"Unexpected paginated path: {path}")


if __name__ == "__main__":
    unittest.main()
