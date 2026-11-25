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

"""Unit tests for the nightly CI status reporter."""

import json
import unittest
from io import BytesIO
from unittest import mock
from urllib.error import HTTPError, URLError

import report


class FakeGitHub:
    def __init__(
        self,
        *,
        jobs_by_run: dict[int, list[dict]],
        last_green_sha: str | None = "green0000",
        prev_run_id: int | None = None,
        open_issues: list[dict] | None = None,
    ):
        self.repository = "apache/opendal"
        self.jobs_by_run = jobs_by_run
        self.last_green_sha = last_green_sha
        self.prev_run_id = prev_run_id
        self.open_issues = open_issues or []
        self.ensured_labels: list[list[str]] = []
        self.created: list[dict] = []
        self.updated: list[tuple[int, str]] = []
        self.closed: list[tuple[int, str]] = []

    def list_run_jobs(self, run_id: int) -> list[dict]:
        return self.jobs_by_run.get(run_id, [])

    def last_green_schedule_sha(self, workflow_file: str, current_run_id: int):
        return self.last_green_sha

    def previous_completed_schedule_run_id(
        self, workflow_file: str, current_run_id: int
    ):
        return self.prev_run_id

    def list_open_issues(self, label: str) -> list[dict]:
        return list(self.open_issues)

    def ensure_labels(self, labels: list[str]) -> None:
        self.ensured_labels.append(labels)

    def create_issue(self, title: str, body: str, labels: list[str]) -> dict:
        number = 100 + len(self.created) + 1
        self.created.append(
            {"number": number, "title": title, "body": body, "labels": labels}
        )
        return {"number": number}

    def update_issue_body(self, number: int, body: str) -> None:
        self.updated.append((number, body))

    def close_issue(self, number: int, comment: str) -> None:
        self.closed.append((number, comment))


class GitHubTests(unittest.TestCase):
    def setUp(self):
        self.github = report.GitHub("apache/opendal", "unused-token")
        self.jobs_url = "https://api.github.com/repos/apache/opendal/actions/runs/10/jobs"
        self.urlopen = self.enterContext(mock.patch("report.urllib.request.urlopen"))
        self.sleep = self.enterContext(mock.patch("report.time.sleep"))

    @staticmethod
    def job_page(total_count, ids):
        jobs = [{"id": job_id} for job_id in ids]
        return BytesIO(json.dumps({"total_count": total_count, "jobs": jobs}).encode())

    def test_run_jobs_stops_at_reported_total(self):
        for total in (0, 50, 384, 3100):
            with self.subTest(total=total):
                offsets = range(0, max(1, total), 25)
                self.urlopen.reset_mock()
                self.urlopen.side_effect = [
                    self.job_page(total, range(offset, min(offset + 25, total)))
                    for offset in offsets
                ]

                self.assertEqual(
                    self.github.list_run_jobs(10),
                    [{"id": job_id} for job_id in range(total)],
                )
                self.assertEqual(
                    [args.args[0].full_url for args in self.urlopen.call_args_list],
                    [
                        f"{self.jobs_url}?per_page=25&page={page}"
                        for page in range(1, len(offsets) + 1)
                    ],
                )

    def test_run_jobs_retries_timeouts_on_the_same_page(self):
        stalled_response = BytesIO()
        stalled_response.read = mock.Mock(side_effect=TimeoutError("read timed out"))
        self.urlopen.side_effect = [
            self.job_page(26, range(25)),
            URLError(TimeoutError("connect timed out")),
            stalled_response,
            self.job_page(26, [25]),
        ]

        self.assertEqual(
            self.github.list_run_jobs(10), [{"id": job_id} for job_id in range(26)]
        )
        self.assertEqual(
            [args.args[0].full_url for args in self.urlopen.call_args_list],
            [f"{self.jobs_url}?per_page=25&page={page}" for page in (1, 2, 2, 2)],
        )
        self.assertEqual(self.sleep.call_args_list, [mock.call(1), mock.call(2)])

    def test_run_jobs_shares_retry_budget_for_http_errors_and_timeouts(self):
        last_error = TimeoutError("last attempt timed out")
        page_url = f"{self.jobs_url}?per_page=25&page=2"
        self.urlopen.side_effect = [
            self.job_page(26, range(25)),
            HTTPError(page_url, 502, "Bad Gateway", {}, None),
            TimeoutError("read timed out"),
            last_error,
        ]

        with self.assertRaises(TimeoutError) as raised:
            self.github.list_run_jobs(10)
        self.assertIs(raised.exception, last_error)
        self.assertEqual(self.urlopen.call_count, 4)
        self.assertEqual(self.sleep.call_args_list, [mock.call(1), mock.call(2)])

    def test_run_jobs_rejects_inconsistent_pages(self):
        first_ids = range(25)
        for total, ids in ((27, [25]), (27, [24, 25]), (27, [25, 25]), (28, [25, 26])):
            with self.subTest(total=total, ids=ids):
                self.urlopen.side_effect = [
                    self.job_page(27, first_ids), self.job_page(total, ids)
                ]
                with self.assertRaisesRegex(RuntimeError, "incomplete job listing .* at page 2"):
                    self.github.list_run_jobs(10)

    def test_run_jobs_rejects_totals_over_the_limit(self):
        self.urlopen.return_value = self.job_page(3101, [])

        with self.assertRaisesRegex(RuntimeError, "pagination limit exceeded .*3101 jobs"):
            self.github.list_run_jobs(10)
        self.urlopen.assert_called_once()

    def test_other_transport_errors_are_not_retried(self):
        timeout = TimeoutError("read timed out")
        wrapped_timeout = URLError(TimeoutError("connect timed out"))
        refused = URLError(ConnectionRefusedError("connection refused"))
        for operation, args, error in (
            (self.github.create_issue, ("title", "body", ["ci/nightly"]), timeout),
            (self.github.close_issue, (42, "comment"), wrapped_timeout),
            (self.github.list_open_issues, ("ci/nightly",), timeout),
            (self.github.ensure_labels, (["ci/nightly"],), timeout),
            (self.github.list_run_jobs, (10,), refused),
        ):
            with self.subTest(operation=operation.__name__):
                self.urlopen.reset_mock()
                self.urlopen.side_effect = error
                with self.assertRaises(type(error)) as raised:
                    operation(*args)
                self.assertIs(raised.exception, error)
                self.urlopen.assert_called_once()
        self.sleep.assert_not_called()

    def test_create_issue_posts_the_original_payload(self):
        self.urlopen.return_value = BytesIO(b'{"number": 42}')

        self.assertEqual(
            self.github.create_issue("title", "body", ["ci/nightly"]), {"number": 42}
        )
        self.urlopen.assert_called_once()
        request = self.urlopen.call_args.args[0]
        self.assertEqual(request.full_url, "https://api.github.com/repos/apache/opendal/issues")
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(
            json.loads(request.data), {"title": "title", "body": "body", "labels": ["ci/nightly"]}
        )


class NightlyReportTests(unittest.TestCase):
    def test_tier_a_opens_on_first_failure_and_ignores_self_job(self):
        gh = FakeGitHub(
            jobs_by_run={
                10: [
                    {
                        "name": "Report Nightly Status",
                        "status": "in_progress",
                        "conclusion": None,
                        "labels": ["ubuntu-latest"],
                        "html_url": "https://example.invalid/self",
                        "steps": [],
                    },
                    {
                        "name": "core / check_clippy",
                        "status": "completed",
                        "conclusion": "failure",
                        "labels": ["ubuntu-latest"],
                        "html_url": "https://example.invalid/clippy",
                        "steps": [
                            {"name": "Cargo clippy", "conclusion": "failure"},
                        ],
                    },
                ]
            }
        )
        summary = report.reconcile(
            gh,
            tier="tier-a",
            workflow_file="nightly_tier_a.yml",
            run_id=10,
            head_sha="deadbeef1",
            self_job_name="Report Nightly Status",
            apply=True,
        )
        self.assertEqual(len(gh.created), 1)
        self.assertEqual(gh.created[0]["title"], "[Nightly] core / check_clippy")
        self.assertEqual(gh.created[0]["labels"], ["ci/nightly"])
        self.assertIn("tier-a::core / check_clippy", gh.created[0]["body"])
        self.assertIn("green0000...deadbeef1", gh.created[0]["body"])
        self.assertEqual(summary["counts"], {"passed": 0, "failed": 1, "unknown": 0})

    def test_extracts_top_level_test_and_retry_steps(self):
        jobs = [
            {
                "name": "behavior / core / ubuntu-latest / s3 / aws_s3",
                "status": "completed",
                "conclusion": "success",
                "labels": ["ubuntu-latest"],
                "html_url": "https://example.invalid/s3",
                "steps": [
                    {"name": "Test Core", "conclusion": "failure"},
                    {"name": "Retry Test Core", "conclusion": "success"},
                ],
            },
            {
                "name": "behavior / core / ubuntu-latest / s3 / aws_s3_sse",
                "status": "completed",
                "conclusion": "failure",
                "labels": ["ubuntu-latest"],
                "html_url": "https://example.invalid/s3-sse",
                "steps": [
                    {"name": "Test Core", "conclusion": "failure"},
                    {"name": "Retry Test Core", "conclusion": "failure"},
                ],
            },
        ]
        outcomes = report.extract_outcomes(jobs, "tier-b", "Report Nightly Status")
        by_id = {o.item_id: o for o in outcomes}
        self.assertEqual(
            by_id["behavior / core / ubuntu-latest / s3 / aws_s3"].state, "passed"
        )
        self.assertEqual(
            by_id["behavior / core / ubuntu-latest / s3 / aws_s3"].first_attempt,
            "failure",
        )
        self.assertEqual(
            by_id["behavior / core / ubuntu-latest / s3 / aws_s3"].retry_attempt,
            "success",
        )
        self.assertEqual(
            by_id["behavior / core / ubuntu-latest / s3 / aws_s3_sse"].state, "failed"
        )

    def test_tier_b_requires_two_consecutive_failures(self):
        job_fail = [
            {
                "name": "behavior / core / ubuntu-latest / s3 / aws_s3",
                "status": "completed",
                "conclusion": "failure",
                "labels": ["ubuntu-latest"],
                "html_url": "https://example.invalid/s3",
                "steps": [
                    {"name": "Test Core", "conclusion": "failure"},
                    {"name": "Retry Test Core", "conclusion": "failure"},
                ],
            }
        ]
        job_pass = [
            {
                "name": "behavior / core / ubuntu-latest / s3 / aws_s3",
                "status": "completed",
                "conclusion": "success",
                "labels": ["ubuntu-latest"],
                "html_url": "https://example.invalid/s3",
                "steps": [
                    {"name": "Test Core", "conclusion": "success"},
                ],
            }
        ]

        # First night failure -> suppressed.
        gh_first = FakeGitHub(
            jobs_by_run={1: job_pass, 2: job_fail},
            prev_run_id=1,
        )
        summary_first = report.reconcile(
            gh_first,
            tier="tier-b",
            workflow_file="nightly_tier_b.yml",
            run_id=2,
            head_sha="sha2",
            self_job_name="Report Nightly Status",
            apply=True,
        )
        self.assertEqual(gh_first.created, [])
        self.assertEqual(len(summary_first["actions"]["suppressed_first_night"]), 1)

        # Second consecutive night failure -> opens issue with tier-b labels.
        gh_second = FakeGitHub(
            jobs_by_run={2: job_fail, 3: job_fail},
            prev_run_id=2,
        )
        report.reconcile(
            gh_second,
            tier="tier-b",
            workflow_file="nightly_tier_b.yml",
            run_id=3,
            head_sha="sha3",
            self_job_name="Report Nightly Status",
            apply=True,
        )
        self.assertEqual(len(gh_second.created), 1)
        self.assertEqual(
            gh_second.created[0]["labels"], ["ci/nightly", "ci/live-service"]
        )
        self.assertEqual(
            gh_second.created[0]["title"],
            "[Nightly Live] behavior / core / ubuntu-latest / s3 / aws_s3",
        )

    def test_only_explicit_pass_closes_open_issue_not_cancelled_or_missing(self):
        open_issues = [
            {
                "number": 42,
                "body": "<!-- opendal-nightly-key: tier-a::core / unit -->",
            },
            {
                "number": 43,
                "body": "<!-- opendal-nightly-key: tier-a::core / doc-test -->",
            },
            {
                "number": 44,
                "body": "<!-- opendal-nightly-key: tier-a::core / check_msrv -->",
            },
        ]
        gh = FakeGitHub(
            jobs_by_run={
                20: [
                    {
                        "name": "core / unit",
                        "status": "completed",
                        "conclusion": "success",
                        "labels": ["ubuntu-latest"],
                        "html_url": "https://example.invalid/unit",
                        "steps": [],
                    },
                    {
                        "name": "core / doc-test",
                        "status": "completed",
                        "conclusion": "cancelled",
                        "labels": ["ubuntu-latest"],
                        "html_url": "https://example.invalid/doctest",
                        "steps": [],
                    },
                    # core / check_msrv omitted (not run)
                ]
            },
            open_issues=open_issues,
        )
        summary = report.reconcile(
            gh,
            tier="tier-a",
            workflow_file="nightly_tier_a.yml",
            run_id=20,
            head_sha="pass1234",
            self_job_name="Report Nightly Status",
            apply=True,
        )
        self.assertEqual([n for n, _ in gh.closed], [42])
        self.assertEqual(
            {item["number"] for item in summary["actions"]["kept_open_unknown"]},
            {43, 44},
        )

    def test_broad_failure_rolls_up_into_single_issue(self):
        jobs = [
            {
                "name": f"suite / job_{i}",
                "status": "completed",
                "conclusion": "failure",
                "labels": ["ubuntu-latest"],
                "html_url": f"https://example.invalid/job_{i}",
                "steps": [],
            }
            for i in range(12)
        ]
        gh = FakeGitHub(jobs_by_run={30: jobs})
        report.reconcile(
            gh,
            tier="tier-a",
            workflow_file="nightly_tier_a.yml",
            run_id=30,
            head_sha="broken123",
            self_job_name="Report Nightly Status",
            apply=True,
        )
        self.assertEqual(len(gh.created), 1)
        self.assertIn("Broad failure (12 items)", gh.created[0]["title"])
        self.assertIn("tier-a::__broad_failure__", gh.created[0]["body"])

    def test_dry_run_makes_no_writes(self):
        gh = FakeGitHub(
            jobs_by_run={
                40: [
                    {
                        "name": "core / unit",
                        "status": "completed",
                        "conclusion": "failure",
                        "labels": ["ubuntu-latest"],
                        "html_url": "https://example.invalid/unit",
                        "steps": [],
                    }
                ]
            }
        )
        summary = report.reconcile(
            gh,
            tier="tier-a",
            workflow_file="nightly_tier_a.yml",
            run_id=40,
            head_sha="sha40",
            self_job_name="Report Nightly Status",
            apply=False,
        )
        self.assertEqual(gh.created, [])
        self.assertEqual(gh.updated, [])
        self.assertEqual(gh.closed, [])
        self.assertEqual(len(summary["actions"]["opened"]), 1)


if __name__ == "__main__":
    unittest.main()
