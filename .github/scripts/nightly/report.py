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

"""Reconcile nightly GitHub Actions outcomes with deduplicated GitHub Issues."""

import argparse
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

KEY_RE = re.compile(r"<!--\s*opendal-nightly-key:\s*(.+?)\s*-->")

FAILURE_CONCLUSIONS = {"failure", "timed_out", "startup_failure"}
BROAD_FAILURE_THRESHOLD = 10
MAX_RUN_JOBS = 3100

TIER_LABELS = {
    "tier-a": ["ci/nightly"],
    "tier-b": ["ci/nightly", "ci/live-service"],
}

LABEL_METADATA = {
    "ci/nightly": {
        "description": "Automated nightly CI regression or health report.",
        "color": "b60205",
    },
    "ci/live-service": {
        "description": "Live external cloud service CI failure.",
        "color": "fbca04",
    },
}


@dataclass(frozen=True)
class ItemOutcome:
    item_id: str
    dedup_key: str
    state: str  # "passed" | "failed" | "unknown"
    os_label: str
    job_url: str
    first_attempt: str
    retry_attempt: str | None
    failed_step: str | None


def request(
    url: str,
    token: str,
    method: str = "GET",
    body: dict[str, Any] | None = None,
    *,
    retry_timeouts: bool = False,
) -> Any:
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "User-Agent": "opendal-nightly-report",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    data = None if body is None else json.dumps(body).encode()
    for attempt in range(3):
        try:
            with urllib.request.urlopen(
                urllib.request.Request(url, data, headers, method=method), timeout=60
            ) as response:
                payload = response.read()
                return json.loads(payload) if payload else None
        except urllib.error.HTTPError as error:
            if error.code not in (429, 500, 502, 503, 504) or attempt == 2:
                detail = error.read().decode("utf-8", errors="replace")[:500]
                raise RuntimeError(
                    f"{method} {url} failed with HTTP {error.code}: {detail}"
                ) from None
        except (TimeoutError, urllib.error.URLError) as error:
            reason = error.reason if isinstance(error, urllib.error.URLError) else error
            if (
                not retry_timeouts
                or method != "GET"
                or not isinstance(reason, TimeoutError)
                or attempt == 2
            ):
                raise
        time.sleep(2**attempt)


class GitHub:
    def __init__(self, repository: str, token: str):
        self.repository = repository
        self.base = f"https://api.github.com/repos/{repository}"
        self.token = token

    def call(
        self, path: str, method: str = "GET", body: dict[str, Any] | None = None
    ) -> Any:
        return request(self.base + path, self.token, method, body)

    def paginate_key(self, path: str, key: str) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        sep = "&" if "?" in path else "?"
        for page in range(1, 32):
            payload = self.call(f"{path}{sep}per_page=100&page={page}")
            batch = payload.get(key, []) if isinstance(payload, dict) else payload
            items.extend(batch)
            if len(batch) < 100:
                return items
        raise RuntimeError(f"pagination limit exceeded for {path}")

    def list_run_jobs(self, run_id: int) -> list[dict[str, Any]]:
        path = f"/actions/runs/{run_id}/jobs"
        # GitHub can return 502 for larger pages of jobs.
        page_size = 25
        jobs: list[dict[str, Any]] = []
        job_ids: set[int] = set()
        total_count = None
        while total_count is None or len(jobs) < total_count:
            page = len(jobs) // page_size + 1
            payload = request(
                f"{self.base}{path}?per_page={page_size}&page={page}",
                self.token,
                retry_timeouts=True,
            )
            if total_count is None:
                total_count = payload["total_count"]
                if not 0 <= total_count <= MAX_RUN_JOBS:
                    raise RuntimeError(
                        f"pagination limit exceeded for {path}: {total_count} jobs"
                    )
            batch = payload["jobs"]
            batch_ids = {job["id"] for job in batch}
            if (
                payload["total_count"] != total_count
                or len(batch) != min(page_size, total_count - len(jobs))
                or len(batch_ids) != len(batch)
                or not job_ids.isdisjoint(batch_ids)
            ):
                raise RuntimeError(f"incomplete job listing for {path} at page {page}")
            jobs.extend(batch)
            job_ids.update(batch_ids)
        return jobs

    def last_green_schedule_sha(
        self, workflow_file: str, current_run_id: int
    ) -> str | None:
        encoded = urllib.parse.quote(workflow_file, safe="")
        payload = self.call(
            f"/actions/workflows/{encoded}/runs?branch=main&event=schedule&status=success&per_page=5"
        )
        for run in payload.get("workflow_runs", []):
            if run.get("id") != current_run_id and run.get("head_sha"):
                return run["head_sha"]
        return None

    def previous_completed_schedule_run_id(
        self, workflow_file: str, current_run_id: int
    ) -> int | None:
        encoded = urllib.parse.quote(workflow_file, safe="")
        payload = self.call(
            f"/actions/workflows/{encoded}/runs?branch=main&event=schedule&status=completed&per_page=5"
        )
        for run in payload.get("workflow_runs", []):
            if run.get("id") != current_run_id:
                return int(run["id"])
        return None

    def list_open_issues(self, label: str) -> list[dict[str, Any]]:
        encoded = urllib.parse.quote(label, safe="")
        issues = self.paginate_key(f"/issues?state=open&labels={encoded}", "")
        return [issue for issue in issues if "pull_request" not in issue]

    def ensure_labels(self, labels: list[str]) -> None:
        existing = {
            item["name"] for item in self.paginate_key("/labels", "")
        }
        for name in labels:
            if name not in existing and name in LABEL_METADATA:
                self.call("/labels", "POST", {"name": name, **LABEL_METADATA[name]})

    def create_issue(
        self, title: str, body: str, labels: list[str]
    ) -> dict[str, Any]:
        return self.call(
            "/issues", "POST", {"title": title, "body": body, "labels": labels}
        )

    def update_issue_body(self, number: int, body: str) -> None:
        self.call(f"/issues/{number}", "PATCH", {"body": body})

    def close_issue(self, number: int, comment: str) -> None:
        self.call(f"/issues/{number}/comments", "POST", {"body": comment})
        self.call(f"/issues/{number}", "PATCH", {"state": "closed"})


def _job_os_label(job: dict[str, Any]) -> str:
    labels = job.get("labels") or []
    if labels:
        return ", ".join(labels)
    return "unknown"


def _step_state(step: dict[str, Any] | None) -> str | None:
    if step is None:
        return None
    conclusion = step.get("conclusion")
    if conclusion in (None, "skipped"):
        return None
    return str(conclusion)


def extract_outcomes(
    jobs: list[dict[str, Any]], tier: str, self_job_name: str
) -> list[ItemOutcome]:
    outcomes: list[ItemOutcome] = []

    for job in jobs:
        job_name = str(job.get("name") or "")
        if not job_name or job_name == self_job_name:
            continue

        os_label = _job_os_label(job)
        job_url = str(job.get("html_url") or "")
        steps = job.get("steps") or []

        status = job.get("status")
        conclusion = job.get("conclusion")
        first_step = next(
            (
                s
                for s in steps
                if str(s.get("name") or "") in ("Test", "Test Core", "Test service")
            ),
            None,
        )
        retry_step = next(
            (
                s
                for s in steps
                if str(s.get("name") or "").startswith("Retry ")
            ),
            None,
        )
        first_res = _step_state(first_step) or str(conclusion or status or "unknown")
        retry_res = _step_state(retry_step)

        failed_step = next(
            (
                str(s.get("name"))
                for s in steps
                if s.get("conclusion") in FAILURE_CONCLUSIONS
            ),
            None,
        )

        if status == "completed" and conclusion == "success":
            state = "passed"
        elif conclusion in FAILURE_CONCLUSIONS:
            state = "failed"
        else:
            state = "unknown"

        outcomes.append(
            ItemOutcome(
                item_id=job_name,
                dedup_key=f"{tier}::{job_name}",
                state=state,
                os_label=os_label,
                job_url=job_url,
                first_attempt=first_res,
                retry_attempt=retry_res,
                failed_step=failed_step,
            )
        )

    return outcomes


def parse_issue_key(body: str | None) -> str | None:
    if not body:
        return None
    match = KEY_RE.search(body)
    return match.group(1).strip() if match else None


def format_issue_title(tier: str, item_id: str) -> str:
    prefix = "[Nightly]" if tier == "tier-a" else "[Nightly Live]"
    return f"{prefix} {item_id}"


def format_item_issue_body(
    tier: str,
    repository: str,
    head_sha: str,
    last_green_sha: str | None,
    run_url: str,
    outcome: ItemOutcome,
) -> str:
    compare_line = (
        f"[`{last_green_sha[:9]}...{head_sha[:9]}`]"
        f"(https://github.com/{repository}/compare/{last_green_sha}...{head_sha})"
        if last_green_sha and last_green_sha != head_sha
        else f"`{head_sha}` (no prior green scheduled run found)"
    )
    retry_line = outcome.retry_attempt if outcome.retry_attempt is not None else "n/a"
    step_line = f"`{outcome.failed_step}`" if outcome.failed_step else "n/a"
    tier_note = (
        "Deterministic nightly regression (Tier A, no external credentials)."
        if tier == "tier-a"
        else "Live external service failure observed across consecutive nightly runs (Tier B)."
    )
    return (
        f"<!-- opendal-nightly-key: {outcome.dedup_key} -->\n\n"
        f"{tier_note}\n\n"
        f"- **Item:** `{outcome.item_id}`\n"
        f"- **Head SHA:** `{head_sha}`\n"
        f"- **Suspect Range:** {compare_line}\n"
        f"- **OS / Runner:** `{outcome.os_label}`\n"
        f"- **First Attempt:** `{outcome.first_attempt}`\n"
        f"- **Retry Attempt:** `{retry_line}`\n"
        f"- **Failing Step:** {step_line}\n"
        f"- **Job Log:** {outcome.job_url}\n"
        f"- **Workflow Run:** {run_url}\n\n"
        "This issue closes automatically once the same check succeeds on `main`.\n"
    )


def format_broad_failure_body(
    tier: str,
    repository: str,
    head_sha: str,
    last_green_sha: str | None,
    run_url: str,
    failures: list[ItemOutcome],
) -> str:
    dedup_key = f"{tier}::__broad_failure__"
    compare_line = (
        f"[`{last_green_sha[:9]}...{head_sha[:9]}`]"
        f"(https://github.com/{repository}/compare/{last_green_sha}...{head_sha})"
        if last_green_sha and last_green_sha != head_sha
        else f"`{head_sha}` (no prior green scheduled run found)"
    )
    lines = [
        f"<!-- opendal-nightly-key: {dedup_key} -->",
        "",
        f"Broad nightly failure in `{tier}` ({len(failures)} new failing items exceeded per-item issue cap).",
        "",
        f"- **Head SHA:** `{head_sha}`",
        f"- **Suspect Range:** {compare_line}",
        f"- **Workflow Run:** {run_url}",
        "",
        "### Failing items",
        "",
    ]
    for item in failures:
        lines.append(
            f"- `{item.item_id}` (`{item.os_label}`, first=`{item.first_attempt}`, retry=`{item.retry_attempt or 'n/a'}`): {item.job_url}"
        )
    lines.append("")
    lines.append(
        "This issue closes automatically after a full green scheduled run on `main`."
    )
    return "\n".join(lines) + "\n"


def reconcile(
    github: GitHub,
    *,
    tier: str,
    workflow_file: str,
    run_id: int,
    head_sha: str,
    self_job_name: str,
    apply: bool,
) -> dict[str, Any]:
    if tier not in TIER_LABELS:
        raise ValueError(f"unsupported tier: {tier}")

    jobs = github.list_run_jobs(run_id)
    outcomes = extract_outcomes(jobs, tier, self_job_name)
    outcome_by_key = {item.dedup_key: item for item in outcomes}

    last_green_sha = github.last_green_schedule_sha(workflow_file, run_id)
    run_url = f"https://github.com/{github.repository}/actions/runs/{run_id}"

    prev_failed_keys: set[str] = set()
    if tier == "tier-b":
        prev_run_id = github.previous_completed_schedule_run_id(workflow_file, run_id)
        if prev_run_id is not None:
            prev_jobs = github.list_run_jobs(prev_run_id)
            prev_outcomes = extract_outcomes(prev_jobs, tier, self_job_name)
            prev_failed_keys = {
                item.dedup_key for item in prev_outcomes if item.state == "failed"
            }

    open_issues = github.list_open_issues("ci/nightly")
    open_by_key: dict[str, dict[str, Any]] = {}
    for issue in open_issues:
        key = parse_issue_key(issue.get("body"))
        if key and key.startswith(f"{tier}::"):
            open_by_key[key] = issue

    actions: dict[str, list[Any]] = {
        "opened": [],
        "updated": [],
        "closed": [],
        "suppressed_first_night": [],
        "kept_open_unknown": [],
    }

    broad_key = f"{tier}::__broad_failure__"
    all_failed = [item for item in outcomes if item.state == "failed"]
    all_passed = [item for item in outcomes if item.state == "passed"]
    all_unknown = [item for item in outcomes if item.state == "unknown"]

    # 1. Close resolved issues strictly on observed success.
    for key, issue in sorted(open_by_key.items()):
        number = int(issue["number"])
        if key == broad_key:
            if len(all_failed) == 0 and len(all_unknown) == 0 and len(all_passed) > 0:
                comment = (
                    f"Resolved by full green run on `{head_sha}` ({run_url}). Closing automatically."
                )
                if apply:
                    github.close_issue(number, comment)
                actions["closed"].append({"number": number, "key": key})
            else:
                actions["kept_open_unknown"].append({"number": number, "key": key})
            continue

        current = outcome_by_key.get(key)
        if current is not None and current.state == "passed":
            comment = (
                f"Check `{current.item_id}` passed on `{head_sha}` "
                f"(first=`{current.first_attempt}`, retry=`{current.retry_attempt or 'n/a'}`): "
                f"{current.job_url}\n\nClosing automatically."
            )
            if apply:
                github.close_issue(number, comment)
            actions["closed"].append({"number": number, "key": key})
        elif current is None or current.state == "unknown":
            actions["kept_open_unknown"].append({"number": number, "key": key})

    # 2. Partition current failures into existing updates vs eligible new issues.
    new_failures: list[ItemOutcome] = []
    for item in all_failed:
        existing = open_by_key.get(item.dedup_key)
        if existing is not None:
            number = int(existing["number"])
            body = format_item_issue_body(
                tier, github.repository, head_sha, last_green_sha, run_url, item
            )
            if apply:
                github.update_issue_body(number, body)
            actions["updated"].append({"number": number, "key": item.dedup_key})
        elif tier == "tier-b" and item.dedup_key not in prev_failed_keys:
            actions["suppressed_first_night"].append(item.dedup_key)
        else:
            new_failures.append(item)

    # 3. Open new issues (or one consolidated broad-failure issue if capped).
    labels = TIER_LABELS[tier]
    if new_failures and apply:
        github.ensure_labels(labels)

    if len(new_failures) > BROAD_FAILURE_THRESHOLD:
        body = format_broad_failure_body(
            tier, github.repository, head_sha, last_green_sha, run_url, new_failures
        )
        existing_broad = open_by_key.get(broad_key)
        if existing_broad is not None:
            number = int(existing_broad["number"])
            if apply:
                github.update_issue_body(number, body)
            actions["updated"].append({"number": number, "key": broad_key})
        else:
            title = format_issue_title(
                tier, f"Broad failure ({len(new_failures)} items)"
            )
            created = (
                github.create_issue(title, body, labels)
                if apply
                else {"number": None}
            )
            actions["opened"].append(
                {"number": created.get("number"), "key": broad_key}
            )
    else:
        for item in new_failures:
            title = format_issue_title(tier, item.item_id)
            body = format_item_issue_body(
                tier, github.repository, head_sha, last_green_sha, run_url, item
            )
            created = (
                github.create_issue(title, body, labels)
                if apply
                else {"number": None}
            )
            actions["opened"].append(
                {"number": created.get("number"), "key": item.dedup_key}
            )

    summary = {
        "tier": tier,
        "head_sha": head_sha,
        "last_green_sha": last_green_sha,
        "apply": apply,
        "counts": {
            "passed": len(all_passed),
            "failed": len(all_failed),
            "unknown": len(all_unknown),
        },
        "actions": actions,
    }
    return summary


def write_step_summary(summary: dict[str, Any]) -> None:
    summary_path = os.getenv("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return
    counts = summary["counts"]
    actions = summary["actions"]
    lines = [
        f"## Nightly Report (`{summary['tier']}`)",
        "",
        f"- **Head SHA:** `{summary['head_sha']}`",
        f"- **Last Green SHA:** `{summary['last_green_sha'] or 'none'}`",
        f"- **Mode:** `{'apply' if summary['apply'] else 'dry-run'}`",
        f"- **Outcomes:** passed={counts['passed']}, failed={counts['failed']}, unknown={counts['unknown']}",
        f"- **Issues:** opened={len(actions['opened'])}, updated={len(actions['updated'])}, closed={len(actions['closed'])}, suppressed_first_night={len(actions['suppressed_first_night'])}",
        "",
    ]
    if actions["suppressed_first_night"]:
        lines.append("### Suppressed first-night live failures (Tier B)")
        lines.append("")
        for key in actions["suppressed_first_night"]:
            lines.append(f"- `{key}`")
        lines.append("")
    with Path(summary_path).open("a", encoding="utf-8") as handle:
        handle.write("\n".join(lines))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Report OpenDAL nightly CI status")
    parser.add_argument("--tier", choices=("tier-a", "tier-b"), required=True)
    parser.add_argument("--workflow-file", required=True)
    parser.add_argument("--run-id", type=int, required=True)
    parser.add_argument("--head-sha", required=True)
    parser.add_argument("--repository", default=os.getenv("GITHUB_REPOSITORY", ""))
    parser.add_argument("--self-job-name", default="Report Nightly Status")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)

    if not args.repository:
        raise ValueError("GITHUB_REPOSITORY or --repository is required")
    token = os.getenv("GH_TOKEN") or os.getenv("GITHUB_TOKEN")
    if not token:
        raise ValueError("GH_TOKEN or GITHUB_TOKEN is required")

    github = GitHub(args.repository, token)
    summary = reconcile(
        github,
        tier=args.tier,
        workflow_file=args.workflow_file,
        run_id=args.run_id,
        head_sha=args.head_sha,
        self_job_name=args.self_job_name,
        apply=args.apply,
    )
    write_step_summary(summary)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
