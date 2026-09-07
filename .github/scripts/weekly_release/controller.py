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

"""Prepare fresh candidates; use GitHub and ATR as the release record."""

import argparse
import datetime as dt
import json
import os
import re
from pathlib import Path

from artifacts import published_files, signed_checksums
from atr import ATR, vote_payload
from github import ensure_ref, ref, reminder
from model import canonical, cutoff, plan_versions, timestamp, version_tuple, versions
from prepare import PACKAGE_FILE, candidate, sync_main
from runtime import api, download, git

REPO = "apache/opendal"
CFG = {"repository": REPO}
RC = re.compile(r"(\d+\.\d+\.\d+)-rc\.[1-9]\d*")


def output(**values):
    with open(os.environ["GITHUB_OUTPUT"], "a") as file:
        file.writelines(f"{key}={value}\n" for key, value in values.items())


def releases(atr):
    result = []
    while True:
        page = atr.request(f"/project/releases/opendal?limit=100&offset={len(result)}")
        result.extend(page["releases"])
        if len(result) >= page["count"]:
            return result
        if not page["releases"]:
            raise ValueError("ATR release listing made no progress")


def require_idle(remote):
    if any(r["phase"] in {"release_candidate", "release_preview"} for r in remote):
        raise ValueError(
            "RM must cancel the open vote or finish the approved release before preparing another candidate"
        )


def cutoff_commit(when):
    for page in range(1, 101):
        rows = api(
            f"repos/{REPO}/actions/workflows/weekly_release.yml/runs?event=push&branch=main&per_page=100&page={page}"
        )["workflow_runs"]
        eligible = [r for r in rows if timestamp(r["created_at"]) <= when]
        if eligible:
            return max(eligible, key=lambda r: (r["created_at"], r["id"]))["head_sha"]
        if len(rows) < 100:
            break
    raise ValueError(
        "no main push snapshot before Friday cutoff; wait for the next week"
    )


def baseline(remote):
    # Include published ATR releases even when GitHub follow-up has not created
    # their final tag yet. Failed drafts and votes never consume a stable version.
    candidates = {
        t[1:]: t
        for t in git("tag", "--list").splitlines()
        if re.fullmatch(r"v\d+\.\d+\.\d+", t)
    }
    for release in remote:
        match = RC.fullmatch(release["version"])
        if release["phase"] == "release" and match:
            tag = "v" + release["version"]
            if ref(REPO, "tags/" + tag):
                candidates.setdefault(match[1], tag)
    available = download("https://downloads.apache.org/opendal/").decode()
    for version in sorted(candidates, key=version_tuple, reverse=True):
        if f'href="{version}/"' in available:
            tag = candidates[version]
            return tag, versions(git("show", f"{tag}:{PACKAGE_FILE}"))
    raise ValueError("no published source release matches a local Git tag")


def prepare_new(atr, rehearsal):
    remote = releases(atr)
    require_idle(remote)
    tag, base = baseline(remote)
    when = dt.datetime.now(dt.timezone.utc)
    sha = git("rev-parse", "origin/main") if rehearsal else cutoff_commit(cutoff(when))
    inventory = versions(git("show", f"{sha}:{PACKAGE_FILE}"))
    targets = plan_versions(base, inventory)
    # A fresh invocation, including a rerun, owns a different RC and branch.
    suffix = str(
        int(os.environ["GITHUB_RUN_ID"]) * 1000 + int(os.environ["GITHUB_RUN_ATTEMPT"])
    )
    rc = f"{targets['core']}-rc.{suffix}"
    branch = "release-candidates/" + ("rehearsal-" if rehearsal else "") + rc
    if ref(REPO, "heads/" + branch) or atr.release(rc):
        raise ValueError("candidate identity exists; start a new workflow run")
    record = {
        "branch": branch,
        "cutoff_sha": sha,
        "cutoff": when.isoformat(),
        "baseline_tag": tag,
        "versions": targets,
        "version": targets["core"],
        "rc": "v" + rc,
        "dry_run": rehearsal,
        "changes": [
            {
                "summary": f"[Changes since {tag}](https://github.com/{REPO}/compare/{tag}...{sha})",
                "packages": targets,
            }
        ],
    }
    pr = candidate(record, CFG)
    print(f"Review candidate versions and merge the preparation PR: {pr['html_url']}")


def merged_candidate(atr):
    pr = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())["pull_request"]
    branch = pr["base"]["ref"]
    key = branch.removeprefix("release-candidates/")
    rc = key.removeprefix("rehearsal-")
    if (
        not pr["merged"]
        or not RC.fullmatch(rc)
        or pr["head"]["repo"]["full_name"] != REPO
        or pr["head"]["ref"] != branch + "-bump"
    ):
        raise ValueError("expected a merged internal candidate version PR")
    require_idle(releases(atr))
    if atr.release(rc):
        raise ValueError(
            "ATR candidate already exists; start a fresh preparation instead of uploading again"
        )
    sha = pr["merge_commit_sha"]
    if ref(REPO, "heads/" + branch) != sha:
        raise ValueError("candidate branch moved after version review")
    if versions(git("show", f"{sha}:{PACKAGE_FILE}"))["core"] != RC.fullmatch(rc)[1]:
        raise ValueError("reviewed core version differs from candidate identity")
    ensure_ref(REPO, "tags/v" + rc, sha)
    output(candidate=sha, rc=rc, rehearsal=str(key.startswith("rehearsal-")).lower())


def handoff(atr, rc, sha, rehearsal):
    release = atr.release(rc)
    if not release or release["phase"] != "release_candidate_draft":
        raise ValueError("expected an ATR draft")
    record = {
        "version": RC.fullmatch(rc)[1],
        "rc": "v" + rc,
        "atr_version": rc,
        "candidate_sha": sha,
        "dry_run": rehearsal,
    }
    request = vote_payload(
        record,
        release["latest_revision_number"],
        [
            c["checker"]
            for c in atr.checks(rc, release["latest_revision_number"])
            if c["status"] in {"concern", "exception"}
        ],
    )
    Path("vote-request.json").write_bytes(canonical(request))
    print(
        f"RM: review ATR revision {request['revision']} and start the Trusted Vote. Refresh concerns after ATR checks finish. Do not rerun compose for this RC."
    )


def follow_up(atr):
    for release in releases(atr):
        rc = release["version"]
        match = RC.fullmatch(rc)
        if not match or release["phase"] not in {
            "release_candidate",
            "release_preview",
            "release",
        }:
            continue
        branch = "release-candidates/" + rc
        sha = ref(REPO, "tags/v" + rc)
        if not sha or ref(REPO, "heads/" + branch) != sha:
            continue  # Other release flows and rehearsals own their own follow-up.
        if release["vote_mode"] != "trusted" or not release["vote_started"]:
            raise ValueError("expected an ATR Trusted Vote")
        record = {
            "version": match[1],
            "rc": "v" + rc,
            "atr_version": rc,
            "candidate_sha": sha,
            "branch": branch,
            "dry_run": False,
            "revision": release["latest_revision_number"],
        }
        if release["phase"] == "release_candidate":
            reminder(REPO, record)
            continue
        if ref(REPO, "tags/v" + match[1]):
            ensure_ref(REPO, "tags/v" + match[1], sha)
            sync_main(record, CFG)
            continue
        record["versions"] = versions(git("show", f"{sha}:{PACKAGE_FILE}"))
        runs = api(
            f"repos/{REPO}/actions/workflows/weekly_release.yml/runs?head_sha={sha}&event=pull_request&per_page=100"
        )["workflow_runs"]
        successful = [r for r in runs if r["conclusion"] == "success"]
        if not successful:
            raise ValueError("no successful compose run for the approved candidate")
        build = max(successful, key=lambda r: r["id"])
        record.update(compose_run=build["id"], compose_attempt=build["run_attempt"])
        record["checksums"] = signed_checksums(record)
        published_files(record)
        if release["phase"] == "release_preview":
            output(
                announce="true", rc=rc, version=match[1], revision=record["revision"]
            )
            return  # ATR changes phase when it accepts the announcement.
        ensure_ref(REPO, "tags/v" + match[1], sha)
        sync_main(record, CFG)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "operation", choices=["prepare", "rehearsal", "merged", "handoff", "follow-up"]
    )
    parser.add_argument("--rc")
    parser.add_argument("--sha")
    parser.add_argument("--rehearsal", action="store_true")
    args = parser.parse_args()
    if os.environ.get("GITHUB_REPOSITORY") != REPO:
        raise ValueError("weekly releases run only in apache/opendal")
    atr = ATR()
    if args.operation in {"prepare", "rehearsal"}:
        prepare_new(atr, args.operation == "rehearsal")
    elif args.operation == "merged":
        merged_candidate(atr)
    elif args.operation == "handoff":
        if (
            not args.rc
            or not RC.fullmatch(args.rc)
            or not args.sha
            or not re.fullmatch(r"[0-9a-f]{40}", args.sha)
        ):
            raise ValueError("handoff requires a valid RC and full commit SHA")
        handoff(atr, args.rc, args.sha, args.rehearsal)
    else:
        follow_up(atr)


if __name__ == "__main__":
    main()
