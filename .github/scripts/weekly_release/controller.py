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

"""Resume a weekly source release from persisted state; never wait on a runner."""

import argparse
import base64
import datetime as dt
import hashlib
import io
import json
import os
import zipfile
from pathlib import Path

from atr import ATR, vote_payload
from github import ensure_ref, ref, reminder
from model import (
    canonical,
    cutoff,
    cycle_id,
    next_rc,
    plan_versions,
    timestamp,
    version_tuple,
    versions,
)
from prepare import PACKAGE_FILE, candidate, sync_main
from runtime import Store, api, config, download, git, run

TERMINAL = {"released", "rehearsed", "cancelled", "skipped"}


def now():
    return dt.datetime.now(dt.timezone.utc)


def initial_state(cfg):
    tag = cfg["baseline_tag"]
    version_tuple(tag.removeprefix("v"))
    # A Git tag alone does not establish that the baseline was published.
    download(f"https://downloads.apache.org/opendal/{tag.removeprefix('v')}/")
    return {
        "schema": 1,
        "baseline": {
            "tag": tag,
            "sha": git("rev-parse", tag + "^{commit}"),
            "versions": versions(git("show", f"{tag}:{PACKAGE_FILE}")),
        },
        "candidates": {},
    }


def load_state(store, cfg):
    if ref(cfg["repository"], "heads/" + cfg["state_branch"]):
        return store.load()
    state = initial_state(cfg)
    repo = cfg["repository"]
    blob = api(
        f"repos/{repo}/git/blobs",
        {"content": base64.b64encode(canonical(state)).decode(), "encoding": "base64"},
    )
    tree = api(
        f"repos/{repo}/git/trees",
        {
            "tree": [
                {
                    "path": "state.json",
                    "mode": "100644",
                    "type": "blob",
                    "sha": blob["sha"],
                }
            ]
        },
    )
    commit = api(
        f"repos/{repo}/git/commits",
        {
            "message": "Initialize weekly release state",
            "tree": tree["sha"],
            "parents": [],
        },
    )
    ensure_ref(repo, "heads/" + cfg["state_branch"], commit["sha"])
    return store.load()


def cutoff_commit(cfg, when):
    # Push workflow runs retain the server timestamp and head even if their job
    # was queued or cancelled. Commit author/committer dates are not push times.
    repo = cfg["repository"]
    runs = []
    for page in range(1, 101):
        data = api(
            f"repos/{repo}/actions/workflows/weekly_release.yml/runs?event=push&branch=main&per_page=100&page={page}"
        )
        part = data["workflow_runs"]
        runs.extend(r for r in part if timestamp(r["created_at"]) <= when)
        if runs or len(part) < 100:
            break
    if not runs:
        raise ValueError(
            "no main push snapshot exists before the cutoff; wait for the next week or run a rehearsal"
        )
    return max(runs, key=lambda r: (r["created_at"], r["id"]))["head_sha"]


def plan(state, cfg, dry_run=False):
    when = now() if dry_run else cutoff(now())
    identity = (
        ("rehearsal-" + os.environ["GITHUB_RUN_ID"]) if dry_run else cycle_id(when)
    )
    if identity in state["candidates"]:
        return
    baseline = state["baseline"]
    sha = git("rev-parse", "origin/main") if dry_run else cutoff_commit(cfg, when)
    comparison = baseline.get("cutoff_sha", baseline["sha"])
    git("merge-base", "--is-ancestor", comparison, sha)
    synced = set()
    for previous in state["candidates"].values():
        if previous.get("sync_number"):
            pr = api(f"repos/{cfg['repository']}/pulls/{previous['sync_number']}")
            if pr["merged"]:
                synced.add(pr["merge_commit_sha"])
    commits = git("rev-list", "--first-parent", f"{comparison}..{sha}").splitlines()
    paths = []
    for commit in commits:
        if commit not in synced:
            paths.extend(
                git(
                    "diff",
                    "--name-only",
                    commit + "^",
                    commit,
                    "--",
                    "core",
                    "bindings",
                    "integrations",
                    "LICENSE",
                    "NOTICE",
                ).splitlines()
            )
    if not paths:
        state["candidates"][identity] = {"id": identity, "phase": "skipped"}
        return
    inventory = versions(git("show", f"{sha}:{PACKAGE_FILE}"))
    # New declarations are compatibility input. Existing declarations remain
    # immutable, so publication consumes them by moving the successful baseline.
    changes = []
    for path in git(
        "diff",
        "--diff-filter=A",
        "--name-only",
        comparison,
        sha,
        "--",
        ".release/changes",
    ).splitlines():
        if path.endswith(".json"):
            changes.append(json.loads(git("show", f"{sha}:{path}")))
    # All packages are released together. Explicit larger versions already
    # reviewed in package.rs and release declarations take precedence.
    changes.append(
        {
            "summary": f"[Changes since {baseline['tag']}](https://github.com/{cfg['repository']}/compare/{baseline['tag']}...{sha})",
            "packages": {p: "patch" for p in inventory},
        }
    )
    targets = plan_versions(baseline["versions"], inventory, changes)
    targets = {p: max(v, inventory[p], key=version_tuple) for p, v in targets.items()}
    tags = git("tag", "--list").splitlines()
    tags += [r["rc"] for r in state["candidates"].values() if "rc" in r]
    rc = next_rc(targets["core"], tags)
    state["candidates"][identity] = {
        "id": identity,
        "phase": "preparing",
        "dry_run": dry_run,
        "cutoff": when.isoformat(),
        "cutoff_sha": sha,
        "baseline_tag": baseline["tag"],
        "versions": targets,
        "version": targets["core"],
        "rc": rc,
        "atr_version": rc.removeprefix("v"),
        "branch": f"release-candidates/{identity.lower()}",
        "changes": changes,
        "dispatches": {},
    }


def workflow(record, cfg, filename, inputs, persist):
    repo = cfg["repository"]
    data = api(
        f"repos/{repo}/actions/workflows/{filename}/runs?event=workflow_dispatch&head_sha={record['candidate_sha']}&per_page=100"
    )
    intent = record["dispatches"].get(filename)
    runs = [
        r
        for r in data["workflow_runs"]
        if r["head_branch"] == record["branch"]
        and r["head_sha"] == record["candidate_sha"]
        and intent
        and timestamp(r["created_at"]) >= timestamp(intent)
    ]
    if runs:
        run = max(runs, key=lambda r: (r["id"], r["run_attempt"]))
        if run["status"] != "completed":
            return None
        if run["conclusion"] != "success":
            raise ValueError(
                f"{filename} failed: {run['html_url']}; rerun the failed workflow"
            )
        return run
    if intent:
        # A lost dispatch response is not permission to create a second upload.
        raise ValueError(
            f"awaiting {filename} dispatch recorded at {intent}; if no run exists, use retry-dispatch"
        )
    if ref(repo, "heads/" + record["branch"]) != record["candidate_sha"]:
        raise ValueError("release branch moved after candidate freeze")
    record["dispatches"][filename] = now().replace(microsecond=0).isoformat()
    persist()
    api(
        f"repos/{repo}/actions/workflows/{filename}/dispatches",
        {"ref": record["branch"], "inputs": inputs},
    )
    return None


def expected_paths(record):
    return {
        f"{record['version']}/apache-opendal-{p.replace('/', '-')}-{v}-src.tar.gz{suffix}"
        for p, v in record["versions"].items()
        for suffix in ("", ".asc", ".sha512")
    }


def output(**values):
    with open(os.environ["GITHUB_OUTPUT"], "a") as file:
        file.writelines(f"{key}={value}\n" for key, value in values.items())


def signed_checksums(record, cfg):
    endpoint = (
        f"repos/{cfg['repository']}/actions/runs/{record['compose_run']}/artifacts"
    )
    artifacts = api(endpoint)["artifacts"]
    name = f"signed-source-{record['compose_run']}-{record['compose_attempt']}"
    matches = [a for a in artifacts if a["name"] == name and not a["expired"]]
    if len(matches) != 1:
        raise ValueError("missing immutable signed artifact")
    record["signed_artifact"] = matches[0]["id"]
    raw = run(
        "gh",
        "api",
        f"repos/{cfg['repository']}/actions/artifacts/{matches[0]['id']}/zip",
    )
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        names = archive.namelist()
        expected = expected_paths(record)
        if (
            len(names) != len(set(names))
            or {record["version"] + "/" + n for n in names} != expected
        ):
            raise ValueError("signed CI artifact inventory differs")
        return {
            record["version"] + "/" + n: hashlib.sha512(archive.read(n)).hexdigest()
            for n in names
        }


def published_files(record):
    # Check the distribution area itself; vote resolution is not publication.
    for path, digest in record["checksums"].items():
        data = download("https://downloads.apache.org/opendal/" + path)
        if hashlib.sha512(data).hexdigest() != digest:
            raise ValueError(f"published bytes differ from the voted candidate: {path}")


def capture_files(record, atr):
    from hashlib import sha512

    path = f"/release/paths/opendal/{record['atr_version']}/{record['revision']}"
    value = atr.request(path)
    if value is None or set(value["rel_paths"]) != expected_paths(record):
        raise ValueError(
            "ATR candidate inventory differs from the expected signed source bundle"
        )
    # Download pinned bytes only after ATR has made the candidate public for voting.
    # The candidate API and vote revision are rechecked before and after downloads.
    result = {}
    for name in value["rel_paths"]:
        # The download/path route serves the currently frozen voting revision.
        raw = download(
            f"https://releases.apache.org/download/path/opendal/{record['atr_version']}/{name}"
        )
        result[name] = sha512(raw).hexdigest()
    release = atr.release(record["atr_version"])
    if (
        release["latest_revision_number"] != record["revision"]
        or release["phase"] != "release_candidate"
    ):
        raise ValueError("ATR candidate changed while recording vote artifacts")
    if result != record["checksums"]:
        raise ValueError(
            "ATR bytes differ from the signed CI artifact; RM must cancel the vote in ATR"
        )
    record["verified_vote_files"] = True


def reconcile(record, state, cfg, atr, persist):
    phase = record["phase"]
    repo = cfg["repository"]
    if phase == "preparing":
        pr = candidate(record, cfg)
        record.update(phase="reviewing", bump_pr=pr["number"], bump_url=pr["html_url"])
    elif phase == "reviewing":
        pr = api(f"repos/{repo}/pulls/{record['bump_pr']}")
        if not pr["merged"]:
            if pr["state"] == "closed":
                record["phase"] = "cancelled"
            return
        sha = pr["merge_commit_sha"]
        git("fetch", "origin", record["branch"])
        git("merge-base", "--is-ancestor", record["cutoff_sha"], sha)
        actual = versions(git("show", f"{sha}:{PACKAGE_FILE}"))
        if actual != record["versions"]:
            raise ValueError(
                "merged version plan differs; cancel and prepare a new candidate"
            )
        record.update(candidate_sha=sha, phase="checking")
    elif phase == "checking":
        # Reconcile the tag on every retry, including a lost creation response.
        ensure_ref(repo, "tags/" + record["rc"], record["candidate_sha"])
        for filename in cfg["validation_workflows"]:
            if not workflow(record, cfg, filename, {}, persist):
                return
        record["phase"] = "composing"
    elif phase == "composing":
        run_id = int(os.environ["GITHUB_RUN_ID"])
        if "compose_run" not in record:
            if ref(repo, "heads/" + record["branch"]) != record["candidate_sha"]:
                raise ValueError("release branch moved after candidate freeze")
            record["compose_run"] = run_id
            persist()
        if record["compose_run"] == run_id:
            # workflow_call retains the initiating actor for ATR OIDC. A bot
            # dispatch would replace it with github-actions[bot].
            output(
                compose="true",
                candidate=record["candidate_sha"],
                rc=record["atr_version"],
            )
            return
        build = api(f"repos/{repo}/actions/runs/{record['compose_run']}")
        if build["status"] != "completed":
            return
        if build["conclusion"] != "success":
            raise ValueError(
                f"compose failed: {build['html_url']}; rerun all jobs of that run"
            )
        record.update(phase="staged", compose_attempt=build["run_attempt"])
    elif phase == "staged":
        release = atr.release(record["atr_version"])
        if not release or release["phase"] != "release_candidate_draft":
            raise ValueError("expected the uploaded ATR draft")
        revision = release["latest_revision_number"]
        ongoing = atr.request(
            f"/checks/ongoing/opendal/{record['atr_version']}/{revision}"
        )
        if ongoing["ongoing"]:
            return
        checks = atr.checks(record["atr_version"], revision)
        if any(c["status"] == "blocker" for c in checks):
            raise ValueError("ATR refuses to start a vote while blockers exist")
        policy = atr.request("/policy/get/opendal")
        if policy["policy_vote_mode"] != "trusted":
            raise ValueError("OpenDAL must use ATR Trusted Vote")
        record["checksums"] = signed_checksums(record, cfg)
        record["revision"] = revision
        paths = atr.request(
            f"/release/paths/opendal/{record['atr_version']}/{revision}"
        )
        if set(paths["rel_paths"]) != expected_paths(record):
            raise ValueError("unexpected ATR files or publication layout")
        # OpenDAL's own validation workflows determine quality. ATR concerns are
        # acknowledged as requested, rather than becoming another approval gate.
        record["vote_request"] = vote_payload(
            record,
            revision,
            [c["checker"] for c in checks if c["status"] in {"concern", "exception"}],
        )
        record["phase"] = "awaiting-vote"
    elif phase == "awaiting-vote":
        release = atr.release(record["atr_version"])
        if not release or release["latest_revision_number"] != record["revision"]:
            raise ValueError(
                "ATR candidate changed; cancel and prepare a new candidate"
            )
        if release["phase"] == "release_candidate_draft":
            record["attention"] = (
                "RM: review vote-request.json and start the 72-hour Trusted Vote in ATR."
            )
            return
        if release["phase"] not in {"release_candidate", "release_preview", "release"}:
            raise ValueError("unexpected ATR vote state")
        if release["vote_mode"] != "trusted" or not release["vote_started"]:
            raise ValueError("ATR vote does not match the prepared Trusted Vote")
        record.pop("attention", None)
        record["phase"] = "voting"
    elif phase == "voting":
        release = atr.release(record["atr_version"])
        if release is None:
            raise ValueError("ATR candidate disappeared; RM must reconcile")
        if release["phase"] == "release_candidate_draft":
            record["phase"] = "cancelled"
            return
        if not record.get("discussion"):
            record["discussion"] = reminder(repo, record)
        if release["phase"] == "release_candidate":
            if not record.get("verified_vote_files"):
                capture_files(record, atr)
            if now() >= timestamp(release["vote_started"]) + dt.timedelta(hours=72):
                record["attention"] = (
                    "Vote remains open after 72 hours; RM may cancel in ATR."
                )
            return
        if (
            release["phase"] not in {"release_preview", "release"}
            or not release["vote_resolved"]
        ):
            raise ValueError("unexpected ATR vote state")
        if record["dry_run"]:
            record["phase"] = "rehearsed"
        else:
            if not record.get("verified_vote_files"):
                raise ValueError(
                    "voted artifact digests were not recorded; RM must reconcile before publication follow-up"
                )
            record["phase"] = "publishing"
    elif phase == "publishing":
        if record["dry_run"]:
            raise ValueError("rehearsals cannot publish")
        published_files(record)
        # Freeze the successful baseline before nonessential GitHub follow-up.
        state["baseline"] = {
            "tag": "v" + record["version"],
            "sha": record["candidate_sha"],
            "versions": record["versions"],
            "cutoff_sha": record["cutoff_sha"],
        }
        record["phase"] = "announcing"
    elif phase == "announcing":
        release = atr.release(record["atr_version"])
        if release["phase"] != "release":
            if record.get("announce_requested"):
                raise ValueError(
                    "announcement outcome needs reconciliation in ATR; no duplicate email"
                )
            record["announce_requested"] = True
            persist()
            output(
                announce="true",
                rc=record["atr_version"],
                version=record["version"],
                revision=release["latest_revision_number"],
            )
            return
        record["phase"] = "syncing"
    elif phase == "syncing":
        # Lightweight final tags do not trigger tag-based language publishers;
        # publishing other channels is a separate, explicitly dispatched workflow.
        ensure_ref(repo, "tags/v" + record["version"], record["candidate_sha"])
        pr = sync_main(record, cfg)
        record.update(
            phase="released", sync_pr=pr["html_url"], sync_number=pr["number"]
        )
    else:
        raise ValueError(f"unknown phase: {phase}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "operation",
        choices=[
            "tick",
            "rehearsal",
            "retry-dispatch",
            "cancel",
            "retry-announce",
        ],
    )
    parser.add_argument("--candidate")
    parser.add_argument("--workflow", choices=["ci_odev.yml", "ci_check.yml"])
    args = parser.parse_args()
    cfg = config()
    if (
        cfg["repository"] != "apache/opendal"
        or os.environ.get("GITHUB_REPOSITORY") != "apache/opendal"
    ):
        raise ValueError("weekly releases run only in apache/opendal")
    store = Store(cfg)
    state = load_state(store, cfg)
    saved = canonical(state)

    def persist():
        nonlocal saved
        current = canonical(state)
        if current != saved:
            store.save(state)
            saved = current

    atr = ATR()
    if args.operation in {"retry-dispatch", "cancel", "retry-announce"}:
        record = state["candidates"][args.candidate]
        if args.operation == "retry-dispatch":
            if record["phase"] != "checking":
                raise ValueError("candidate is not dispatching")
            record["dispatches"].pop(args.workflow, None)
        elif args.operation == "retry-announce":
            if record["phase"] != "announcing":
                raise ValueError("candidate is not announcing")
            record.pop("announce_requested", None)
        else:
            release = atr.release(record["atr_version"])
            if release and release["phase"] != "release_candidate_draft":
                raise ValueError(
                    "cancel the vote in ATR first; a published release cannot be cancelled"
                )
            if record["phase"] in {"publishing", "announcing", "syncing", "released"}:
                raise ValueError("publication must resume with the same candidate")
            record["phase"] = "cancelled"
        persist()
        return
    active = [r for r in state["candidates"].values() if r["phase"] not in TERMINAL]
    if len(active) > 1:
        raise ValueError("multiple active candidates")
    if not active:
        plan(state, cfg, dry_run=args.operation == "rehearsal")
        persist()
        active = [r for r in state["candidates"].values() if r["phase"] not in TERMINAL]
    elif args.operation == "rehearsal":
        raise ValueError(
            "finish or cancel the active candidate before starting a rehearsal"
        )
    for record in active:
        try:
            for _ in range(12):
                previous = record["phase"]
                reconcile(record, state, cfg, atr, persist)
                record.pop("error", None)
                persist()
                if record["phase"] == previous or record["phase"] in TERMINAL:
                    break
        except Exception as error:
            record["error"] = str(error)
            raise
        finally:
            persist()
            if record["phase"] == "awaiting-vote":
                handoff = Path("vote-request.json")
                handoff.write_bytes(canonical(record["vote_request"]))
            summary = f"Candidate {record['id']}: {record['phase']}\n{record.get('error', record.get('attention', ''))}\n"
            print(summary)
            if path := os.environ.get("GITHUB_STEP_SUMMARY"):
                with open(path, "a") as file:
                    file.write(summary)


if __name__ == "__main__":
    main()
