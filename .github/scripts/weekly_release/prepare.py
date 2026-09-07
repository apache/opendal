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

import os
import tempfile
from pathlib import Path

from github import ensure_ref, pull_request, push
from model import PACKAGE_PATTERN, versions
from runtime import git, run

PACKAGE_FILE = "dev/src/release/package.rs"


def apply_versions(root, targets, baseline):
    path = root / PACKAGE_FILE
    current = versions(path.read_text())
    if not set(targets).issubset(current):
        raise ValueError("cannot update an unknown package")
    path.write_text(
        PACKAGE_PATTERN.sub(
            lambda m: f'make_package("{m[1]}", "{targets.get(m[1], m[2])}"',
            path.read_text(),
        )
    )
    run(
        "cargo",
        "run",
        "--quiet",
        "--manifest-path",
        root / "dev/Cargo.toml",
        "--",
        "update-version",
        "--baseline",
        baseline,
        cwd=root,
        env=build_env(),
    )
    # Generated native loader package versions are not managed by update-version.
    if "bindings/nodejs" in targets:
        for name in ("index.cjs", "index.mjs"):
            file = root / "bindings/nodejs" / name
            if file.exists():
                file.write_text(
                    file.read_text().replace(
                        current["bindings/nodejs"], targets["bindings/nodejs"]
                    )
                )


def build_env():
    return {
        k: v
        for k, v in os.environ.items()
        if not any(
            word in k.upper()
            for word in ("TOKEN", "SECRET", "PASSWORD", "PAT", "GIT_CONFIG")
        )
        or k == "PATH"
    }


def candidate(record, cfg):
    repo = cfg["repository"]
    branch = record["branch"]
    ensure_ref(repo, "heads/" + branch, record["cutoff_sha"])
    bump_branch = branch + "-bump"
    with tempfile.TemporaryDirectory(prefix="opendal-prepare-") as tmp:
        root = Path(tmp) / "repo"
        git("worktree", "add", "--detach", root, record["cutoff_sha"])
        try:
            apply_versions(root, record["versions"], record["baseline_tag"])
            run(
                "python3",
                "scripts/dependencies.py",
                "generate",
                cwd=root,
                env=build_env(),
            )
            changelog = root / "CHANGELOG.md"
            notes = "\n".join(
                f"- {c['summary']}" for c in record["changes"] if c["packages"]
            )
            changelog.write_text(
                f"# v{record['version']}\n\n{notes}\n\n" + changelog.read_text()
            )
            git("add", "--all", cwd=root)
            env = dict(
                build_env(),
                GIT_AUTHOR_NAME="OpenDAL Release",
                GIT_AUTHOR_EMAIL="dev@opendal.apache.org",
                GIT_COMMITTER_NAME="OpenDAL Release",
                GIT_COMMITTER_EMAIL="dev@opendal.apache.org",
                GIT_AUTHOR_DATE=record["cutoff"],
                GIT_COMMITTER_DATE=record["cutoff"],
            )
            run(
                "git",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "-m",
                f"Prepare {record['rc']}",
                cwd=root,
                env=env,
            )
            push(bump_branch, root)
        finally:
            git("worktree", "remove", "--force", root)
    body = (
        f"Prepare {record['rc']} from cutoff `{record['cutoff_sha']}`.\n\n"
        "Review package versions, compatibility and changelog before merging into this release branch. "
        "The weekly controller then builds and stages the candidate and prepares an ATR Trusted Vote for the release manager to start.\n\n"
        + (
            "This is a rehearsal: official publication and main synchronization are disabled.\n\n"
            if record["dry_run"]
            else ""
        )
        + "Generated from the reviewed package inventory. Review compatibility before merging."
    )
    return pull_request(
        repo, bump_branch, branch, f"chore(release): prepare {record['rc']}", body
    )


def sync_main(record, cfg):
    return pull_request(
        cfg["repository"],
        record["branch"],
        "main",
        f"chore(release): merge released {record['version']} back to main",
        f"Source release {record['version']} has been published through ATR. "
        f"Merge its version and changelog updates from `{record['candidate_sha']}` back into main.\n\n"
        "Generated by the weekly release controller. Resolve conflicts through this PR; do not change the released commit.",
    )
