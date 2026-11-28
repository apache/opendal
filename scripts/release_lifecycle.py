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

"""Synchronize OpenDAL candidate discussions and publication with ATR.

ATR owns vote decisions. Git refs, Discussions, Actions runs and published
releases own their respective completion records; this module keeps no journal.
"""

import argparse
import base64
import dataclasses
import hashlib
import json
import os
import re
import subprocess
import tempfile
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

REPO = "apache/opendal"
ATR = "https://releases.apache.org"
RC = re.compile(r"([0-9]+\.[0-9]+\.[0-9]+)-rc\.([1-9][0-9]*)")
FINAL_WORKFLOWS = (
    "release_rust.yml",
    "release_python.yml",
    "release_nodejs.yml",
    "release_ruby.yml",
    "release_dotnet.yml",
    "release_dart.yml",
    "docs.yml",
)


def command(*args, data=None, cwd=None):
    result = subprocess.run(
        args, input=data, text=True, capture_output=True, cwd=cwd, check=False
    )
    if result.returncode:
        raise RuntimeError(f"{args[0]} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def gh(*args):
    return command("gh", *args)


def api(path, payload=None, optional=False):
    args = ["gh", "api", path]
    if payload is not None:
        args += ["--input", "-"]
    result = subprocess.run(
        args,
        input=json.dumps(payload) if payload is not None else None,
        text=True,
        capture_output=True,
        check=False,
    )
    if optional and result.returncode and "HTTP 404" in result.stderr:
        return None
    if result.returncode:
        raise RuntimeError(f"GitHub {path}: {result.stderr.strip()}")
    return json.loads(result.stdout) if result.stdout.strip() else None


def pages(path):
    return [
        item
        for page in json.loads(gh("api", "--paginate", "--slurp", path))
        for item in page
    ]


def request_json(url, payload=None, headers=None):
    headers = {"Accept": "application/json", **(headers or {})}
    if payload is not None:
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode() if payload is not None else None,
        headers=headers,
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        body = response.read()
        return json.loads(body) if body else None


def atr_jwt():
    url = os.environ["ACTIONS_ID_TOKEN_REQUEST_URL"] + f"&audience={ATR}/"
    headers = {
        "Authorization": "bearer " + os.environ["ACTIONS_ID_TOKEN_REQUEST_TOKEN"]
    }
    return request_json(url, headers=headers)["value"]


def atr_release(version, *, optional=False):
    try:
        release = request_json(f"{ATR}/api/release/get/opendal/{version}")["release"]
    except urllib.error.HTTPError as error:
        if optional and error.code == 404:
            return None
        raise
    if release["project_key"] != "opendal" or release["version"] != version:
        raise ValueError("ATR returned a different release")
    return release


def selected_revision(release):
    field = (
        "latest_revision_number"
        if release["phase"] == "release_candidate_draft"
        else "voted_revision_number"
    )
    number = release.get(field)
    if not isinstance(number, str) or not re.fullmatch(r"[0-9]+", number):
        raise ValueError(f"ATR revision is missing: {field}")
    return number


def manifest_url(version, revision):
    return f"{ATR}/api/release/manifest/opendal/{version}/{revision}"


def atr_manifest(version, revision):
    manifest = request_json(manifest_url(version, revision))
    if (manifest["project"], manifest["version"], manifest["revision"]) != (
        "opendal",
        version,
        revision,
    ):
        raise ValueError("ATR returned a different revision manifest")
    files = {}
    for entry in manifest["files"]:
        path, digest = entry["path"], entry["digest"]
        if path in files or not re.fullmatch(r"sha3-256:[a-f0-9]{64}", digest or ""):
            raise ValueError(f"Ambiguous or missing ATR digest for {path}")
        files[path] = digest
    if not files:
        raise ValueError("Empty ATR revision manifest")
    return files


def ref_sha(kind, name):
    ref = api(f"repos/{REPO}/git/ref/{kind}/{name}", optional=True)
    if ref is None:
        return None
    obj = ref["object"]
    while obj["type"] == "tag":
        obj = api(f"repos/{REPO}/git/tags/{obj['sha']}")["object"]
    if obj["type"] != "commit":
        raise ValueError(f"{name} does not identify a commit")
    return obj["sha"]


@dataclasses.dataclass(frozen=True)
class Candidate:
    rc: str
    sha: str

    def __post_init__(self):
        if not RC.fullmatch(self.rc) or not re.fullmatch(r"[0-9a-f]{40}", self.sha):
            raise ValueError("expected X.Y.Z-rc.N and a full commit SHA")

    @property
    def version(self):
        return RC.fullmatch(self.rc)[1]

    @property
    def branch(self):
        return f"releases/{self.rc}"

    @classmethod
    def load(cls, rc):
        if not RC.fullmatch(rc):
            raise ValueError("expected X.Y.Z-rc.N")
        sha = ref_sha("heads", f"releases/{rc}")
        if not sha or ref_sha("tags", f"v{rc}") != sha:
            raise ValueError(f"RC branch and tag disagree: {rc}")
        return cls(rc, sha)

    def revision_status(self, revision, files):
        encoded = json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
        return {
            "state": "success",
            "context": f"release/atr/{self.version}/{revision}",
            "description": f"{self.rc} {hashlib.sha256(encoded).hexdigest()}",
            "target_url": manifest_url(self.version, revision),
        }

    def recorded_revision(self, status):
        for recorded in pages(f"repos/{REPO}/commits/{self.sha}/statuses?per_page=100"):
            if (
                recorded["context"] == status["context"]
                and recorded["creator"]["login"] == "github-actions[bot]"
            ):
                return {key: recorded.get(key) for key in status}
        return None

    def atr(self):
        release = atr_release(self.version, optional=True)
        if release is None:
            # Before the transition, each RC owned a separate ATR release and vote.
            return atr_release(self.rc)
        revision = selected_revision(release)
        status = self.revision_status(revision, atr_manifest(self.version, revision))
        return release if self.recorded_revision(status) == status else None


def passed(release):
    return release["phase"] in {"release_preview", "release"} and bool(
        release.get("vote_resolved")
    )


def discussion_data():
    query = """query($endCursor:String) { repository(owner:"apache",name:"opendal") {
      id discussionCategories(first:100) { nodes { id name } }
      discussions(first:100,after:$endCursor) {
        nodes { id title body url } pageInfo { hasNextPage endCursor }
      }
    }}"""
    data = json.loads(
        gh("api", "graphql", "--paginate", "--slurp", "-f", f"query={query}")
    )
    repo = data[0]["data"]["repository"]
    discussions = [
        d for page in data for d in page["data"]["repository"]["discussions"]["nodes"]
    ]
    return (
        repo["id"],
        {c["name"]: c["id"] for c in repo["discussionCategories"]["nodes"]},
        discussions,
    )


def discussion(title, body, category="General"):
    repo, categories, discussions = discussion_data()
    matches = [d for d in discussions if d["title"] == title]
    if len(matches) > 1:
        raise ValueError(f"multiple discussions named {title}")
    if matches:
        # Actions tokens can create Discussions but cannot edit their bodies.
        # Publish lifecycle changes as comments on the original entry point.
        return matches[0]
    result = api(
        "graphql",
        {
            "query": "mutation($repo:ID!,$category:ID!,$title:String!,$body:String!){createDiscussion(input:{repositoryId:$repo,categoryId:$category,title:$title,body:$body}){discussion{id url body}}}",
            "variables": {
                "repo": repo,
                "category": categories[category],
                "title": title,
                "body": body,
            },
        },
    )
    return result["data"]["createDiscussion"]["discussion"]


def comment_once(discussion_id, marker, text):
    query = """query($id:ID!,$endCursor:String){node(id:$id){...on Discussion{
      comments(first:100,after:$endCursor){nodes{body} pageInfo{hasNextPage endCursor}}
    }}}"""
    replies = json.loads(
        gh(
            "api",
            "graphql",
            "--paginate",
            "--slurp",
            "-f",
            f"query={query}",
            "-f",
            f"id={discussion_id}",
        )
    )
    if any(
        marker in c["body"]
        for page in replies
        for c in page["data"]["node"]["comments"]["nodes"]
    ):
        return
    api(
        "graphql",
        {
            "query": "mutation($id:ID!,$body:String!){addDiscussionComment(input:{discussionId:$id,body:$body}){comment{id}}}",
            "variables": {"id": discussion_id, "body": f"{marker}\n{text}"},
        },
    )


def announcement(candidate, atr_version):
    v = candidate.version
    return f"""Hi everyone,

Apache OpenDAL {v} is now available.

OpenDAL is an Open Data Access Layer that provides unified access to storage services.

- [Downloads](https://opendal.apache.org/download)
- [Release notes](https://github.com/{REPO}/releases/tag/v{v})
- [Approved candidate]({ATR}/vote/opendal/{atr_version})
- [Website](https://opendal.apache.org/)

Thanks to everyone who contributed and verified this release.

The Apache OpenDAL community
"""


def version_notes(candidate):
    from release_impact import PLAN_PATH, render_plan

    content = api(
        f"repos/{REPO}/contents/{PLAN_PATH}?ref={candidate.sha}", optional=True
    )
    if content is None:
        return ""
    plan = json.loads(base64.b64decode(content["content"]))
    return render_plan(plan)


def vote_start_instructions(candidate, atr_version, revision):
    if atr_version == candidate.rc:
        return (
            f"Start this vote using the [ATR form]({ATR}/voting/opendal/{atr_version}). "
            f"Set **Publish location** to `{candidate.version}` for this existing RC-named release."
        )
    return f"""Before starting the vote, check **Publish location** on the [ATR vote form]({ATR}/voting/opendal/{atr_version}).
If it differs from `{candidate.version}`, set it to `{candidate.version}` and start the verified revision `{revision}` in the form.
If it already matches, you can start the vote with the command below. Run it only once.

```bash
uv run --python 3.13 --with "apache-trusted-releases @ git+https://github.com/apache/tooling-releases-client" python - <<'PYTHON'
from atrclient import api
from atrclient.models.api import VoteStartArgs

task = api.vote_start(VoteStartArgs(
    project="opendal",
    version="{atr_version}",
    revision="{revision}",
    email_to="dev@opendal.apache.org",
    automatic_resolve_when_finished=True,
    automatic_publish_when_resolved=True,
    notify_when_finished=True,
)).task
print(task.model_dump_json(indent=2))
assert task.task_args["automatic_resolve_when_finished"] is True
assert task.task_args["automatic_publish_when_resolved"] is True
PYTHON
```

This uses the official ATR client and the same credentials as `atr`. It checks
that the returned task enables both automatic resolution and publication. If a
check fails, inspect that task in ATR; do not repeat the start command. The CLI's
`--auto-publish` option alone does not enable automatic resolution."""


def notice(candidate, release, status=None):
    rc = candidate.rc
    atr_version = release["version"]
    phase = release["phase"]
    revision = selected_revision(release)
    if status is None:
        status = {
            "release_candidate_draft": "Candidate uploaded; RM verification is next.",
            "release_candidate": "Voting is open. Review the candidate and vote through ATR or the dev mailing list.",
            "release_preview": "Vote passed; final publication is being prepared.",
            "release": "ATR announcement sent; checking GitHub publication and follow-up.",
        }[phase]
        if phase == "release_candidate_draft" and atr_version == rc:
            status += f" Start voting in the ATR web form with Publish location set to `{candidate.version}`."
    branch = (
        f"releases/{candidate.version}"
        if release["phase"] == "release"
        else candidate.branch
    )
    body = (
        f"""**Apache OpenDAL {candidate.version} has been released.**

- [GitHub Release](https://github.com/{REPO}/releases/tag/v{candidate.version})
- [ATR status]({ATR}/vote/opendal/{atr_version})
- [Source branch](https://github.com/{REPO}/tree/{branch}); commit: `{candidate.sha}`.

Follow the status replies below for optional package distribution and cleanup.
"""
        if phase == "release"
        else f"""**Release candidate: {rc}**

ATR is the release authority. Follow the latest status replies below and the ATR
vote page for current progress; this opening post contains candidate instructions.

- [ATR checks]({ATR}/checks/opendal/{atr_version}) · [Download candidate]({ATR}/download/path/opendal/{atr_version}) · [Vote]({ATR}/vote/opendal/{atr_version})
- [Candidate branch](https://github.com/{REPO}/tree/{branch}) · [RC tag](https://github.com/{REPO}/releases/tag/v{rc})
- Candidate commit: `{candidate.sha}`; [ATR revision `{revision}` manifest]({manifest_url(atr_version, revision)}).
- [RC builds](https://github.com/{REPO}/actions?query=branch%3Av{rc}) · [Scheduled publication](https://github.com/{REPO}/actions/workflows/release_lifecycle.yml) · [Manual publication](https://github.com/{REPO}/actions/workflows/release_publish.yml)

{version_notes(candidate)}
### Release manager: next actions

1. Review the candidate in ATR. Other builds and language package staging are optional and do not block starting the ATR vote.
2. Review the source artifacts and announcement draft below.
3. Start the vote on the selected revision with automatic resolution and publication enabled. ATR checks the tally at the scheduled end; inspect ATR if the vote remains unresolved.

### CLI for release managers and agents

Read-only checks:

```bash
atr check status opendal {atr_version} {revision}
atr vote tabulate opendal {atr_version}
```

{vote_start_instructions(candidate, atr_version, revision)}

ATR attempts automatic resolution at the scheduled vote end and publishes the
source archives if the vote passes. Insufficient votes leave the vote open for
manual follow-up; do not assume later ballots automatically trigger another
attempt. If manual resolution is needed, review the tally first:

```bash
atr vote resolve opendal {atr_version} passed
```

These commands change the release state; agents need the RM's authorization for voting actions. The hourly GitHub workflow follows ATR's resolved result to create the final branch and tag, publish packages, and finish announcements and cleanup. It does not tally or resolve votes.

<details><summary>CLI installation and first-time authentication</summary>

Use the [official ATR CLI](https://github.com/apache/tooling-releases-client/blob/main/RELEASE-PROCESS.md). Configure `atr set asf.uid <ASF ID>` and `atr set tokens.pat` using its hidden prompt. Do not paste credentials into this Discussion. Browser controls on ATR are also available.

</details>

### Community participation

Download and verify the candidate. During voting, ASF committers can vote on ATR; everyone can reply to the official vote thread on dev@opendal.apache.org. This Discussion tracks progress, not a separate ballot.

<details><summary>Final announcement draft</summary>

{announcement(candidate, atr_version)}
</details>
"""
    )
    current = discussion(f"Release candidate: {rc}", body)
    if phase == "release_candidate":
        seq = release.get("current_vote_seq")
        if not isinstance(seq, int):
            raise ValueError("ATR vote sequence is missing")
        comment_once(
            current["id"],
            f"<!-- opendal-vote:{rc}:{seq} -->",
            f"Voting is now open: [{rc}]({ATR}/vote/opendal/{atr_version}). The ATR page shows the deadline and participation instructions. Follow ATR and these status replies for current progress; the opening post records candidate preparation. Other builds and language package staging are optional and do not block this vote.",
        )
    else:
        atr_status = (
            "ATR source release published"
            if phase == "release"
            else f"ATR revision: `{revision}`"
        )
        progress = f"""**{status}**

- {atr_status}; [current ATR status]({ATR}/vote/opendal/{atr_version}).
- [Source branch](https://github.com/{REPO}/tree/{branch}) · [Scheduled publication](https://github.com/{REPO}/actions/workflows/release_lifecycle.yml) · [Manual publication](https://github.com/{REPO}/actions/workflows/release_publish.yml).
"""
        digest = hashlib.sha256(progress.encode()).hexdigest()
        comment_once(
            current["id"],
            f"<!-- opendal-status:{rc}:{release.get('current_vote_seq')}:{digest} -->",
            progress,
        )
    return current


def workflow_runs(workflow, ref=None):
    args = [
        "run",
        "list",
        "--repo",
        REPO,
        "--workflow",
        workflow,
        "--limit",
        "100",
        "--json",
        "databaseId,headSha,status,conclusion,displayTitle,url,event",
    ]
    if ref:
        args += ["--branch", ref]
    return json.loads(gh(*args))


def dispatch(workflow, ref, fields=None):
    args = ["workflow", "run", workflow, "--repo", REPO, "--ref", ref]
    for name, value in (fields or {}).items():
        args += ["-f", f"{name}={value}"]
    gh(*args)


def candidates():
    refs = pages(f"repos/{REPO}/git/matching-refs/heads/releases/")
    return [
        ref["ref"].removeprefix("refs/heads/releases/")
        for ref in refs
        if RC.fullmatch(ref["ref"].removeprefix("refs/heads/releases/"))
    ]


def sync():
    approved_by_version = {}
    unbound = set()
    bound = set()
    errors = []
    for rc in candidates():
        try:
            candidate = Candidate.load(rc)
            release = candidate.atr()
            if release is None:
                unbound.add(candidate.version)
                continue
            if release["version"] == candidate.version:
                bound.add(candidate.version)
            if passed(release):
                approved_by_version.setdefault(candidate.version, []).append(rc)
            else:
                notice(candidate, release)
        except (RuntimeError, ValueError, OSError, KeyError) as error:
            errors.append(f"{rc}: {error}")
    unresolved = unbound - bound
    for version in sorted(unresolved):
        errors.append(f"{version}: no Git RC matches the selected ATR revision")
    approved = []
    for version, matches in approved_by_version.items():
        if version in unresolved:
            continue
        if len(matches) == 1:
            approved.extend(matches)
        else:
            errors.append(f"{version}: multiple approved candidates: {matches}")
    with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
        output.write(f"candidates={json.dumps(approved)}\n")
    if errors:
        raise RuntimeError("\n".join(errors))


def final_refs(candidate):
    branch, tag = f"releases/{candidate.version}", f"v{candidate.version}"
    existing_branch, existing_tag = ref_sha("heads", branch), ref_sha("tags", tag)
    for existing in (existing_branch, existing_tag):
        if existing and existing != candidate.sha:
            raise ValueError("final ref already identifies another commit")
    if existing_branch and existing_tag:
        return
    command("git", "fetch", "origin", candidate.sha)
    with tempfile.TemporaryDirectory(prefix="od-tag-", dir="/tmp") as temporary:
        home = Path(temporary)
        home.chmod(0o700)
        command(
            "gpg",
            "--homedir",
            temporary,
            "--batch",
            "--import",
            data=os.environ["GPG_SECRET_KEY"],
        )
        fingerprint = os.environ["SOURCE_SIGNING_FINGERPRINT"]
        if not re.fullmatch(r"[0-9A-F]{40}", fingerprint):
            raise ValueError("full signing fingerprint required")
        previous = os.environ.get("GNUPGHOME")
        os.environ["GNUPGHOME"] = temporary
        try:
            if not existing_tag:
                command(
                    "git",
                    "-c",
                    "user.name=OpenDAL Release",
                    "-c",
                    "user.email=dev@opendal.apache.org",
                    "-c",
                    f"user.signingkey={fingerprint}",
                    "tag",
                    "-s",
                    tag,
                    candidate.sha,
                    "-m",
                    f"Release {candidate.version} from {candidate.rc}",
                )
                command("git", "verify-tag", tag)
            refs = []
            leases = []
            if not existing_branch:
                refs.append(f"{candidate.sha}:refs/heads/{branch}")
                leases.append(f"--force-with-lease=refs/heads/{branch}:")
            if not existing_tag:
                refs.append(f"refs/tags/{tag}")
                leases.append(f"--force-with-lease=refs/tags/{tag}:")
            command(
                "git",
                "-c",
                "credential.helper=!gh auth git-credential",
                "push",
                "--atomic",
                *leases,
                "origin",
                *refs,
            )
        finally:
            if previous is None:
                os.environ.pop("GNUPGHOME", None)
            else:
                os.environ["GNUPGHOME"] = previous
            subprocess.run(
                ["gpgconf", "--homedir", temporary, "--kill", "gpg-agent"], check=False
            )


def publish_builds(candidate):
    pending = False
    failures = []
    for workflow in FINAL_WORKFLOWS:
        state = api(f"repos/{REPO}/actions/workflows/{workflow}")["state"]
        if state.startswith("disabled_"):
            print(f"Skipped {workflow}: {state}")
            continue
        runs = workflow_runs(workflow, f"v{candidate.version}")
        runs = [
            r
            for r in runs
            if r["headSha"] == candidate.sha
            and r["event"] in {"push", "workflow_dispatch"}
        ]
        if not runs:
            fields = {}
            if workflow == "release_nodejs.yml":
                fields = {"nodejs-publish": "true", "nodejs-publish-dry-run": "false"}
            if workflow == "docs.yml":
                fields = {
                    "release_version": f"v{candidate.version}",
                    "deploy-nightlies": "true",
                }
            dispatch(workflow, f"v{candidate.version}", fields)
            pending = True
        elif runs[0]["status"] != "completed":
            pending = True
        elif runs[0]["conclusion"] != "success":
            failures.append(runs[0]["url"])
    if failures:
        raise RuntimeError("Rerun failed publication jobs: " + ", ".join(failures))
    return not pending


def nexus_release(candidate):
    state = api(f"repos/{REPO}/actions/workflows/release_java.yml")["state"]
    if state.startswith("disabled_"):
        print(f"Skipped release_java.yml: {state}")
        return True
    pom = ET.fromstring(
        command("git", "show", f"{candidate.sha}:bindings/java/pom.xml")
    )
    version = pom.findtext("{http://maven.apache.org/POM/4.0.0}version")
    if not version or not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version):
        raise ValueError("Java package version is missing")
    central = f"https://repo.maven.apache.org/maven2/org/apache/opendal/opendal/{version}/opendal-{version}.pom"
    try:
        with urllib.request.urlopen(central, timeout=60) as response:
            published = ET.fromstring(response.read())
            if (
                published.findtext("{http://maven.apache.org/POM/4.0.0}version")
                != version
            ):
                raise ValueError("Maven Central returned a different package version")
            return True
    except urllib.error.HTTPError as error:
        if error.code != 404:
            raise
        error.close()
    runs = workflow_runs("release_java.yml", f"v{candidate.rc}")
    runs = [r for r in runs if r["headSha"] == candidate.sha]
    if not runs or runs[0]["conclusion"] != "success":
        raise ValueError("Java RC staging has not succeeded")
    log = gh("run", "view", str(runs[0]["databaseId"]), "--repo", REPO, "--log")
    ids = set(re.findall(r"deployByRepositoryId/(orgapacheopendal-[0-9]+)/", log))
    if len(ids) != 1:
        raise ValueError(
            "Java RC run must identify exactly one Nexus staging repository"
        )
    repo = ids.pop()
    token = base64.b64encode(
        f"{os.environ['NEXUS_USER']}:{os.environ['NEXUS_PASSWORD']}".encode()
    ).decode()
    headers = {"Authorization": f"Basic {token}"}
    base = "https://repository.apache.org/service/local/staging"
    data = request_json(f"{base}/repository/{repo}", headers=headers)
    if data.get("transitioning"):
        return False
    if data["type"] == "released":
        return False  # Wait for the exact version to appear on Maven Central.
    if data["type"] != "closed":
        raise ValueError(f"Nexus {repo} must be closed before publication")
    request_json(
        f"{base}/bulk/promote",
        {
            "data": {
                "stagedRepositoryIds": [repo],
                "description": f"Release Apache OpenDAL {candidate.version}",
            }
        },
        headers,
    )
    return False


def sync_versions(candidate):
    branch = f"automation/sync-release-{candidate.version}"
    pulls = json.loads(
        gh(
            "pr",
            "list",
            "--repo",
            REPO,
            "--head",
            branch,
            "--state",
            "all",
            "--json",
            "url,state",
        )
    )
    if pulls:
        if pulls[0]["state"] == "CLOSED":
            raise ValueError(
                f"Version sync PR was closed without merging: {pulls[0]['url']}"
            )
        return pulls[0]["url"]
    if not ref_sha("heads", branch):
        with tempfile.TemporaryDirectory(prefix="od-sync-") as directory:
            command("git", "fetch", "origin", "main")
            command("git", "worktree", "add", "--detach", directory, "origin/main")
            try:
                command("git", "checkout", "-b", branch, cwd=directory)
                command(
                    "cargo",
                    "run",
                    "--quiet",
                    "--manifest-path",
                    "dev/Cargo.toml",
                    "--",
                    "update-version",
                    "--baseline",
                    f"v{candidate.version}",
                    "--sync",
                    cwd=directory,
                )
                for lock in command(
                    "git", "ls-files", "*Cargo.lock", cwd=directory
                ).splitlines():
                    command(
                        "cargo",
                        "update",
                        "--workspace",
                        "--manifest-path",
                        str(Path(lock).with_name("Cargo.toml")),
                        cwd=directory,
                    )
                command("python3", "scripts/dependencies.py", "generate", cwd=directory)
                changelog = Path(directory) / "CHANGELOG.md"
                text = changelog.read_text()
                heading = f"# v{candidate.version}"
                if heading not in text.splitlines():
                    changelog.write_text(
                        f"{heading}\n\n- [Release notes](https://github.com/{REPO}/releases/tag/v{candidate.version})\n\n{text}"
                    )
                if not command("git", "status", "--porcelain", cwd=directory):
                    return "main already includes the released versions"
                command("git", "add", "--all", cwd=directory)
                command(
                    "git",
                    "-c",
                    "user.name=OpenDAL Release",
                    "-c",
                    "user.email=dev@opendal.apache.org",
                    "-c",
                    "commit.gpgsign=false",
                    "commit",
                    "-m",
                    f"Sync released versions for {candidate.version}",
                    cwd=directory,
                )
                command(
                    "git",
                    "-c",
                    "credential.helper=!gh auth git-credential",
                    "push",
                    "origin",
                    branch,
                    cwd=directory,
                )
            finally:
                command("git", "worktree", "remove", "--force", directory)
    body = f"""# Which issue does this PR close?

Release follow-up for {candidate.version}.

# Rationale for this change

Bring the published package versions back to main while preserving newer versions and development changes.

# What changes are included in this PR?

Synchronize the package inventory, generated dependency versions, lockfiles and changelog from v{candidate.version} using the existing version tool.

This PR is created with GITHUB_TOKEN, which does not trigger pull-request CI.
After reviewing it, close and reopen it with a maintainer account (or push an
update) to trigger the required checks before merging.

# Are there any user-facing changes?

No runtime behavior changes.

# AI Usage Statement

None. Generated by the repository release workflow.
"""
    return command(
        "gh",
        "pr",
        "create",
        "--repo",
        REPO,
        "--head",
        branch,
        "--base",
        "main",
        "--draft",
        "--title",
        f"chore: sync released versions for {candidate.version}",
        "--body-file",
        "-",
        data=body,
    )


def cleanup(candidate):
    for kind, name in (
        ("heads", f"releases/{candidate.version}"),
        ("tags", f"v{candidate.version}"),
    ):
        if ref_sha(kind, name) != candidate.sha:
            raise ValueError(
                "final publication refs no longer match the approved commit"
            )
    # Keep the approved branch discoverable until all other deletions succeed.
    for rc in sorted(candidates(), key=lambda rc: rc == candidate.rc):
        if RC.fullmatch(rc)[1] != candidate.version:
            continue
        # Every removed branch retains its own RC tag, including rejected candidates.
        old = Candidate.load(rc)
        command(
            "git",
            "-c",
            "credential.helper=!gh auth git-credential",
            "push",
            "origin",
            f"--force-with-lease=refs/heads/{old.branch}:{old.sha}",
            f":refs/heads/{old.branch}",
        )


def publish(candidate):
    release = candidate.atr()
    if release is None:
        raise ValueError(f"{candidate.rc} does not match the selected ATR revision")
    if not passed(release):
        raise ValueError("ATR has not resolved this vote as passed")
    revision = selected_revision(release)
    final_refs(candidate)
    body = announcement(candidate, release["version"])
    existing = api(f"repos/{REPO}/releases/tags/v{candidate.version}", optional=True)
    if existing and (existing["draft"] or existing["prerelease"]):
        raise ValueError("existing GitHub Release is still draft or prerelease")
    if not existing:
        api(
            f"repos/{REPO}/releases",
            {
                "tag_name": f"v{candidate.version}",
                "name": f"v{candidate.version}",
                "body": body + "\n" + version_notes(candidate),
                "draft": False,
                "prerelease": False,
                "generate_release_notes": True,
            },
        )
    if release["phase"] != "release":
        # This is the endpoint used by apache/tooling-actions/release-on-atr.
        # ATR validates publication and download propagation before sending mail.
        request_json(
            f"{ATR}/api/publisher/release/announce",
            {
                "publisher": "github",
                "jwt": atr_jwt(),
                "version": release["version"],
                "revision": revision,
                "email_to": "announce@apache.org",
                "body": body,
                "path_suffix": release.get("download_path_suffix") or None,
            },
        )
        release = candidate.atr()
        if (
            release is None
            or release["phase"] != "release"
            or selected_revision(release) != revision
        ):
            raise ValueError("ATR has not confirmed the announcement")
    discussion(
        f"[ANNOUNCE] Release Apache OpenDAL {candidate.version}", body, "Announcements"
    )
    sync_pr = sync_versions(candidate)
    current = notice(
        candidate,
        release,
        f"Apache OpenDAL {candidate.version} has been released. Version sync: {sync_pr}",
    )
    comment_once(
        current["id"],
        f"<!-- opendal-published:{candidate.rc} -->",
        f"[Apache OpenDAL {candidate.version}](https://github.com/{REPO}/releases/tag/v{candidate.version}) is available. "
        f"[Source branch](https://github.com/{REPO}/tree/releases/{candidate.version}). Version sync: {sync_pr}. RC tags are retained.",
    )
    # Language distributions are conveniences, not prerequisites for the ASF release.
    complete = True
    errors = []
    for distribute in (publish_builds, nexus_release):
        try:
            if not distribute(candidate):
                complete = False
        except (RuntimeError, ValueError, OSError) as error:
            complete = False
            errors.append(str(error))
    if not complete:
        status = (
            f"Apache OpenDAL {candidate.version} has been released. "
            "Optional package distribution "
        )
        status += (
            "needs attention: " + "; ".join(errors) if errors else "is still running."
        )
        print(status)
        notice(candidate, release, status)
        # Discovery uses RC branches; retain them until optional follow-up completes.
        return
    cleanup(candidate)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["notify", "sync", "publish", "baseline"])
    parser.add_argument("--rc")
    parser.add_argument("--status", help="Publication status for the candidate notice")
    args = parser.parse_args()
    if args.action == "baseline":
        releases = pages(f"repos/{REPO}/releases?per_page=100")
        final = [
            r
            for r in releases
            if not r["draft"]
            and not r["prerelease"]
            and re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+", r["tag_name"])
        ]
        if not final:
            raise ValueError("no published final GitHub release")
        print(max(final, key=lambda r: r["published_at"])["tag_name"])
    elif args.action == "sync":
        sync()
    else:
        candidate = Candidate.load(args.rc or "")
        if args.action == "notify":
            release = candidate.atr()
            if release is None:
                raise ValueError(
                    f"{candidate.rc} does not match the selected ATR revision"
                )
            notice(candidate, release, args.status)
        else:
            publish(candidate)


if __name__ == "__main__":
    main()
