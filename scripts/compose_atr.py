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

"""Stage one complete signed candidate and bind its ATR revision to the Git RC."""

import argparse
import hashlib
import subprocess
import tempfile
import time
from pathlib import Path

from release_lifecycle import (
    ATR,
    REPO,
    Candidate,
    api,
    atr_jwt,
    atr_manifest,
    atr_release,
    candidates,
    ref_sha,
    request_json,
    selected_revision,
)


def signed_manifest(directory, version):
    files = {}
    for path in directory.iterdir():
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"Unexpected entry in the signed bundle: {path.name}")
        with path.open("rb") as stream:
            files[path.name] = (
                "sha3-256:" + hashlib.file_digest(stream, "sha3_256").hexdigest()
            )
    archives = {name for name in files if name.endswith("-src.tar.gz")}
    if f"apache-opendal-core-{version}-src.tar.gz" not in archives or set(files) != {
        name + suffix for name in archives for suffix in ("", ".asc", ".sha512")
    }:
        raise ValueError(
            "Expected signed source archives with both .asc and .sha512 sidecars"
        )
    return files


def upload(candidate, directory):
    with tempfile.TemporaryDirectory(prefix="od-atr-", dir="/tmp") as temporary:
        key = Path(temporary) / "key"
        subprocess.run(
            ["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", key], check=True
        )
        request_json(
            f"{ATR}/api/publisher/ssh/register",
            {
                "publisher": "github",
                "jwt": atr_jwt(),
                "ssh_key": key.with_suffix(".pub").read_text().strip(),
                "source_commit": candidate.sha,
            },
        )
        subprocess.run(
            [
                "rsync",
                "-av",
                "--delete",
                "-e",
                f"ssh -p 2222 -i {key} -o BatchMode=yes -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new",
                "--",
                f"{directory}/",
                f"github@releases.apache.org:/opendal/{candidate.version}/",
            ],
            check=True,
        )


def accepted_revision(candidate, files, previous):
    # ATR may finish archive validation after rsync exits. The old revision stays visible meanwhile.
    deadline = time.monotonic() + 20 * 60
    while time.monotonic() < deadline:
        release = atr_release(candidate.version, optional=True)
        if release is not None:
            if release["phase"] != "release_candidate_draft":
                raise ValueError(f"ATR {candidate.version} has left the compose phase")
            number = release.get("latest_revision_number")
            if (
                number
                and number != previous
                and release.get("commit_hash") == candidate.sha
            ):
                revision = selected_revision(release)
                remote = atr_manifest(candidate.version, revision)
                if remote == files:
                    return revision
                mismatches = sorted(
                    name
                    for name in files.keys() | remote
                    if files.get(name) != remote.get(name)
                )
                raise ValueError(
                    f"ATR revision {revision} differs from the signed bundle: {mismatches}"
                )
        time.sleep(10)
    raise TimeoutError(
        f"ATR has not accepted the signed bundle for {candidate.rc} at {candidate.sha}"
    )


def compose(candidate, directory):
    if ref_sha("tags", f"v{candidate.rc}") != candidate.sha:
        raise ValueError(f"Git tag v{candidate.rc} does not identify {candidate.sha}")
    files = signed_manifest(directory, candidate.version)
    if atr_release(candidate.rc, optional=True) is not None:
        raise ValueError(
            f"Finish or withdraw the existing ATR candidate {candidate.rc} before preparing a new Git RC"
        )
    release = atr_release(candidate.version, optional=True)
    previous = None
    revision = None
    if release is None:
        if ref_sha("tags", f"v{candidate.version}") is not None:
            raise ValueError(f"Git already has a final tag for {candidate.version}")
        for rc in candidates():
            if rc != candidate.rc and rc.startswith(f"{candidate.version}-rc."):
                legacy = atr_release(rc, optional=True)
                if legacy and legacy["phase"] != "release_candidate_draft":
                    raise ValueError(
                        f"Resolve the existing ATR release {rc} before starting {candidate.version}"
                    )
    else:
        if release["phase"] != "release_candidate_draft":
            locked = selected_revision(release)
            status = candidate.revision_status(locked, files)
            if (
                candidate.recorded_revision(status) == status
                and atr_manifest(candidate.version, locked) == files
            ):
                return
            raise ValueError(
                f"ATR {candidate.version} is in {release['phase']}; only a draft can be replaced"
            )
        previous = release.get("latest_revision_number")
        if (
            previous
            and release.get("commit_hash") == candidate.sha
            and atr_manifest(candidate.version, selected_revision(release)) == files
        ):
            revision = previous
    if revision is None:
        upload(candidate, directory)
        revision = accepted_revision(candidate, files, previous)
    status = candidate.revision_status(revision, files)
    recorded = candidate.recorded_revision(status)
    if recorded is None:
        api(f"repos/{REPO}/statuses/{candidate.sha}", status)
    elif recorded != status:
        raise ValueError(
            f"ATR revision {revision} already identifies a different candidate or manifest"
        )
    print(f"{candidate.rc} at {candidate.sha}: {status['target_url']}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("rc")
    parser.add_argument("sha")
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    compose(Candidate(args.rc, args.sha), args.directory)


if __name__ == "__main__":
    main()
