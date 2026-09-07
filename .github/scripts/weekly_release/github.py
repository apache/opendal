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

"""GitHub candidate branches, version PRs and vote reminders."""

import base64
import os

from runtime import api, pages, run


def ref(repo, name):
    values = api(f"repos/{repo}/git/matching-refs/{name}")
    return next(
        (v["object"]["sha"] for v in values if v["ref"] == "refs/" + name), None
    )


def ensure_ref(repo, name, sha):
    existing = ref(repo, name)
    if existing:
        if existing != sha:
            raise ValueError(f"ref already names a different commit: {name}")
    else:
        api(f"repos/{repo}/git/refs", {"ref": "refs/" + name, "sha": sha})


def push(branch, cwd):
    # Git receives credentials through process environment, never argv or disk.
    auth = base64.b64encode(
        ("x-access-token:" + os.environ["GH_TOKEN"]).encode()
    ).decode()
    env = dict(
        os.environ,
        GIT_CONFIG_COUNT="1",
        GIT_CONFIG_KEY_0="http.https://github.com/.extraheader",
        GIT_CONFIG_VALUE_0="AUTHORIZATION: basic " + auth,
    )
    run("git", "push", "origin", f"HEAD:refs/heads/{branch}", cwd=cwd, env=env)


def pull_request(repo, head, base, title, body):
    matches = pages(f"repos/{repo}/pulls?state=all&head=apache:{head}&base={base}")
    if matches:
        return matches[0]
    return api(
        f"repos/{repo}/pulls",
        {"head": head, "base": base, "title": title, "body": body},
    )


def reminder(repo, record):
    # The version PR is already the candidate's GitHub entry point.
    prs = pages(
        f"repos/{repo}/pulls?state=closed&head=apache:{record['branch']}-bump&base={record['branch']}"
    )
    if not prs:
        raise ValueError("candidate version PR not found")
    pr = prs[0]
    link = f"https://releases.apache.org/vote/opendal/{record['atr_version']}"
    if link not in (pr["body"] or ""):
        body = (
            (pr["body"] or "")
            + f"\n\nFormal Trusted Vote: {link}\n\nVerify the candidate and cast your ballot in ATR. GitHub comments are discussion only."
        )
        api(f"repos/{repo}/pulls/{pr['number']}", {"body": body}, method="PATCH")
