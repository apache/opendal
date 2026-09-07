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

"""GitHub release operations with deterministic identities for retry reconciliation."""

import base64
import os

from runtime import api, pages, run


def graphql(query, **variables):
    value = api("graphql", {"query": query, "variables": variables})
    if value.get("errors"):
        raise RuntimeError("GitHub GraphQL operation failed")
    return value["data"]


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
    title = f"[{'REHEARSAL' if record['dry_run'] else 'VOTE REMINDER'}] OpenDAL {record['rc']}"
    body = (
        f"The formal vote is on [ATR](https://releases.apache.org/vote/opendal/{record['atr_version']}).\n\n"
        f"Source commit: `{record['candidate_sha']}`. ATR revision: `{record['revision']}`.\n\n"
        "Please independently rebuild the source archives and verify signatures using OpenDAL KEYS. "
        "Use your own ATR account or API token to submit your ballot and verification evidence. "
        "Replies here are discussion, not formal ballots. ATR sends ballot receipts and the result to dev@opendal.apache.org.\n\n"
        "The vote runs for at least 72 hours from ATR's vote announcement. "
        + (
            "This rehearsal never publishes an official release."
            if record["dry_run"]
            else "ATR automatically publishes the approved source files after a passing vote."
        )
    )
    cursor = None
    while True:
        data = graphql(
            """query($cursor:String){repository(owner:"apache",name:"opendal"){
          id discussionCategories(first:100){nodes{id name}}
          discussions(first:100,after:$cursor,orderBy:{field:CREATED_AT,direction:DESC}){
            nodes{id title url} pageInfo{hasNextPage endCursor}}}}""",
            cursor=cursor,
        )["repository"]
        for item in data["discussions"]["nodes"]:
            if item["title"] == title:
                return item["url"]
        info = data["discussions"]["pageInfo"]
        if not info["hasNextPage"]:
            break
        cursor = info["endCursor"]
    category = next(
        c["id"] for c in data["discussionCategories"]["nodes"] if c["name"] == "General"
    )
    return graphql(
        """mutation($repo:ID!,$category:ID!,$title:String!,$body:String!){
      createDiscussion(input:{repositoryId:$repo,categoryId:$category,title:$title,body:$body}){
        discussion{url}}}""",
        repo=data["id"],
        category=category,
        title=title,
        body=body,
    )["createDiscussion"]["discussion"]["url"]
