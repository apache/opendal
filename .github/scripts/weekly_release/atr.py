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

"""Read public ATR state and prepare the RM-owned vote request."""

import json
from urllib.error import HTTPError
from urllib.request import Request, urlopen


class ATR:
    def request(self, path):
        req = Request("https://releases.apache.org/api" + path)
        try:
            with urlopen(req, timeout=60) as response:
                return json.load(response)
        except HTTPError as error:
            if error.code == 404:
                return None
            raise RuntimeError(f"ATR {path} returned HTTP {error.code}") from None

    def release(self, version):
        data = self.request(f"/release/get/opendal/{version}")
        return data["release"] if data else None

    def checks(self, version, revision):
        result = []
        offset = 0
        while True:
            data = self.request(
                f"/checks/list/opendal/{version}/{revision}?limit=1000&offset={offset}"
            )
            if data is None:
                raise ValueError("ATR check revision not found")
            result.extend(data["checks"])
            offset += len(data["checks"])
            if offset >= data["count"]:
                return result
            if not data["checks"]:
                raise ValueError("ATR checks pagination made no progress")


def vote_payload(record, revision, concerns):
    rehearsal = record["dry_run"]
    label = "[REHEARSAL] " if rehearsal else ""
    body = (
        f"Hello OpenDAL community,\n\n{label}Please review OpenDAL {record['version']} "
        f"candidate {record['rc']}, ATR revision {revision}.\n\n"
        f"Candidate: https://releases.apache.org/vote/opendal/{record['atr_version']}\n"
        f"Source commit: {record['candidate_sha']}\n"
        "KEYS: https://downloads.apache.org/opendal/KEYS\n\n"
        "Independently rebuild and verify the source archives on trusted hardware. "
        "Record your verification evidence and cast your own ballot through ATR's "
        "website or API. GitHub comments and email replies are discussion only, "
        "not Trusted Vote ballots. The vote stays open for at least 72 hours.\n"
    )
    if rehearsal:
        body += "\nThis is a rehearsal only. No official publication is authorized.\n"
    else:
        body += "\nA passing vote authorizes ATR to publish these exact source files automatically.\n"
    return {
        "project": "opendal",
        "version": record["atr_version"],
        "revision": revision,
        "email_to": "dev@opendal.apache.org",
        "vote_duration": 72,
        "subject": f"[VOTE] {label}Release Apache OpenDAL {record['rc']}",
        "body": body,
        "concerns_noted": sorted(set(concerns)),
        "notify_when_finished": True,
        "automatic_resolve_when_finished": True,
        "automatic_publish_when_resolved": not rehearsal,
    }
