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

"""Task-only AWS provider proof; never modifies bucket or KMS configuration."""

import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
from urllib.parse import urlsplit
import uuid


class AwsFailure(Exception):
    def __init__(self, operation, stderr):
        match = re.search(r"An error occurred \(([A-Za-z0-9_.-]+)\)", stderr)
        self.operation = operation
        self.code = match.group(1) if match else "CLI_ERROR"


def main():
    result_dir = Path(os.environ["RUNNER_TEMP"]) / "task-pr6531"
    result_dir.mkdir(parents=True, exist_ok=False)
    summary = {"cases": [], "cleanup_verified": 0, "passed": False}
    owned = []
    pending = []
    error = None
    try:
        summary["source_head"] = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip()
        bucket = os.environ["OPENDAL_S3_BUCKET"]
        region = os.environ["OPENDAL_S3_REGION"]
        endpoint = os.environ.get("OPENDAL_S3_ENDPOINT") or (
            f"https://s3.{region}.amazonaws.com"
        )
        uri = urlsplit(endpoint)
        assert uri.scheme == "https" and "@" not in uri.netloc
        assert uri.hostname and (
            uri.hostname.endswith(".amazonaws.com")
            or uri.hostname.endswith(".amazonaws.com.cn")
        ), "AWS endpoint required"
        assert uri.path in ("", "/") and not uri.query and not uri.fragment
        child_env = os.environ.copy()
        child_env.update(
            AWS_ACCESS_KEY_ID=os.environ["OPENDAL_S3_ACCESS_KEY_ID"],
            AWS_SECRET_ACCESS_KEY=os.environ["OPENDAL_S3_SECRET_ACCESS_KEY"],
            AWS_REGION=region,
            AWS_DEFAULT_REGION=region,
            AWS_EC2_METADATA_DISABLED="true",
            AWS_MAX_ATTEMPTS="1",
            AWS_PAGER="",
            AWS_CLI_AUTO_PROMPT="off",
        )
        token = os.environ.get("OPENDAL_S3_SESSION_TOKEN") or os.environ.get(
            "OPENDAL_S3_SECURITY_TOKEN"
        )
        if token:
            child_env["AWS_SESSION_TOKEN"] = token
        else:
            child_env.pop("AWS_SESSION_TOKEN", None)
        version = subprocess.check_output(
            ["aws", "--version"], env=child_env, text=True, stderr=subprocess.STDOUT
        ).strip()
        assert version.startswith("aws-cli/2."), "AWS CLI v2 required"
        summary["aws_cli"] = version
        root = os.environ.get("OPENDAL_S3_ROOT", "").strip("/")
        prefix = f"pr6531-bucket-key-{uuid.uuid4()}"
        summary["owned_prefix"] = prefix
        payload = b"OpenDAL PR6531 actual AWS SSE-KMS bucket-key proof\n"

        def request(operation, suffix, *args, expected_absent=False):
            key = "/".join(part for part in (root, suffix) if part)
            proc = subprocess.run(
                ["aws", "s3api", operation, "--bucket", bucket, "--key", key,
                 "--region", region, "--endpoint-url", endpoint,
                 "--output", "json", *args],
                env=child_env, capture_output=True, text=True, check=False, timeout=120,
            )
            if proc.returncode:
                failure = AwsFailure(operation, proc.stderr)
                if expected_absent and failure.code in ("404", "NotFound", "NoSuchKey", "NoSuchVersion"):
                    return None
                raise failure
            if expected_absent:
                raise AssertionError("Deleted task object/version remains readable")
            return json.loads(proc.stdout or "{}")

        with tempfile.TemporaryDirectory(prefix="opendal-pr6531-") as temp:
            body = Path(temp) / "payload"
            body.write_bytes(payload)
            try:
                for label, value in (("omitted", None), ("false", False), ("true", True)):
                    suffix = f"{prefix}/{label}"
                    flags = [] if value is None else [
                        "--bucket-key-enabled" if value else "--no-bucket-key-enabled"
                    ]
                    item = {"suffix": suffix, "version": None, "put_confirmed": False}
                    owned.append(item)
                    put = request("put-object", suffix, "--body", str(body),
                                  "--server-side-encryption", "aws:kms", *flags)
                    version_id = put.get("VersionId")
                    item.update(version=version_id, put_confirmed=True)
                    selector = ["--version-id", version_id] if version_id is not None else []
                    head = request("head-object", suffix, *selector)
                    if version_id is not None:
                        assert head.get("VersionId") == version_id
                    assert head.get("ServerSideEncryption") == "aws:kms"
                    assert head.get("ContentLength") == len(payload)
                    observed = head.get("BucketKeyEnabled", False)
                    assert isinstance(observed, bool)
                    if value is not None:
                        assert observed is value, "Object-level Bucket Key value differs"
                    destination = Path(temp) / label
                    request("get-object", suffix, *selector, str(destination))
                    assert destination.read_bytes() == payload
                    case = {"case": label, "bucket_key": observed,
                            "bytes_equal": True, "version_selected": version_id is not None}
                    summary["cases"].append(case)
                    print("CASE " + json.dumps(case), flush=True)
            finally:
                for item in reversed(owned):
                    try:
                        if not item["put_confirmed"]:
                            try:
                                discovered = request("head-object", item["suffix"])
                            except AwsFailure as failure:
                                if failure.code in ("404", "NotFound", "NoSuchKey", "NoSuchVersion"):
                                    summary["cleanup_verified"] += 1
                                    continue
                                raise
                            item["version"] = discovered.get("VersionId")
                        selector = ["--version-id", item["version"]] if item["version"] is not None else []
                        request("delete-object", item["suffix"], *selector)
                        request("head-object", item["suffix"], *selector, expected_absent=True)
                        summary["cleanup_verified"] += 1
                    except Exception as cleanup_error:
                        pending.append(item)
                        summary.setdefault("cleanup_errors", []).append(
                            {"type": type(cleanup_error).__name__,
                             "code": getattr(cleanup_error, "code", "ASSERTION")}
                        )
        assert len(summary["cases"]) == 3 and not pending
        summary["passed"] = True
    except Exception as exc:
        error = exc
        summary["error"] = {"type": type(exc).__name__,
                            "operation": getattr(exc, "operation", None),
                            "code": getattr(exc, "code", "VALIDATION_ERROR")}
    finally:
        (result_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        if pending:
            (result_dir / "cleanup.json").write_text(json.dumps(pending, indent=2) + "\n")
        print("RESULT " + json.dumps(summary), flush=True)
    return 1 if error or pending else 0


if __name__ == "__main__":
    raise SystemExit(main())
