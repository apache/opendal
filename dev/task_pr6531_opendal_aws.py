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

"""Task-only actual AWS multipart SSE comparison; never modifies bucket or KMS configuration."""

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
    result_dir = Path(os.environ["RUNNER_TEMP"]) / "task-s3-multipart-sse"
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
        # Match S3Builder's existing template for the AWS global endpoint.
        summary["endpoint_template_applied"] = endpoint.rstrip("/") == "https://s3.amazonaws.com"
        if summary["endpoint_template_applied"]:
            endpoint = f"https://s3.{region}.amazonaws.com"
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
        prefix = f"task-s3-multipart-sse-{uuid.uuid4()}"
        summary["owned_prefix"] = prefix
        payload = b"OpenDAL actual AWS multipart SSE-KMS request scope\n"

        def request(operation, suffix, *args, expected_absent=False):
            key = "/".join(part for part in (root, suffix) if part)
            key_args = ["--prefix", key] if operation == "list-multipart-uploads" else ["--key", key]
            proc = subprocess.run(
                ["aws", "s3api", operation, "--bucket", bucket, *key_args,
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

        def cleanup_item(item, counter):
            try:
                if item.get("multipart_pending"):
                    key = "/".join(part for part in (root, item["suffix"]) if part)
                    uploads = request("list-multipart-uploads", item["suffix"])
                    for upload in uploads.get("Uploads", []):
                        if upload["Key"] == key:
                            request("abort-multipart-upload", item["suffix"],
                                    "--upload-id", upload["UploadId"])
                    remaining = request("list-multipart-uploads", item["suffix"])
                    assert not any(x["Key"] == key for x in remaining.get("Uploads", []))
                    item["multipart_pending"] = False
                if not item["put_confirmed"]:
                    try:
                        discovered = request("head-object", item["suffix"])
                    except AwsFailure as failure:
                        if failure.code in ("404", "NotFound", "NoSuchKey", "NoSuchVersion"):
                            summary[counter] = summary.get(counter, 0) + 1
                            return True
                        raise
                    item["version"] = discovered.get("VersionId")
                assert isinstance(item["version"], str) and item["version"], "Exact version required"
                selector = ["--version-id", item["version"]]
                request("delete-object", item["suffix"], *selector)
                request("head-object", item["suffix"], *selector, expected_absent=True)
                summary[counter] = summary.get(counter, 0) + 1
                return True
            except Exception as cleanup_error:
                pending.append(item)
                summary.setdefault("cleanup_errors", []).append(
                    {"type": type(cleanup_error).__name__,
                     "code": getattr(cleanup_error, "code", "ASSERTION")}
                )
                return False

        binary = Path(__file__).resolve().parents[1] / "core" / "target" / "debug" / "examples" / "task_pr6531_opendal"
        assert binary.is_file(), "Prebuilt task binary required"
        rust_env = os.environ.copy()
        rust_env.pop("AWS_REGION", None)
        rust_env.pop("AWS_DEFAULT_REGION", None)
        for name in ("RUST_LOG", "RUST_BACKTRACE"):
            rust_env.pop(name, None)
        summary["explicit_region_without_aws_region_env"] = True

        with tempfile.TemporaryDirectory(prefix="opendal-pr6531-") as temp:
            try:
                for label, mode in (("upload", "multipart"), ("copy", "multipart_copy")):
                    if mode == "multipart_copy" and not summary["cases"][-1]["passed"]:
                        summary["cases"].append({"case": label, "mode": mode,
                                                 "passed": False, "executed": False,
                                                 "skip_reason": "source_preparation_failed"})
                        continue
                    suffix = f"{prefix}/{label}"
                    source = f"{prefix}/upload" if mode == "multipart_copy" else ""
                    expected = b"z" * (5 * 1024 * 1024) + payload
                    item = {"suffix": suffix, "version": None, "put_confirmed": False,
                            "multipart_pending": True}
                    owned.append(item)
                    try:
                        proc = subprocess.run([str(binary), mode, suffix, source],
                                              env=rust_env, capture_output=True, text=True,
                                              check=False, timeout=180)
                        if proc.returncode:
                            failure = RuntimeError("OpenDAL task process failed")
                            failure.operation = f"opendal-{mode}"
                            kind = re.search(r"OPENDAL_ERROR kind=([A-Za-z]+)", proc.stderr)
                            failure.code = kind.group(1) if kind else "RUST_PROCESS_FAILURE"
                            failure.diagnostic = dict(re.findall(
                                r"\b(stage|s3_operation|http_status|s3_code|mentions_sse|mentions_bucket_key)=([A-Za-z0-9_-]+)", proc.stderr))
                            raise failure
                        marker, separator, returned_version = proc.stdout.rstrip("\n").partition("\t")
                        assert marker == "VERSION" and separator == "\t" and "\n" not in returned_version
                        item["multipart_pending"] = False
                        discovered = request("head-object", suffix)
                        version_id = discovered.get("VersionId")
                        assert isinstance(version_id, str) and version_id, "Versioned AWS fixture required"
                        item.update(version=version_id, put_confirmed=True)
                        if returned_version:
                            assert returned_version == version_id, "OpenDAL version differs from AWS"
                        selector = ["--version-id", version_id]
                        head = request("head-object", suffix, *selector)
                        assert head.get("VersionId") == version_id
                        assert head.get("ServerSideEncryption") == "aws:kms"
                        assert head.get("ContentLength") == len(expected)
                        if mode in ("multipart", "multipart_copy"):
                            assert head.get("ETag", "").strip('"').endswith("-2"), "Two-part AWS upload required"
                        observed = head.get("BucketKeyEnabled", False)
                        assert isinstance(observed, bool)
                        destination = Path(temp) / label
                        request("get-object", suffix, *selector, str(destination))
                        assert destination.read_bytes() == expected
                        case = {"case": label, "mode": mode, "passed": True, "executed": True, "bucket_key": observed,
                                "bytes_equal": True, "version_selected": True,
                                "opendal_version_returned": bool(returned_version)}
                        if mode in ("multipart", "multipart_copy"):
                            case["two_parts_confirmed"] = True
                        summary["cases"].append(case)
                        print("CASE " + json.dumps(case), flush=True)
                    except Exception as case_error:
                        case = {"case": label, "mode": mode, "passed": False, "executed": True,
                                "error": {"type": type(case_error).__name__,
                                          "operation": getattr(case_error, "operation", None),
                                          "code": getattr(case_error, "code", "VALIDATION_ERROR")},
                                "diagnostic": getattr(case_error, "diagnostic", {})}
                        summary["cases"].append(case)
                        print("CASE " + json.dumps(case), flush=True)
            finally:
                for item in reversed(owned):
                    cleanup_item(item, "cleanup_verified")
        assert len(summary["cases"]) == 2 and summary["cleanup_verified"] == len(owned) and not pending
        assert all(case["passed"] for case in summary["cases"]), "Multipart SSE validation has failed or unexecuted operations"
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
