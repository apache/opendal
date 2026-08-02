# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import tempfile
from pathlib import Path

import opendal


def main() -> None:
    try:
        opendal.Operator("s3", bucket="prototype-bucket")
    except opendal.RuntimeProtocolError as err:
        assert "import its package first" in str(err)
    else:
        raise AssertionError("S3 must not be registered before its package import")

    import opendal.services.s3 as s3

    assert s3.MANIFEST["component"]["id"] == "s3"
    s3_library = Path(s3.__file__).parent / "_native" / "libs3_extension.so"
    held_library = s3_library.with_suffix(".so.held")
    s3_library.rename(held_library)
    try:
        try:
            opendal.Operator("s3", bucket="prototype-bucket")
        except opendal.RuntimeProtocolError:
            pass
        else:
            raise AssertionError("import must not load the S3 native artifact")
    finally:
        held_library.rename(s3_library)

    with opendal.Operator(
        "s3", bucket="prototype-bucket", region="us-east-1"
    ) as operator:
        assert operator.info == {
            "scheme": "s3",
            "name": "prototype-bucket",
            "root": "/",
        }

    import opendal.services.fs as fs

    assert fs.MANIFEST["component"]["id"] == "fs"
    with tempfile.TemporaryDirectory() as root:
        with opendal.Operator("fs", root=root) as operator:
            operator.write("hello.txt", b"hello from the shared runtime")
            assert operator.read("hello.txt") == b"hello from the shared runtime"
            assert Path(root, "hello.txt").read_bytes() == b"hello from the shared runtime"
            assert operator.info["scheme"] == "fs"

    print("imported s3 and fs service packages")
    print("deferred native service loading until operator construction")
    print("constructed S3 operator and completed FS write/read through runtime handles")


if __name__ == "__main__":
    main()
