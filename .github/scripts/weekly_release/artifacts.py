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

import hashlib
import io
import zipfile

from runtime import api, download, run


def expected_paths(record):
    return {
        f"{record['version']}/apache-opendal-{p.replace('/', '-')}-{v}-src.tar.gz{suffix}"
        for p, v in record["versions"].items()
        for suffix in ("", ".asc", ".sha512")
    }


def signed_checksums(record):
    endpoint = f"repos/apache/opendal/actions/runs/{record['compose_run']}/artifacts"
    artifacts = api(endpoint)["artifacts"]
    name = f"signed-source-{record['compose_run']}-{record['compose_attempt']}"
    matches = [a for a in artifacts if a["name"] == name and not a["expired"]]
    if len(matches) != 1:
        raise ValueError("missing immutable signed artifact")
    raw = run(
        "gh",
        "api",
        f"repos/apache/opendal/actions/artifacts/{matches[0]['id']}/zip",
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
