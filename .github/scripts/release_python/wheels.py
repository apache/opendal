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

"""Identify shared GCC runtimes added to wheels by maturin's auditwheel repair."""

import argparse
import base64
import csv
import hashlib
import io
import json
import re
import tempfile
import zipfile
from pathlib import Path

LICENSE_EXPRESSION = "GPL-3.0-or-later WITH GCC-exception-3.1"
LICENSE_DIR = Path(__file__).with_name("licenses")
LICENSE_FILES = ("LICENSE-GPL-3.0.txt", "LICENSE-GCC-exception-3.1.txt")
RUNTIME_PATTERN = re.compile(r"opendal\.libs/libgcc_s(?:-[^.]+)?\.so\.1")
RUNTIME_NOTICE = """Bundled GCC runtime
===================

This wheel contains libgcc_s, the GCC runtime library, copied into opendal.libs
by maturin's auditwheel repair. It is licensed under GPL-3.0-or-later WITH
GCC-exception-3.1. See LICENSE-GPL-3.0.txt and LICENSE-GCC-exception-3.1.txt.

Homepage and source: https://gcc.gnu.org/
The wheel's CycloneDX SBOM records each bundled runtime's path and SHA-256.
The runtime is a separate shared library loaded by the Python extension.
"""


def complete_wheel(wheel: Path) -> bool:
    with zipfile.ZipFile(wheel) as archive:
        infos = {info.filename: info for info in archive.infolist()}
        bundled = sorted(
            name
            for name in infos
            if name.startswith("opendal.libs/") and not name.endswith("/")
        )
        if not bundled:
            return False
        if any(not RUNTIME_PATTERN.fullmatch(name) for name in bundled):
            raise ValueError(f"{wheel}: unrecognized bundled libraries: {bundled}")
        contents = {name: archive.read(name) for name in infos}

    (metadata_name,) = [
        name for name in contents if name.endswith(".dist-info/METADATA")
    ]
    dist_info = metadata_name.rsplit("/", 1)[0]
    sbom_name = f"{dist_info}/sboms/opendal-python.cyclonedx.json"
    sbom = json.loads(contents[sbom_name])
    if sbom.get("bomFormat") != "CycloneDX":
        raise ValueError(f"{wheel}: expected a CycloneDX SBOM")
    root_ref = sbom["metadata"]["component"]["bom-ref"]
    components = sbom.setdefault("components", [])
    dependencies = sbom.setdefault("dependencies", [])
    root_dependency = next((d for d in dependencies if d["ref"] == root_ref), None)
    if root_dependency is None:
        root_dependency = {"ref": root_ref, "dependsOn": []}
        dependencies.append(root_dependency)
    root_dependencies = root_dependency.setdefault("dependsOn", [])
    for name in bundled:
        digest = hashlib.sha256(contents[name]).hexdigest()
        ref = f"urn:sha256:{digest}"
        if not any(component.get("bom-ref") == ref for component in components):
            components.append(
                {
                    "type": "library",
                    "bom-ref": ref,
                    "name": "libgcc_s",
                    "scope": "required",
                    "licenses": [{"expression": LICENSE_EXPRESSION}],
                    "hashes": [{"alg": "SHA-256", "content": digest}],
                    "externalReferences": [
                        {"type": "website", "url": "https://gcc.gnu.org/"}
                    ],
                    "properties": [{"name": "opendal:wheel:path", "value": name}],
                }
            )
        if ref not in root_dependencies:
            root_dependencies.append(ref)
        if not any(dependency["ref"] == ref for dependency in dependencies):
            dependencies.append({"ref": ref, "dependsOn": []})
    contents[sbom_name] = (json.dumps(sbom, indent=2) + "\n").encode()

    for filename in LICENSE_FILES:
        contents[f"{dist_info}/licenses/{filename}"] = (
            LICENSE_DIR / filename
        ).read_bytes()
    contents[f"{dist_info}/licenses/LIBGCC-NOTICE"] = RUNTIME_NOTICE.encode()
    header, separator, body = contents[metadata_name].decode().partition("\n\n")
    if not separator:
        raise ValueError(f"{wheel}: METADATA has no description separator")
    for filename in (*LICENSE_FILES, "LIBGCC-NOTICE"):
        field = f"License-File: {filename}"
        if field not in header.splitlines():
            header += "\n" + field
    if LICENSE_EXPRESSION not in header:
        header, count = re.subn(
            r"(?m)^License-Expression: (.+)$",
            lambda match: (
                f"License-Expression: ({match[1]}) AND ({LICENSE_EXPRESSION})"
            ),
            header,
        )
        if count != 1:
            raise ValueError(f"{wheel}: expected one License-Expression")
    if RUNTIME_NOTICE not in body:
        body += "\n\n" + RUNTIME_NOTICE
    contents[metadata_name] = (header + separator + body).encode()

    # Repair changes wheel metadata, so every resulting byte must be covered by RECORD.
    record_name = f"{dist_info}/RECORD"
    record = io.StringIO(newline="")
    writer = csv.writer(record, lineterminator="\n")
    for name, content in contents.items():
        if name != record_name and not name.endswith("/"):
            digest = base64.urlsafe_b64encode(hashlib.sha256(content).digest()).rstrip(
                b"="
            )
            writer.writerow((name, "sha256=" + digest.decode(), len(content)))
    writer.writerow((record_name, "", ""))
    contents[record_name] = record.getvalue().encode()

    with tempfile.TemporaryDirectory(dir=wheel.parent) as tmpdir:
        output = Path(tmpdir) / wheel.name
        with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for name, content in contents.items():
                info = infos.get(name)
                if info is None:
                    info = zipfile.ZipInfo(name, infos[metadata_name].date_time)
                    info.compress_type = zipfile.ZIP_DEFLATED
                    info.external_attr = 0o100644 << 16
                archive.writestr(info, content)
        output.replace(wheel)
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    wheels = sorted(args.directory.glob("*.whl"))
    if not wheels:
        parser.error(f"no wheels found in {args.directory}")
    for wheel in wheels:
        if complete_wheel(wheel):
            print(f"Identified bundled GCC runtime in {wheel.name}")


if __name__ == "__main__":
    main()
