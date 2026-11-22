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

from argparse import ArgumentParser
from hashlib import file_digest
from pathlib import Path
import shutil
from zipfile import BadZipFile, ZipFile

from build import get_cargo_artifact_name


CLASSIFIERS = (
    "linux-x86_64",
    "linux-x86_64-musl",
    "linux-aarch_64",
    "linux-aarch_64-musl",
    "osx-x86_64",
    "osx-aarch_64",
    "windows-x86_64",
)

LICENSE_FILES = (
    "META-INF/DEPENDENCIES",
    "META-INF/LICENSE",
    "META-INF/LICENSE-MPL-2.0.txt",
    "META-INF/NOTICE",
)


def platform_libraries(inputs, artifact):
    for classifier in CLASSIFIERS:
        jar = inputs / f"{artifact}-{classifier}.jar"
        library = f"native/{classifier}/{get_cargo_artifact_name(classifier)}"
        yield jar, library


def stage(inputs, artifact, classes, output):
    if inputs is None or not inputs.is_dir():
        raise ValueError(f"Set -Dall.natives.dir to a directory of platform JARs: {inputs or '(unset)'}")
    if output.exists():
        shutil.rmtree(output)
    shutil.copytree(
        classes, output, ignore=lambda directory, _: {"native"} if Path(directory) == classes else set()
    )

    for jar, library in platform_libraries(inputs, artifact):
        if not jar.is_file():
            raise ValueError(f"Missing platform JAR: {jar}")
        with ZipFile(jar) as archive:
            entries = [entry for entry in archive.infolist() if entry.filename == library]
            if len(entries) != 1 or entries[0].file_size == 0:
                raise ValueError(f"{jar}: expected one non-empty native library at {library}")
            target = output / library
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(entries[0]) as source, target.open("wb") as destination:
                shutil.copyfileobj(source, destination)


def native_entries(archive):
    return sorted(
        entry.filename
        for entry in archive.infolist()
        if entry.filename.startswith("native/") and not entry.is_dir()
    )


def verify(inputs, base_jar, all_jar):
    libraries = list(platform_libraries(inputs, base_jar.stem))
    with ZipFile(base_jar) as base, ZipFile(all_jar) as combined:
        if native_entries(base):
            raise ValueError(f"{base_jar}: unexpected native libraries")
        if native_entries(combined) != sorted(library for _, library in libraries):
            raise ValueError(f"{all_jar}: expected exactly the seven platform libraries")

        base_entries = set(base.namelist())
        combined_entries = set(combined.namelist())
        classes = {name for name in base_entries if name.endswith(".class")}
        if "org/apache/opendal/NativeLibrary.class" not in classes:
            raise ValueError(f"{base_jar}: missing NativeLibrary.class")
        if classes != {name for name in combined_entries if name.endswith(".class")}:
            raise ValueError(f"{all_jar}: compiled classes differ from {base_jar}")
        for name in LICENSE_FILES:
            if name not in base_entries or name not in combined_entries:
                raise ValueError(f"Expected {name} in both {base_jar} and {all_jar}")
        for name in sorted(classes) + list(LICENSE_FILES):
            if base.read(name) != combined.read(name):
                raise ValueError(f"{all_jar}: {name} differs from {base_jar}")

        for jar, library in libraries:
            with ZipFile(jar) as platform:
                if native_entries(platform) != [library]:
                    raise ValueError(f"{jar}: expected only {library}")
                with platform.open(library) as original, combined.open(library) as bundled:
                    digest = file_digest(original, "sha256").hexdigest()
                    if digest != file_digest(bundled, "sha256").hexdigest():
                        raise ValueError(f"{all_jar}: {library} differs from {jar}")
                    print(f"{digest}  {library}")


def main():
    parser = ArgumentParser(description="Stage or verify the OpenDAL all-natives JAR")
    commands = parser.add_subparsers(dest="command", required=True)
    stage_parser = commands.add_parser("stage")
    stage_parser.add_argument("--inputs")
    stage_parser.add_argument("artifact")
    stage_parser.add_argument("classes", type=Path)
    stage_parser.add_argument("output", type=Path)
    verify_parser = commands.add_parser("verify")
    verify_parser.add_argument("inputs", type=Path)
    verify_parser.add_argument("base_jar", type=Path)
    verify_parser.add_argument("all_jar", type=Path)
    args = parser.parse_args()

    try:
        if args.command == "stage":
            inputs = Path(args.inputs) if args.inputs else None
            stage(inputs, args.artifact, args.classes, args.output)
        else:
            verify(args.inputs, args.base_jar, args.all_jar)
    except (BadZipFile, OSError, ValueError) as error:
        parser.exit(1, f"all-natives: {error}\n")


if __name__ == "__main__":
    main()
