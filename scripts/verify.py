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


import subprocess
import sys
import os

BASE_DIR = os.getcwd()


def check_rust():
    try:
        subprocess.run(["cargo", "--version"], check=True)
        return True
    except FileNotFoundError:
        return False
    except Exception as e:
        raise Exception("Check rust met unexpected error", e)


def check_java():
    try:
        subprocess.run(["java", "--version"], check=True)
        return True
    except FileNotFoundError:
        return False
    except Exception as e:
        raise Exception("Check java met unexpected error", e)


def build_core():
    print("Start building opendal core")

    subprocess.run(["cargo", "build", "--release"], cwd="core", check=True)


def build_java_binding():
    print("Start building opendal java binding")

    subprocess.run(
        [
            "./mvnw",
            "clean",
            "install",
            "-DskipTests=true",
            "-Dcargo-build.profile=release",
        ],
        check=True,
        cwd="bindings/java",
    )


def main():
    if not check_rust():
        print(
            "Cargo is not found, please check if rust development has been setup correctly"
        )
        print("Visit https://www.rust-lang.org/tools/install for more information")
        sys.exit(1)

    build_core()

    if check_java():
        build_java_binding()
    else:
        print("Java is not found, skipped building java binding")


if __name__ == "__main__":
    main()
