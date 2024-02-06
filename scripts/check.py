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
import os

# Define colors for output
YELLOW = "\033[37;1m"
GREEN = "\033[32;1m"
ENDCOLOR = "\033[0m"


def check_signature(pkg):
    """Check the GPG signature of the package."""
    try:
        subprocess.check_call(["gpg", "--verify", f"{pkg}.asc", pkg])
        print(GREEN + "Success to verify the gpg sign for " + pkg + ENDCOLOR)
    except subprocess.CalledProcessError:
        print(YELLOW + "Failed to verify the gpg sign for " + pkg + ENDCOLOR)


def check_sha512sum(pkg):
    """Check the sha512 checksum of the package."""
    try:
        subprocess.check_call(["sha512sum", "--check", f"{pkg}.sha512"])
        print(GREEN + "Success to verify the checksum for " + pkg + ENDCOLOR)
    except subprocess.CalledProcessError:
        print(YELLOW + "Failed to verify the checksum for " + pkg + ENDCOLOR)


def main():
    # Get a list of all files in the current directory
    files = [f for f in os.listdir(".") if os.path.isfile(f)]

    for pkg in files:
        # Skip files that don't have a corresponding .asc or .sha512 file
        if not os.path.exists(f"{pkg}.asc") or not os.path.exists(f"{pkg}.sha512"):
            continue

        print(f"> Checking {pkg}")

        # Perform the checks
        check_signature(pkg)
        check_sha512sum(pkg)


if __name__ == "__main__":
    main()
