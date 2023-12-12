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


def check(directory):
    print(f"Checking dependencies of {directory}")
    subprocess.run(["cargo", "deny", "check", "license"], cwd=directory)


def main():
    base_dirs = ["bindings", "bin", "core", "integrations"]
    for base_dir in base_dirs:
        for root, dirs, files in os.walk(base_dir):
            if "Cargo.toml" in files:
                check(root)


if __name__ == "__main__":
    main()
