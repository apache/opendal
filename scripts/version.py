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

from constants import PACKAGES, get_package_version


def increment_patch_version(version):
    parts = version.split(".")
    parts[-1] = str(int(parts[-1]) + 1)
    return ".".join(parts)


if __name__ == "__main__":
    print(f"| {'Name':<25} | {'Version':<7} | {'Next':<7} |")
    print(f"| {'-':<25} | {'-':<7} | {'-':<7} |")
    for v in PACKAGES:
        cur = get_package_version(v)
        next = increment_patch_version(cur)

        print(f"| {str(v):<25} | {cur:<7} | {next:<7} |")
