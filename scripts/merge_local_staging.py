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

import os
import shutil
import sys


def copy_and_append_index(target_directory, dir_path):
    sub_dir_name = os.path.basename(os.listdir(dir_path)[0])
    sub_dir_path = os.path.join(dir_path, sub_dir_name)

    # Create target subdirectory if it doesn't exist
    target_sub_dir = os.path.join(target_directory, sub_dir_name)
    os.makedirs(target_sub_dir, exist_ok=True)

    # Append contents of .index file
    with open(os.path.join(sub_dir_path, ".index"), "r") as index_file:
        with open(os.path.join(target_sub_dir, ".index"), "a") as target_index_file:
            target_index_file.write(index_file.read())

    # Copy contents from source subdirectory to target subdirectory
    for item in os.listdir(sub_dir_path):
        s = os.path.join(sub_dir_path, item)
        d = os.path.join(target_sub_dir, item)
        if os.path.isdir(s):
            shutil.copytree(s, d, dirs_exist_ok=True)
        else:
            shutil.copy2(s, d)


if len(sys.argv) < 3:
    print("Expected target directory and at least one local staging directory")
    sys.exit(1)

target_directory = sys.argv[1]

# Loop through each provided directory argument
for i in range(2, len(sys.argv)):
    dir_path = sys.argv[i]
    copy_and_append_index(target_directory, dir_path)
