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


# copy the content from staging_directory to target_directory
# and append the content of .index file.
def copy_and_append_index(target_directory, staging_directory):
    # Process all subdirectories in the staging directory
    for sub_dir_name in os.listdir(staging_directory):
        sub_dir_path = os.path.join(staging_directory, sub_dir_name)

        # Skip if it's not a directory
        if not os.path.isdir(sub_dir_path):
            continue

        # Create target subdirectory if it doesn't exist
        target_sub_dir = os.path.join(target_directory, sub_dir_name)
        os.makedirs(target_sub_dir, exist_ok=True)

        # Append contents of .index file if it exists
        index_file_path = os.path.join(sub_dir_path, ".index")
        if os.path.isfile(index_file_path):
            with open(index_file_path, "r") as index_file:
                with open(os.path.join(target_sub_dir, ".index"), "a") as target_index_file:
                    print(f"Appending {index_file_path} to {target_sub_dir}.index")
                    target_index_file.write(index_file.read())

        # Copy contents from source subdirectory to target subdirectory
        for item in os.listdir(sub_dir_path):
            source_item = os.path.join(sub_dir_path, item)
            destination_item = os.path.join(target_sub_dir, item)
            print(f"Copying {source_item} to {destination_item}")
            if os.path.isdir(source_item):
                shutil.copytree(source_item, destination_item, dirs_exist_ok=True)
            else:
                shutil.copy2(source_item, destination_item)


if len(sys.argv) < 3:
    print("Expected target directory and at least one local staging directory")
    sys.exit(1)

target_directory = sys.argv[1]

# Loop through each provided directory argument
for dir_path in sys.argv[2:]:
    if not os.path.isdir(dir_path):
        print(f"{dir_path} is not a valid directory.")
        sys.exit(1)
    copy_and_append_index(target_directory, dir_path)
