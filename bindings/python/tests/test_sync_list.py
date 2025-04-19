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

from uuid import uuid4

import pytest


@pytest.fixture(scope="function")
def test_list_dir(operator):
    """Provides a temporary directory with files and a subdirectory for list tests."""
    dir_name = f"list_dir_{str(uuid4())}/"
    files = sorted([f"{dir_name}file_{i}_{str(uuid4())}" for i in range(5)])
    # Create files for testing
    for f in files:
        operator.write(f, b"test_content")
    # Create a subdirectory
    subdir_name = f"{dir_name}subdir_{str(uuid4())}/"
    operator.create_dir(subdir_name)
    files.append(subdir_name)  # Add subdir to expected non-recursive list
    files.sort()

    yield dir_name, files

    # Cleanup
    operator.remove_all(dir_name)


@pytest.mark.need_capability(
    "list", "write", "create_dir", "delete", "list_with_start_after"
)
def test_sync_list_with_start_after(operator, test_list_dir):
    """Tests listing starting after a specific file."""
    dir_name, files = test_list_dir
    start_after_index = 2
    start_after_path = files[start_after_index]

    entries = list(operator.list(dir_name, start_after=start_after_path))
    entry_paths = sorted(
        [e.path for e in entries if e.path != dir_name and e.path != start_after_path]
    )

    expected_paths = sorted(files[start_after_index + 1 :])

    assert entry_paths == expected_paths, (
        f"Expected {expected_paths}, but got {entry_paths}"
    )


@pytest.mark.need_capability(
    "list", "write", "create_dir", "delete", "list_with_start_after"
)
def test_sync_list_with_start_after_non_existent(operator, test_list_dir):
    """Tests listing starting after a non-existent file."""
    dir_name, files = test_list_dir
    start_after_path = f"{dir_name}non_existent_file_{str(uuid4())}"

    # Behavior might vary; assert it doesn't error and returns a list.
    try:
        entries = list(operator.list(dir_name, start_after=start_after_path))
        assert isinstance(entries, list)  # Check it returns a list without erroring
    except Exception as e:
        pytest.fail(f"Listing with non-existent start_after raised an exception: {e}")


@pytest.mark.need_capability(
    "list", "write", "create_dir", "delete", "list_with_start_after"
)
def test_sync_list_with_start_after_last(operator, test_list_dir):
    """Tests listing starting after the last item in the directory."""
    dir_name, files = test_list_dir
    start_after_path = files[-1]  # Start after the last item

    entries = list(operator.list(dir_name, start_after=start_after_path))
    # Exclude the base directory from results
    entry_paths = [e.path for e in entries if e.path != dir_name]

    assert entry_paths == [], (
        f"Expected empty list when starting after last item, but got {entry_paths}"
    )
