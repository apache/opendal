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

import ast
from datetime import datetime
from pathlib import Path

import pytest

import opendal
from opendal.types import EntryMode

STUBS = Path(__file__).resolve().parents[1] / "python" / "opendal"

# Every `Capability` property as `capability.pyi` declared it before
# `Capability` was generated from core. Existing code relies on each name and
# type, so the generated class must keep them all.
HISTORICAL_CAPABILITY = {
    "copy": "bool",
    "copy_with_if_not_exists": "bool",
    "create_dir": "bool",
    "delete": "bool",
    "delete_with_recursive": "bool",
    "delete_with_version": "bool",
    "list": "bool",
    "list_with_deleted": "bool",
    "list_with_limit": "bool",
    "list_with_recursive": "bool",
    "list_with_start_after": "bool",
    "list_with_versions": "bool",
    "presign": "bool",
    "presign_delete": "bool",
    "presign_read": "bool",
    "presign_stat": "bool",
    "presign_write": "bool",
    "read": "bool",
    "read_with_if_match": "bool",
    "read_with_if_modified_since": "bool",
    "read_with_if_none_match": "bool",
    "read_with_if_unmodified_since": "bool",
    "read_with_override_cache_control": "bool",
    "read_with_override_content_disposition": "bool",
    "read_with_override_content_type": "bool",
    "read_with_version": "bool",
    "rename": "bool",
    "rename_with_if_not_exists": "bool",
    "shared": "bool",
    "stat": "bool",
    "stat_with_if_match": "bool",
    "stat_with_if_modified_since": "bool",
    "stat_with_if_none_match": "bool",
    "stat_with_if_unmodified_since": "bool",
    "stat_with_version": "bool",
    "write": "bool",
    "write_can_append": "bool",
    "write_can_empty": "bool",
    "write_can_multi": "bool",
    "write_multi_max_size": "int | None",
    "write_multi_min_size": "int | None",
    "write_total_max_size": "int | None",
    "write_with_cache_control": "bool",
    "write_with_content_disposition": "bool",
    "write_with_content_encoding": "bool",
    "write_with_content_type": "bool",
    "write_with_if_match": "bool",
    "write_with_if_none_match": "bool",
    "write_with_if_not_exists": "bool",
    "write_with_user_metadata": "bool",
}

# Every `Metadata` property as `types.pyi` declared it before `Metadata` was
# generated from core.
HISTORICAL_METADATA = {
    "content_disposition": "str | None",
    "content_encoding": "str | None",
    "content_length": "int",
    "content_md5": "str | None",
    "content_type": "str | None",
    "etag": "str | None",
    "is_dir": "bool",
    "is_file": "bool",
    "last_modified": "datetime | None",
    "mode": "EntryMode",
    "user_metadata": "dict[str, str] | None",
    "version": "str | None",
}


def stub_properties(stub: str, class_name: str) -> dict[str, str]:
    tree = ast.parse((STUBS / stub).read_text())
    cls = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == class_name
    )
    return {
        node.name: ast.unparse(node.returns)
        for node in cls.body
        if isinstance(node, ast.FunctionDef)
        and node.returns is not None
        and any(
            isinstance(d, ast.Name) and d.id == "property" for d in node.decorator_list
        )
    }


def matches_type(value: object, annotation: str) -> bool:
    scalars = {"bool": bool, "int": int, "str": str}
    for name in annotation.split(" | "):
        if name == "None" and value is None:
            return True
        # `type(...) is` keeps `bool` from passing as `int`.
        if type(value) is scalars.get(name):
            return True
        if name == "datetime" and isinstance(value, datetime):
            return True
        if name == "EntryMode" and isinstance(value, EntryMode):
            return True
        if name == "dict[str, str]" and isinstance(value, dict):
            return all(
                isinstance(k, str) and isinstance(v, str) for k, v in value.items()
            )
    return False


@pytest.mark.parametrize(("name", "annotation"), HISTORICAL_CAPABILITY.items())
def test_capability_keeps_historical_property(name, annotation):
    assert stub_properties("capability.pyi", "Capability").get(name) == annotation

    cap = opendal.Operator("memory").capability()
    value = getattr(cap, name)
    assert matches_type(value, annotation), f"{name} returned {value!r}"


@pytest.mark.parametrize(("name", "annotation"), HISTORICAL_METADATA.items())
def test_metadata_keeps_historical_property(name, annotation):
    assert stub_properties("types.pyi", "Metadata").get(name) == annotation

    op = opendal.Operator("memory")
    op.write("file", b"OpenDAL")
    value = getattr(op.stat("file"), name)
    assert matches_type(value, annotation), f"{name} returned {value!r}"
