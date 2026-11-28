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

"""Docstrings with quotes and backslashes keep their text through `just fmt`."""

import ast
import importlib.util
import inspect
import shutil
import subprocess
import sys
from pathlib import Path

BINDING = Path(__file__).resolve().parents[1]

_spec = importlib.util.spec_from_file_location(
    "postprocess_stubs", BINDING / "scripts" / "postprocess_stubs.py"
)
postprocess_stubs = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(postprocess_stubs)

# A core `ListOptions` doc with quotes and backslashes.
DOC = '"Options" for list operations, with \\d, \\\\ and """ inside.\n\nMore text.'


def just_fmt(tmp_path: Path, name: str, source: str) -> str:
    """Run the ruff steps of `just fmt` on `source` at the binding path `name`."""
    shutil.copy(BINDING / "ruff.toml", tmp_path / "ruff.toml")
    (tmp_path / name).parent.mkdir(parents=True)
    (tmp_path / name).write_text(source)
    ruff = [sys.executable, "-m", "ruff"]
    subprocess.run([*ruff, "format", "--quiet", name], cwd=tmp_path, check=True)
    subprocess.run(
        [*ruff, "check", "--unsafe-fixes", "--fix", "--silent", name],
        cwd=tmp_path,
        check=True,
    )
    return (tmp_path / name).read_text()


def docstring(source: str) -> str:
    """Return the cleaned docstring of the only class in `source`."""
    (cls,) = (n for n in ast.parse(source).body if isinstance(n, ast.ClassDef))
    return inspect.cleandoc(ast.get_docstring(cls, clean=False))


def test_stub_docstring_survives_fmt(tmp_path):
    # `_opendal.pyi` as pyo3-introspection 0.30.1 writes the pyclass doc.
    stub = (
        '"""Stub."""\n\n'
        "from typing import final\n\n"
        "@final\n"
        "class ListOptions:\n"
        '    """\n'
        '    "Options" for list operations, with \\\\d, \\\\\\\\ and ""\\" inside.\n'
        "\n"
        "    More text.\n"
        '    """\n'
    )
    assert docstring(stub) == DOC
    escaped = postprocess_stubs.escape_docstrings(stub)
    assert docstring(escaped) == DOC
    assert docstring(just_fmt(tmp_path, "python/opendal/_opendal.pyi", escaped)) == DOC


def test_options_docstring_survives_fmt(tmp_path):
    # The first line is `dev/src/generate/python.rs` output, which
    # `test_list_options_survive_any_core_doc` checks.
    options = (
        '"""Options."""\n\n'
        "from typing import TypedDict\n\n\n"
        "class ListKwargs(TypedDict, total=False):\n"
        '    """\\u0022Options" for list operations, with \\u005cd, \\u005c\\u005c'
        ' and ""\\u0022 inside.\n'
        "\n"
        "    More text.\n"
        '    """\n'
    )
    assert docstring(options) == DOC
    formatted = just_fmt(tmp_path, "python/opendal/options.py", options)
    assert formatted == options
    assert docstring(formatted) == DOC


def test_escape_docstring_reads_back():
    for doc in [
        DOC,
        '"',
        '""""',
        'ends with ""',
        "tab\tand\x7f",
        "plain text",
    ]:
        source = '"""' + postprocess_stubs.escape_docstring(doc) + '"""'
        assert ast.literal_eval(source) == doc
        assert "\\" not in source.replace("\\u", "")
        assert not source.startswith('""""')
