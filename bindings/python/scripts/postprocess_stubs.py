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

"""Post-process the stubs emitted by ``maturin --generate-stubs``.

Temporary fixups, expected to disappear as PyO3 introspection matures:

1. Inject the module imports that our forward-reference string annotations need.
   PyO3 writes ``-> "collections.abc.Awaitable[...]"`` verbatim but does not emit
   the import. (Custom-import injection is on the PyO3 introspection roadmap.)
2. Replace the generated ``exceptions.pyi`` with a hand-written one, but only
   while the generated stub is incomplete. ``create_exception!`` produces
   ``PyErr`` subtypes (not ``#[pyclass]``es), which PyO3 cannot yet introspect,
   so it emits just ``def __getattr__(name) -> Incomplete``. Once upstream can
   describe these types, the generated stub will no longer be incomplete and is
   kept as-is -- automatically retiring this hack.
"""

from __future__ import annotations

from pathlib import Path

GEN = Path(__file__).resolve().parent.parent / "python" / "opendal" / "_opendal"

# Names referenced only inside forward-reference string annotations, which PyO3
# does not emit imports for. Keyed by stub file -> import lines to prepend.
IMPORTS = {
    "operator.pyi": (
        "import collections.abc\n"
        "from .file import AsyncFile\n"
        "from .services import Scheme\n"
        "from .types import Entry, PresignedRequest\n"
    ),
    "file.pyi": ("import collections.abc\nimport types\nimport typing_extensions\n"),
}

# Hand-written exceptions stub, written only when the generated one is
# incomplete (see module docstring). Mirrors the `create_exception!` types in
# `src/errors.rs`; keep in sync until PyO3 can introspect them.
EXCEPTIONS_STUB = """\
class Error(Exception): ...
class Unexpected(Exception): ...
class Unsupported(Exception): ...
class ConfigInvalid(Exception): ...
class NotFound(Exception): ...
class PermissionDenied(Exception): ...
class IsADirectory(Exception): ...
class NotADirectory(Exception): ...
class AlreadyExists(Exception): ...
class IsSameFile(Exception): ...
class ConditionNotMatch(Exception): ...
class RateLimited(Exception): ...
class RangeNotSatisfied(Exception): ...
"""


def inject_imports() -> None:
    """Prepend forward-reference imports PyO3 introspection does not emit."""
    for name, header in IMPORTS.items():
        path = GEN / name
        path.write_text(header + path.read_text())


def fix_incomplete_exceptions() -> None:
    """Replace exceptions.pyi with the hand-written stub while it is incomplete."""
    path = GEN / "exceptions.pyi"
    incomplete = "__getattr__" in path.read_text()
    if incomplete:
        path.write_text(EXCEPTIONS_STUB)


def main() -> None:
    """Run the post-generation stub fixups."""
    inject_imports()
    fix_incomplete_exceptions()


if __name__ == "__main__":
    main()
