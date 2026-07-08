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

"""Post-process the stubs from ``maturin --generate-stubs``.

``maturin`` writes them under ``opendal/_opendal/``, but the public API and the
classes' ``module=`` use ``opendal.<name>``; type checkers resolve from the
filesystem, so the stubs must live at ``opendal/<name>.pyi``. This relocates
them and applies two temporary fixups (until PyO3 introspection covers them):

1. Inject the imports our forward-ref string annotations need (PyO3 writes the
   annotation but not the import).
2. Replace ``exceptions.pyi`` with a hand-written stub while the generated one is
   incomplete -- ``create_exception!`` types are not introspectable yet, so PyO3
   emits only ``__getattr__``. When that changes, the stub is kept as-is.
"""

from __future__ import annotations

import re
from pathlib import Path

PKG = Path(__file__).resolve().parent.parent / "python" / "opendal"
GEN = PKG / "_opendal"

# Public ``opendal.<name>`` submodules whose stubs are relocated to ``<name>.pyi``.
SUBMODULES = (
    "capability",
    "exceptions",
    "file",
    "layers",
    "operator",
    "services",
    "types",
)

# Imports for forward-ref string annotations that PyO3 does not emit. Relative
# imports stay valid after relocation (the siblings move together).
IMPORTS = {
    "operator": (
        "import collections.abc\n"
        "from .file import AsyncFile\n"
        "from .services import Scheme\n"
        "from .types import Entry, PresignedRequest\n"
    ),
    "file": "import collections.abc\nimport types\nimport typing_extensions\n",
    # Generated service configs use `PathBuf` params, which pyo3 renders as
    # `str | PathLike[str]`; inject the import the annotation needs.
    "services": "from os import PathLike\n",
}

# Mirrors the `create_exception!` types in `src/errors.rs`; keep in sync.
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


def fix_incomplete_exceptions() -> None:
    """Use the hand-written exceptions stub while the generated one is incomplete."""
    path = GEN / "exceptions.pyi"
    if "__getattr__" in path.read_text():
        path.write_text(EXCEPTIONS_STUB)


def rewrite_config_init(text: str) -> str:
    """Rewrite generated config ``__new__`` methods as ``__init__``.

    PyO3 emits pyclass constructors as ``__new__(cls, ...) -> XConfig``. Rename
    them to ``__init__(self, ...) -> None`` so mkdocstrings' ``merge_init_into_class``
    renders the typed constructor signature on the class. Typing is unchanged --
    ``XConfig(...)`` still checks against the same parameters.
    """
    text = re.sub(
        r"def __new__\(\n(\s*)cls,",
        r"def __init__(\n\1self,",
        text,
    )
    text = re.sub(
        r"def __new__\(cls,",
        r"def __init__(self,",
        text,
    )
    text = re.sub(
        r"\) -> [A-Za-z0-9]+Config: \.\.\.",
        r") -> None: ...",
        text,
    )
    return text


def relocate() -> None:
    """Move the stubs to the public ``opendal/<name>.pyi`` paths."""
    for name in SUBMODULES:
        text = (GEN / f"{name}.pyi").read_text()
        if name == "services":
            text = rewrite_config_init(text)
        (PKG / f"{name}.pyi").write_text(IMPORTS.get(name, "") + text)

    # Stub for the ``opendal/_opendal.*.so`` extension itself.
    (PKG / "_opendal.pyi").write_text((GEN / "__init__.pyi").read_text())

    for child in GEN.iterdir():
        child.unlink()
    GEN.rmdir()


def main() -> None:
    """Apply the fixups, then relocate to the public paths."""
    fix_incomplete_exceptions()
    relocate()


if __name__ == "__main__":
    main()
