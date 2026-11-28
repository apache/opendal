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
them and applies three temporary fixups (until PyO3 introspection covers them):

1. Inject the imports our forward-ref string annotations need (PyO3 writes the
   annotation but not the import). Restore the `ChecksumAlgorithm` alias on
   the generated checksum property because PyO3 infers its Rust string type.
2. Replace ``exceptions.pyi`` with a hand-written stub while the generated one is
   incomplete -- ``create_exception!`` types are not introspectable yet, so PyO3
   emits only ``__getattr__``. When that changes, the stub is kept as-is.
3. Rewrite docstring escapes so that ``just fmt`` keeps the docstring text (see
   ``escape_docstrings``).
"""

from __future__ import annotations

import ast
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
        "from .config import ContextConfig, LayerConfig, ServiceConfig\n"
        "from .file import AsyncFile\n"
        "from .options import ChecksumAlgorithm, OpenKwargs\n"
        "from .services import Scheme\n"
        "from .types import Entry, PresignedRequest\n"
        "from typing_extensions import Unpack\n"
    ),
    "file": "import collections.abc\nimport types\nimport typing\n",
    "types": "from .options import ChecksumAlgorithm\n",
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
class Conflict(Exception): ...
class ChecksumMismatch(Unexpected): ...
"""


def fix_incomplete_exceptions() -> None:
    """Use the hand-written exceptions stub while the generated one is incomplete."""
    path = GEN / "exceptions.pyi"
    if "__getattr__" in path.read_text():
        path.write_text(EXCEPTIONS_STUB)


def pin_disjoint_base_import(text: str) -> str:
    """Import ``disjoint_base`` from ``typing_extensions`` instead of ``typing``.

    PyO3 emits ``from typing import ..., disjoint_base`` when the build
    interpreter targets Python 3.15+, where ``typing.disjoint_base`` exists.
    The stubs must import it from ``typing_extensions`` so they also work on
    the minimum supported Python (3.11).
    """
    lines = []
    for line in text.splitlines(keepends=True):
        if line.startswith("from typing import") and "disjoint_base" in line:
            names = [n.strip() for n in line[len("from typing import") :].split(",")]
            names.remove("disjoint_base")
            line = f"from typing import {', '.join(names)}\n" if names else ""
            line += "from typing_extensions import disjoint_base\n"
        lines.append(line)
    return "".join(lines)


def escape_docstring(doc: str) -> str:
    r"""Return the source between the quotes of a docstring that reads as ``doc``.

    A backslash becomes ``\u005c``, and an ASCII control character other than a
    newline becomes its ``\u00XX`` escape. A ``"`` becomes ``\u0022`` when it
    would join the quotes around it: a leading ``"``, every third ``"`` in a
    row, and the quotes right before the closing quotes. Ruff's D301 accepts
    these escapes. ``dev/src/generate/python.rs`` escapes backslashes and
    quotes in the docstrings of ``options.py`` the same way.
    """
    body = doc.rstrip('"')
    out = []
    run = 0
    for at, c in enumerate(body):
        if c == "\\":
            out.append(r"\u005c")
        elif c == '"' and (at == 0 or run == 2):
            out.append(r"\u0022")
        elif c != "\n" and c.isascii() and not c.isprintable():
            out.append(rf"\u{ord(c):04x}")
        else:
            out.append(c)
        run = run + 1 if c == '"' and at > 0 and run < 2 else 0
    out.append(r"\u0022" * (len(doc) - len(body)))
    return "".join(out)


def escape_docstrings(text: str) -> str:
    r"""Rewrite each docstring in ``text`` whose source holds a backslash.

    PyO3 writes a backslash in a docstring as ``\\``, a control character as an
    escape such as ``\t``, and every third ``"`` in a row as ``\"``. Ruff's D301
    fix, which ``just fmt`` applies, adds an ``r`` prefix to such a docstring,
    so Python reads different text. The rewritten docstring holds the same
    text with escapes that D301 accepts (see ``escape_docstring``).
    """
    data = text.encode()
    starts = [0]
    for line in data.splitlines(keepends=True):
        starts.append(starts[-1] + len(line))
    edits = []
    for node in ast.walk(ast.parse(text)):
        if not (
            isinstance(node, ast.Expr)
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
        ):
            continue
        start = starts[node.lineno - 1] + node.col_offset
        end = starts[node.end_lineno - 1] + node.end_col_offset
        source = data[start:end]
        if b"\\" in source and source.startswith(b'"""'):
            quoted = '"""' + escape_docstring(node.value.value) + '"""'
            edits.append((start, end, quoted.encode()))
    for start, end, quoted in sorted(edits, reverse=True):
        data = data[:start] + quoted + data[end:]
    return data.decode()


def relocate() -> None:
    """Move the stubs to the public ``opendal/<name>.pyi`` paths."""
    for name in SUBMODULES:
        text = escape_docstrings(
            pin_disjoint_base_import((GEN / f"{name}.pyi").read_text())
        )
        if name == "types":
            inferred = "def checksum(self, /) -> tuple[str, bytes] | None:"
            if inferred not in text:
                msg = "unexpected generated Metadata.checksum signature"
                raise ValueError(msg)
            text = text.replace(
                inferred, inferred.replace("tuple[str,", "tuple[ChecksumAlgorithm,"), 1
            )
        (PKG / f"{name}.pyi").write_text(IMPORTS.get(name, "") + text)

    # Stub for the ``opendal/_opendal.*.so`` extension itself.
    (PKG / "_opendal.pyi").write_text(
        escape_docstrings(pin_disjoint_base_import((GEN / "__init__.pyi").read_text()))
    )

    for child in GEN.iterdir():
        child.unlink()
    GEN.rmdir()


def main() -> None:
    """Apply the fixups, then relocate to the public paths."""
    fix_incomplete_exceptions()
    relocate()


if __name__ == "__main__":
    main()
