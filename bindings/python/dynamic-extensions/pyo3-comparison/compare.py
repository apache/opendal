# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import argparse
import json
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable

import opendal_fs_capsule
import opendal_fs_direct
import opendal_mime_capsule
import opendal_mime_direct
import opendal_poc


@dataclass
class VariantState:
    attempted: bool = False
    operator_type: str | None = None
    operator_type_identity: int | None = None
    is_base_operator: bool | None = None
    fs_round_trip: bool | None = None
    fs_stat_succeeded: bool | None = None
    fs_stat_before_layer: str | None = None
    fs_stat_after_layer: str | None = None
    layer_applied: bool | None = None
    layer_error: str | None = None
    base_operator_layer_error: str | None = None
    error: str | None = None


@dataclass
class ComparisonState:
    base_operator_type_identity: int
    base_memory_scheme: str
    capsule: VariantState
    direct: VariantState


def initial_state() -> ComparisonState:
    return ComparisonState(
        base_operator_type_identity=id(opendal_poc.Operator),
        base_memory_scheme=opendal_poc.Operator().scheme(),
        capsule=VariantState(),
        direct=VariantState(),
    )


def run_capsule(root: Path) -> VariantState:
    state = VariantState(attempted=True)
    try:
        variant_root = root / "capsule"
        variant_root.mkdir(exist_ok=True)
        operator = opendal_fs_capsule.create(str(variant_root))
        state.operator_type = repr(type(operator))
        state.operator_type_identity = id(type(operator))
        state.is_base_operator = isinstance(operator, opendal_poc.Operator)
        operator.write("hello.txt", b"hello from capsule")
        state.fs_round_trip = operator.read("hello.txt") == b"hello from capsule"
        state.fs_stat_before_layer = operator.content_type("hello.txt")
        state.fs_stat_succeeded = True
        layered = operator.layer(opendal_mime_capsule.MimeGuessLayer())
        state.fs_stat_after_layer = layered.content_type("hello.txt")
        state.layer_applied = state.fs_stat_after_layer == "text/plain"
    except Exception as error:
        state.error = f"{type(error).__name__}: {error}"
    return state


def run_direct(root: Path) -> VariantState:
    state = VariantState(attempted=True)
    try:
        variant_root = root / "direct"
        variant_root.mkdir(exist_ok=True)
        operator = opendal_fs_direct.create(str(variant_root))
        state.operator_type = repr(type(operator))
        state.operator_type_identity = id(type(operator))
        state.is_base_operator = isinstance(operator, opendal_poc.Operator)
        operator.write("hello.txt", b"hello from direct")
        state.fs_round_trip = operator.read("hello.txt") == b"hello from direct"
        state.fs_stat_before_layer = operator.content_type("hello.txt")
        state.fs_stat_succeeded = True
        try:
            layered = opendal_mime_direct.apply(operator)
        except Exception as error:
            state.layer_error = f"{type(error).__name__}: {error}"
            state.layer_applied = False
        else:
            state.fs_stat_after_layer = layered.content_type("hello.txt")
            state.layer_applied = state.fs_stat_after_layer == "text/plain"

        try:
            opendal_mime_direct.apply(opendal_poc.Operator())
        except Exception as error:
            state.base_operator_layer_error = f"{type(error).__name__}: {error}"
    except Exception as error:
        state.error = f"{type(error).__name__}: {error}"
    return state


def render(state: ComparisonState, *, clear: bool) -> None:
    if clear:
        print("\033[2J\033[H", end="")
    print("\033[1mPyO3 extension comparison\033[0m")
    print(json.dumps(asdict(state), indent=2, sort_keys=True))
    print()
    print(
        "\033[1m[c]\033[0m capsule  "
        "\033[1m[d]\033[0m direct  "
        "\033[1m[a]\033[0m both  "
        "\033[1m[r]\033[0m reset  "
        "\033[1m[q]\033[0m quit"
    )


def apply_action(
    state: ComparisonState,
    action: str,
    root: Path,
) -> ComparisonState:
    actions: dict[str, Callable[[Path], VariantState]] = {
        "c": run_capsule,
        "d": run_direct,
    }
    if action == "r":
        return initial_state()
    if action == "a":
        state.capsule = run_capsule(root)
        state.direct = run_direct(root)
    elif action in actions:
        setattr(state, "capsule" if action == "c" else "direct", actions[action](root))
    return state


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--interactive", action="store_true")
    args = parser.parse_args()

    with tempfile.TemporaryDirectory(prefix="opendal-pyo3-comparison-") as root:
        root_path = Path(root)
        state = initial_state()

        if args.all or not args.interactive:
            state = apply_action(state, "a", root_path)
            render(state, clear=False)
            return

        while True:
            render(state, clear=True)
            action = input("> ").strip().lower()[:1]
            if action == "q":
                return
            state = apply_action(state, action, root_path)


if __name__ == "__main__":
    main()
