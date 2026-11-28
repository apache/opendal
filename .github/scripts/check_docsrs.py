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

"""Simulate docs.rs builds inside the official crates-build-env Linux sandbox."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import tomllib
from dataclasses import dataclass
from pathlib import Path

DEFAULT_IMAGE = "ghcr.io/rust-lang/crates-build-env/linux:latest"
DEFAULT_TARGET = "x86_64-unknown-linux-gnu"
# Match docs.rs default sandbox memory limit (6 GiB = 6,442,450,944 bytes).
DEFAULT_MEMORY_LIMIT = 6442450944
# Match docs.rs builder concurrency (-j12).
DEFAULT_JOBS = 12
UNCONDITIONAL_RUSTDOC_ARGS = (
    "--static-root-path",
    "/-/rustdoc.static/",
    "--extern-html-root-takes-precedence",
)


@dataclass(frozen=True)
class DocsRsMetadata:
    package_name: str
    all_features: bool = False
    no_default_features: bool = False
    features: tuple[str, ...] = ()
    rustc_args: tuple[str, ...] = ()
    rustdoc_args: tuple[str, ...] = ()
    cargo_args: tuple[str, ...] = ()


def load_docsrs_metadata(manifest_path: Path) -> DocsRsMetadata:
    with manifest_path.open("rb") as fp:
        manifest = tomllib.load(fp)

    package = manifest.get("package", {})
    package_name = package.get("name")
    if not isinstance(package_name, str) or not package_name:
        raise ValueError(f"{manifest_path}: missing [package].name")

    docsrs = package.get("metadata", {}).get("docs", {}).get("rs", {})
    if not isinstance(docsrs, dict):
        raise ValueError(f"{manifest_path}: [package.metadata.docs.rs] must be a table")

    return DocsRsMetadata(
        package_name=package_name,
        all_features=bool(docsrs.get("all-features", False)),
        no_default_features=bool(docsrs.get("no-default-features", False)),
        features=tuple(docsrs.get("features", ())),
        rustc_args=tuple(docsrs.get("rustc-args", ())),
        rustdoc_args=tuple(docsrs.get("rustdoc-args", ())),
        cargo_args=tuple(docsrs.get("cargo-args", ())),
    )


def filter_metadata_cargo_args(args: tuple[str, ...]) -> list[str]:
    """Match docs.rs's filtering of -Zrustdoc-scrape-examples from metadata cargo-args."""
    filtered: list[str] = []
    idx = 0
    while idx < len(args):
        arg = args[idx]
        if arg == "-Zrustdoc-scrape-examples":
            idx += 1
            continue
        if arg == "-Z" and idx + 1 < len(args) and args[idx + 1] == "rustdoc-scrape-examples":
            idx += 2
            continue
        filtered.append(arg)
        idx += 1
    return filtered


def build_cargo_rustdoc_command(
    *,
    cargo_bin: str,
    toolchain: str,
    manifest_path: str,
    metadata: DocsRsMetadata,
    target: str,
    jobs: int,
    mode: str,
) -> list[str]:
    """Build the cargo rustdoc argv matching docs.rs's CommandArgs::finish()."""
    if mode not in ("html", "json"):
        raise ValueError(f"unsupported mode: {mode}")

    cmd = [
        cargo_bin,
        f"+{toolchain}",
        "rustdoc",
        "--manifest-path",
        manifest_path,
        "--lib",
        "-Zrustdoc-map",
    ]

    if metadata.features:
        cmd.extend(["--features", " ".join(metadata.features)])
    if metadata.all_features:
        cmd.append("--all-features")
    if metadata.no_default_features:
        cmd.append("--no-default-features")

    if metadata.rustc_args:
        rustc_array = json.dumps(list(metadata.rustc_args))
        cmd.extend(
            [
                "--config",
                f"build.rustflags={rustc_array}",
                "-Zhost-config",
                "-Ztarget-applies-to-host",
                "--config",
                f"host.rustflags={rustc_array}",
            ]
        )

    rustdoc_flags = ["--cfg", "docsrs", *metadata.rustdoc_args]
    if mode == "html":
        rustdoc_flags.extend(["-Z", "unstable-options", "--emit=html-non-static-files"])
    else:
        rustdoc_flags.extend(["-Z", "unstable-options", "--output-format", "json"])
    rustdoc_flags.extend(UNCONDITIONAL_RUSTDOC_ARGS)

    cmd.extend(["--config", f"build.rustdocflags={json.dumps(rustdoc_flags)}"])
    cmd.extend(
        [
            "--offline",
            "-Zunstable-options",
            f'--config=doc.extern-map.registries.crates-io="https://docs.rs/{{pkg_name}}/{{version}}/{target}"',
            f"-j{jobs}",
            "--target",
            target,
        ]
    )

    if mode == "html":
        cmd.append("-Zrustdoc-scrape-examples")

    cmd.extend(filter_metadata_cargo_args(metadata.cargo_args))
    return cmd


def build_container_script(commands: list[tuple[str, list[str]]]) -> str:
    lines = ["set -euo pipefail"]
    for label, argv in commands:
        quoted = shlex.join(argv)
        lines.append(f"echo {shlex.quote(f'==> {label}')}")
        lines.append(f"echo {shlex.quote(f'+ {quoted}')}")
        lines.append("t0=$(date +%s)")
        lines.append(quoted)
        lines.append("t1=$(date +%s)")
        lines.append(f'echo "==> {label} succeeded in $((t1 - t0))s"')
    lines.append(
        'if [ -r /sys/fs/cgroup/memory.peak ]; then '
        'echo "==> cgroup memory.peak: $(cat /sys/fs/cgroup/memory.peak) bytes"; '
        "fi"
    )
    return "\n".join(lines) + "\n"


def resolve_repo_root(manifest_paths: list[Path]) -> Path:
    script_repo_root = Path(__file__).resolve().parents[2]
    for path in manifest_paths:
        path.resolve().relative_to(script_repo_root)
    return script_repo_root


def run_fetch(toolchain: str, workspace_manifest: Path, target: str) -> None:
    cmd = [
        "cargo",
        f"+{toolchain}",
        "fetch",
        "--locked",
        "--manifest-path",
        str(workspace_manifest),
        "--target",
        target,
    ]
    print(f"==> Fetching locked dependencies: {shlex.join(cmd)}", flush=True)
    subprocess.run(cmd, check=True)


def run_docsrs_check(args: argparse.Namespace) -> None:
    manifest_paths = [Path(p).resolve() for p in args.manifest_path]
    repo_root = resolve_repo_root(manifest_paths)

    cargo_home = Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo")).resolve()
    rustup_home = Path(os.environ.get("RUSTUP_HOME", Path.home() / ".rustup")).resolve()

    if not args.skip_fetch:
        run_fetch(args.toolchain, repo_root / "core" / "Cargo.toml", args.target)

    commands: list[tuple[str, list[str]]] = []
    modes = ["html", "json"] if args.check_json else ["html"]
    for manifest_path in manifest_paths:
        rel_manifest = manifest_path.relative_to(repo_root).as_posix()
        container_manifest = f"/opt/rustwide/workdir/{rel_manifest}"
        metadata = load_docsrs_metadata(manifest_path)
        for mode in modes:
            argv = build_cargo_rustdoc_command(
                cargo_bin="/opt/rustwide/cargo-home/bin/cargo",
                toolchain=args.toolchain,
                manifest_path=container_manifest,
                metadata=metadata,
                target=args.target,
                jobs=args.jobs,
                mode=mode,
            )
            commands.append((f"{metadata.package_name} ({mode})", argv))

    script = build_container_script(commands)

    target_dir = Path(tempfile.mkdtemp(prefix="opendal-docsrs-target-"))
    try:
        docker_cmd = [
            "docker",
            "run",
            "--rm",
            "--user",
            f"{os.getuid()}:{os.getgid()}",
            "--memory",
            str(args.memory_limit),
            "--memory-swap",
            str(args.memory_limit),
            "--network",
            "none",
            "-v",
            f"{repo_root}:/opt/rustwide/workdir:ro",
            "-v",
            f"{target_dir}:/opt/rustwide/target:rw",
            "-v",
            f"{cargo_home}:/opt/rustwide/cargo-home:ro",
            "-v",
            f"{rustup_home}:/opt/rustwide/rustup-home:ro",
            "-e",
            "CARGO_HOME=/opt/rustwide/cargo-home",
            "-e",
            "RUSTUP_HOME=/opt/rustwide/rustup-home",
            "-e",
            "CARGO_TARGET_DIR=/opt/rustwide/target",
            "-e",
            "DOCS_RS=1",
            "-w",
            "/opt/rustwide/workdir/core",
            args.image,
            "bash",
            "-c",
            script,
        ]
        started = time.monotonic()
        print(
            f"==> Running docs.rs sandbox check ({args.image}, "
            f"memory={args.memory_limit} bytes, jobs={args.jobs})",
            flush=True,
        )
        subprocess.run(docker_cmd, check=True)
        elapsed = int(time.monotonic() - started)
        print(f"==> docs.rs sandbox check passed in {elapsed}s", flush=True)
    finally:
        shutil.rmtree(target_dir, ignore_errors=True)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Simulate docs.rs builds in the official crates-build-env Linux container."
    )
    parser.add_argument(
        "--manifest-path",
        action="append",
        help="Path to a Cargo.toml to check (repeatable; defaults to core/Cargo.toml).",
    )
    parser.add_argument(
        "--toolchain",
        default="nightly",
        help="Rustup toolchain name to invoke inside the container (default: nightly).",
    )
    parser.add_argument(
        "--target",
        default=DEFAULT_TARGET,
        help=f"Rust target triple (default: {DEFAULT_TARGET}).",
    )
    parser.add_argument(
        "--image",
        default=DEFAULT_IMAGE,
        help=f"Docker image matching docs.rs (default: {DEFAULT_IMAGE}).",
    )
    parser.add_argument(
        "--memory-limit",
        type=int,
        default=DEFAULT_MEMORY_LIMIT,
        help=f"Container memory and swap cap in bytes (default: {DEFAULT_MEMORY_LIMIT}).",
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=DEFAULT_JOBS,
        help=f"Cargo -j parallelism (default: {DEFAULT_JOBS}).",
    )
    parser.add_argument(
        "--no-json",
        dest="check_json",
        action="store_false",
        help="Skip the rustdoc JSON pass and check HTML only.",
    )
    parser.add_argument(
        "--skip-fetch",
        action="store_true",
        help="Skip running `cargo fetch --locked` before starting the offline container.",
    )
    args = parser.parse_args(argv)
    if not args.manifest_path:
        args.manifest_path = ["core/Cargo.toml"]
    return args


def main() -> int:
    args = parse_args()
    run_docsrs_check(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
