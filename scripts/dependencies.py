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

from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, REMAINDER
import subprocess
import os


DIRS = [
    "core",

    "bin/oli",
    "bin/oay",
    "bin/ofs",

    "bindings/c",
    "bindings/cpp",
    "bindings/dotnet",
    "bindings/haskell",
    "bindings/java",
    "bindings/lua",
    "bindings/nodejs",
    "bindings/ocaml",
    "bindings/php",
    "bindings/python",
    "bindings/ruby",

    "integrations/dav-server",
    "integrations/object_store",
]


def check_deps():
    cargo_dirs = DIRS
    for root in cargo_dirs:
        print(f"Checking dependencies of {root}")
        subprocess.run(["cargo", "deny", "check", "license"], cwd=root)


def generate_deps():
    cargo_dirs = DIRS
    for root in cargo_dirs:
        print(f"Generating dependencies {root}")
        result = subprocess.run(
            ["cargo", "deny", "list", "-f", "tsv", "-t", "0.6"],
            cwd=root,
            capture_output=True,
            text=True,
        )
        with open(f"{root}/DEPENDENCIES.rust.tsv", "w") as f:
            f.write(result.stdout)


if __name__ == "__main__":
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.set_defaults(func=parser.print_help)
    subparsers = parser.add_subparsers()

    parser_check = subparsers.add_parser(
        'check',
        description="Check dependencies",
        help="Check dependencies")
    parser_check.set_defaults(func=check_deps)

    parser_generate = subparsers.add_parser(
        'generate',
        description="Generate dependencies",
        help="Generate dependencies")
    parser_generate.set_defaults(func=generate_deps)

    args = parser.parse_args()
    arg_dict = dict(vars(args))
    del arg_dict['func']
    args.func(**arg_dict)
