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

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Mapping
import tomllib
import subprocess

ROOT_DIR = Path(__file__).parent.parent


@dataclass
class Config:
    description: str
    type: str
    required: bool = False
    sensitive: bool = False
    default: str = ""
    available: List[str] = field(default_factory=list)
    example: List[str] = field(default_factory=list)

    @property
    def rust_type(self):
        type_mapping = {
            "string": "String",
            "usize": "usize",
            "bool": "bool",
        }

        try:
            value_type = type_mapping[self.type]
        except KeyError:
            raise ValueError(f"Not supported type {self.type}, must be bug")

        if self.required:
            return value_type
        # Always return bool without Option
        elif value_type == "bool":
            return f"bool"
        else:
            return f"Option<{value_type}>"


@dataclass
class Spec:
    # name is parsed from the spec file name.
    name: str
    description: str
    config: Mapping[str, Config]

    # Convert the name from snake_case to CamelCase
    @property
    def camel_case_name(self):
        return to_camel_case(self.name)


def list_specs() -> List[Spec]:
    specs = []
    for path in (ROOT_DIR / "specs").glob("*.toml"):
        with open(path, "rb") as f:
            data = tomllib.load(f)

            spec = Spec(
                # Parse and set the spec name
                name=path.stem,
                description=data["description"],
                config={
                    item: Config(**data["config"][item]) for item in data["config"]
                },
            )
            specs.append(spec)
    return specs


# Convert from snake_case to CamelCase
def to_camel_case(name: str) -> str:
    return name.replace("_", " ").title().replace(" ", "")


# Convert from plain string to rust comments
def to_comments(v: str) -> str:
    return "\n".join(f"/// {line}" for line in v.strip().splitlines())


def generate_rust(spec: Spec):
    # Header
    content = """
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::fmt::Debug;
use std::fmt::Formatter;
use serde::Deserialize;
use crate::raw::*;
"""

    # Config struct Start
    content += f"""
/// Config for {spec.description.strip()}
#[derive(Default, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct {spec.camel_case_name}Config {{
"""

    # Config fields
    for name, cfg in spec.config.items():
        content += f"""
{to_comments(cfg.description)}
pub {name}: {cfg.rust_type},
"""

    # Config struct End
    content += "}"

    # Impl Debug for Config Start
    content += f"""
impl Debug for {spec.camel_case_name}Config {{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {{
    let mut d = f.debug_struct("{spec.camel_case_name}Config");
"""

    # Config fields
    for name, cfg in spec.config.items():
        if cfg.sensitive:
            content += f"""
d.field("{name}", &self.{name}.as_deref().map(mask_secret));
"""
        else:
            content += f"""
d.field("{name}", &self.{name});
"""

    # Impl Debug for Config End
    content += f"""
    d.finish()
}}
}}
"""

    return content


if __name__ == "__main__":
    for spec in list_specs():
        content = generate_rust(spec)
        with open(ROOT_DIR / "core/src/services" / spec.name / "generated.rs", "w") as f:
            f.write(content)
    subprocess.run(
        ["cargo", "fmt"],
        cwd=ROOT_DIR / "core",
        check=True,
    )
