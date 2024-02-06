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

import os
import subprocess
from pathlib import Path
import tomllib

OPENDAL_VERSION = os.getenv('OPENDAL_VERSION')
OPENDAL_VERSION_RC = os.getenv('OPENDAL_VERSION_RC', 'rc.1')

if not OPENDAL_VERSION:
    print("OPENDAL_VERSION is unset")
    exit(1)
else:
    print(f"var is set to '{OPENDAL_VERSION}'")

# tar source code
release_version = OPENDAL_VERSION
# rc versions
rc_version = OPENDAL_VERSION_RC
# Corresponding git repository branch
git_branch = f"release-{release_version}-{rc_version}"

dist_path = Path('dist')
if not dist_path.exists():
    dist_path.mkdir()

print("> Checkout version branch")
subprocess.run(['git', 'checkout', '-B', git_branch], check=True)

print("> Start package")
subprocess.run(['git', 'archive', '--format=tar.gz', '--output', f"dist/apache-opendal-{release_version}-src.tar.gz", '--prefix', f"apache-opendal-{release_version}-src/", '--add-file=Cargo.toml', git_branch], check=True)

os.chdir('dist')
print("> Generate signature")
for i in Path('.').glob('*.tar.gz'):
    print(i)
    subprocess.run(['gpg', '--armor', '--output', f"{i}.asc", '--detach-sig', str(i)], check=True)

print("> Check signature")
for i in Path('.').glob('*.tar.gz'):
    print(i)
    subprocess.run(['gpg', '--verify', f"{i}.asc", str(i)], check=True)

print("> Generate sha512sum")
for i in Path('.').glob('*.tar.gz'):
    print(i)
    subprocess.run(['sha512sum', str(i)], stdout=open(f"{i}.sha512", "w"))

print("> Check sha512sum")
for i in Path('.').glob('*.tar.gz.sha512'):
    print(i)
    subprocess.run(['sha512sum', '--check', str(i)], check=True)
