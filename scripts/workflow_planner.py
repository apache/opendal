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

import sys
import json

def plan(changed_files):
    jobs = {
        "components": {
            "core": True
        },
        "services": {
            "fs": True,
        },
        "matrix": [
            {
                "os": "ubuntu-latest",
                "features": "services-fs,services-s3",
            },
            {
                "os": "windows-latest",
                "features": "services-fs,services-s3",
            }
        ],
        "cases": [
            {
                "os": "ubuntu-latest",
                "setup": "local-fs",
                "service": "fs"
            },
            {
                "os": "windows-latest",
                "setup": "local-fs",
                "service": "fs"
            }
        ]
    }
    return json.dumps(jobs)

if __name__ == '__main__':
    changed_files = sys.argv[1:]
    result = plan(changed_files)
    print(result)
