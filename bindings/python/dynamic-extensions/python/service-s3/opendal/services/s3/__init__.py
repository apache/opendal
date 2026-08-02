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

from pathlib import Path

from opendal._runtime import _register_service

MANIFEST = {
    "required_runtime_protocol": 1,
    "package_id": "opendal-service-s3-poc",
    "component": {"kind": "service", "id": "s3", "aliases": []},
    "native_entry_symbol": "opendal_service_s3_bootstrap_v1",
}

_register_service(MANIFEST, Path(__file__).parent / "_native" / "libs3_extension.so")
