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

import opendal
import pytest


@pytest.fixture
def operator() -> opendal.Operator:
    # Get current service name from envs.
    service_name = os.environ.get('TEST_SERVICE_NAME')
    if not service_name:
        raise ValueError('TEST_SERVICE_NAME is not provided.')
    service_name = service_name.lower()

    # Read arguments from envs.
    prefix = f'opendal_{service_name}_'
    config = {}
    for key in os.environ.keys():
        if key.lower().startswith(prefix):
            config[key[len(prefix):].lower()] = os.environ.get(key)
    
    # Check if current test be enabled.
    test_flag = config.get("test", "")
    if test_flag != 'on' and test_flag != 'true':
        raise KeyError(f'Service {service_name} test is not enabled.')
    
    operator = opendal.Operator(service_name, **config)
    return operator
