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

import opendal


def test_sync_write(operator: opendal.Operator):
    filename = 'test_file_name.txt'
    content = b'Hello, world!'
    size = len(content)
    try:
        operator.write(filename, content)
    except Exception as e:
        pytest.fail(f'test_sync_write fail with {e}')
    assert operator.stat(filename)
