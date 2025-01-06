#!/bin/bash
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

set +ex
username="foo"
server="127.0.0.1"
port=22
know_hosts_strategy="no"
identity_file="/home/foo/.ssh/keys/id_rsa"
while :; do
    (
        sftp -oPort=$port -o StrictHostKeyChecking=$know_hosts_strategy -o IdentityFile=$identity_file $username@$server << EOF
        bye
EOF
    ) 2>/dev/null
    if [[ $? -eq 0 ]]; then
        echo "SFTP is available, proceeding..."
        break
    else
        echo "Waiting for SFTP to be available..."
        sleep 1
    fi
done
