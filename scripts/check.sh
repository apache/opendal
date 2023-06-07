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

set -e

YELLOW="\033[37;1m"
GREEN="\033[32;1m"
ENDCOLOR="\033[0m"

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 {YOUR RELEASE TAR FILE}" >&2
  exit 1
fi

PKG=$1

if [ ! -f "$PKG" ]; then
    echo "File '$PKG' does not exist."
    exit 1
fi

echo "> Check signature"
gpg --verify "$PKG.asc" "$PKG"

if [ $? -eq 0 ]
then
    printf $GREEN"Success to verify the gpg sign"$ENDCOLOR"\n"
else
    printf $YELLOW"Failed to verify the gpg sign"$ENDCOLOR"\n"
fi

echo "> Check sha512sum"
sha512sum --check "$PKG.sha512"

if [ $? -eq 0 ]
then
    printf $GREEN"Success to verify the checksum"$ENDCOLOR"\n"
else
    printf $YELLOW"Failed to verify the checksum"$ENDCOLOR"\n"
fi
