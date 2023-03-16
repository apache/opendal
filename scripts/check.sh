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

if [ -z ${OPENDAL_VERSION} ]; then
    echo "OPENDAL_VERSION is unset";
    exit 1
else
    echo "var is set to '$OPENDAL_VERSION'";
fi

# tar source code
release_version=${OPENDAL_VERSION}
# rc versions
rc_version=${OPENDAL_VERSION_RC:rc1}
# Corresponding git repository branch
git_branch=release-${release_version}-${rc_version}

echo "> Checkout release"
svn co https://dist.apache.org/repos/dist/dev/incubator/opendal/${release_version}-${rc_version}/ incubator-opendal-release-verify
cd incubator-opendal-release-verify

echo "> Check signature"
for i in *.tar.gz; do echo $i; gpg --verify $i.asc $i ; done
echo "> Check sha512sum"
for i in *.tar.gz; do echo $i; sha512sum --check $i.sha512; done

echo "> Check content"
tar -xvf apache-incubator-opendal-${release_version}-src.tar.gz
echo "> Check license"
cd apache-incubator-opendal-${release_version}-src
docker run -it --rm -v $(pwd):/github/workspace -u $(id -u):$(id -g) ghcr.io/korandoru/hawkeye-native check
