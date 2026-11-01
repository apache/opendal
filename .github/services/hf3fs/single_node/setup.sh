#!/usr/bin/env bash
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

set -euo pipefail

sudo apt-get update -qq
sudo apt-get install -y "linux-modules-extra-$(uname -r)" rdma-core ibverbs-utils \
  cmake ninja-build libuv1-dev liblz4-dev liblzma-dev libdouble-conversion-dev \
  libdwarf-dev libunwind-dev libaio-dev libgflags-dev libgoogle-glog-dev \
  libgtest-dev libgmock-dev clang-14 clang-format-14 lld-14 libgoogle-perftools-dev \
  libssl-dev gcc-12 g++-12 libboost-all-dev meson libibverbs-dev libnuma-dev \
  libevent-dev libfmt-dev autoconf automake libtool gettext fuse3
sudo modprobe rdma_rxe
device=$(ip -4 route show default | awk 'NR == 1 {print $5}')
sudo rdma link add opendal_rxe type rxe netdev "$device"
ibv_devinfo -d opendal_rxe

curl -fL --retry 3 -o "$RUNNER_TEMP/fdb.deb" \
  https://github.com/apple/foundationdb/releases/download/7.1.61/foundationdb-clients_7.1.61-1_amd64.deb
sudo dpkg -i "$RUNNER_TEMP/fdb.deb"
curl -fL --retry 3 -o "$RUNNER_TEMP/fdbserver" \
  https://github.com/apple/foundationdb/releases/download/7.1.61/fdbserver.x86_64
sudo install -m 755 "$RUNNER_TEMP/fdbserver" /usr/local/bin/fdbserver

git clone -q --depth 1 --branch fuse-3.16.2 https://github.com/libfuse/libfuse.git "$RUNNER_TEMP/libfuse"
meson setup "$RUNNER_TEMP/libfuse/build" "$RUNNER_TEMP/libfuse" -Dexamples=false -Dtests=false
ninja -C "$RUNNER_TEMP/libfuse/build"
sudo ninja -C "$RUNNER_TEMP/libfuse/build" install
sudo ldconfig

if [[ "$CACHE_HIT" == true ]]; then
  exit 0
fi

git clone -q https://github.com/deepseek-ai/3FS.git "$RUNNER_TEMP/3fs"
cd "$RUNNER_TEMP/3fs"
git checkout 22fca04564c7cc230fd8b9523b8b92864e1dad47
git submodule update --init --recursive --depth 1 --jobs 4
./patches/apply.sh
cmake -S . -B build -G Ninja -DCMAKE_C_COMPILER=clang-14 -DCMAKE_CXX_COMPILER=clang++-14 \
  -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_FLAGS_RELEASE='-O1 -DNDEBUG' \
  -DCMAKE_CXX_FLAGS_RELEASE='-O1 -DNDEBUG' -DSHUFFLE_METHOD=g++11
cmake --build build -j 3 --target mgmtd_main meta_main storage_main hf3fs_fuse_main admin_cli hf3fs_api_shared
