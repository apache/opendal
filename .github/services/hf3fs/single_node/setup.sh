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

STAGE_DIR="$RUNNER_TEMP/3fs"
FDB_CLIENTS_URL="https://github.com/apple/foundationdb/releases/download/7.1.61/foundationdb-clients_7.1.61-1_amd64.deb"
FDB_CLIENTS_SHA256="c8ffc7604795719a11c615236c49cc8ac277fddd28caaa5fe670b11e6a54dc2f"
FDB_SERVER_URL="https://github.com/apple/foundationdb/releases/download/7.1.61/fdbserver.x86_64"
FDB_SERVER_SHA256="6fd31402d5cd0a797307077d6f39591a806d39068fc6a28e3c6cb96b96081438"

sudo apt-get update -qq
# Runtime packages required on both cache hit and cache miss.
sudo apt-get install -y --no-install-recommends \
  "linux-modules-extra-$(uname -r)" rdma-core ibverbs-utils ibverbs-providers \
  fuse3 gettext-base \
  libatomic1 libuv1 liblz4-1 liblzma5 libdouble-conversion3 \
  libdwarf1 libunwind8 libaio1 libgflags2.2 libgoogle-glog0v5 \
  libgoogle-perftools4 libssl3 libibverbs1 libnuma1 \
  libevent-2.1-7 libevent-extra-2.1-7 libevent-pthreads-2.1-7 libfmt8 \
  libboost-context1.74.0 libboost-filesystem1.74.0 \
  libboost-program-options1.74.0 libboost-regex1.74.0 \
  libboost-system1.74.0 libboost-thread1.74.0

sudo modprobe rdma_rxe
device=$(ip -4 route show default | awk 'NR == 1 {print $5}')
sudo rdma link add opendal_rxe type rxe netdev "$device"
ibv_devinfo -d opendal_rxe

curl -fsSL --retry 3 -o "$RUNNER_TEMP/fdb.deb" "$FDB_CLIENTS_URL"
echo "$FDB_CLIENTS_SHA256  $RUNNER_TEMP/fdb.deb" | sha256sum -c -
sudo dpkg -i "$RUNNER_TEMP/fdb.deb"

curl -fsSL --retry 3 -o "$RUNNER_TEMP/fdbserver" "$FDB_SERVER_URL"
echo "$FDB_SERVER_SHA256  $RUNNER_TEMP/fdbserver" | sha256sum -c -
sudo install -m 755 "$RUNNER_TEMP/fdbserver" /usr/local/bin/fdbserver

if [[ "${CACHE_HIT:-false}" != true ]]; then
  # Build-only packages and libfuse 3.16.2 build stay on the cache-miss path.
  sudo apt-get install -y --no-install-recommends \
    cmake ninja-build libuv1-dev liblz4-dev liblzma-dev libdouble-conversion-dev \
    libdwarf-dev libunwind-dev libaio-dev libgflags-dev libgoogle-glog-dev \
    libgtest-dev libgmock-dev clang-14 clang-format-14 lld-14 libgoogle-perftools-dev \
    libssl-dev gcc-12 g++-12 libboost-all-dev meson libibverbs-dev libnuma-dev \
    libevent-dev libfmt-dev autoconf automake libtool gettext

  git clone -q --depth 1 --branch fuse-3.16.2 https://github.com/libfuse/libfuse.git "$RUNNER_TEMP/libfuse"
  meson setup "$RUNNER_TEMP/libfuse/build" "$RUNNER_TEMP/libfuse" -Dexamples=false -Dtests=false
  ninja -C "$RUNNER_TEMP/libfuse/build"
  sudo ninja -C "$RUNNER_TEMP/libfuse/build" install
  sudo ldconfig

  SRC_DIR="$RUNNER_TEMP/3fs-src"
  git clone -q https://github.com/deepseek-ai/3FS.git "$SRC_DIR"
  cd "$SRC_DIR"
  git checkout 22fca04564c7cc230fd8b9523b8b92864e1dad47
  git submodule update --init --recursive --depth 1 --jobs 4
  ./patches/apply.sh
  cmake -S . -B build -G Ninja -DCMAKE_C_COMPILER=clang-14 -DCMAKE_CXX_COMPILER=clang++-14 \
    -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_FLAGS_RELEASE='-O1 -DNDEBUG' \
    -DCMAKE_CXX_FLAGS_RELEASE='-O1 -DNDEBUG' -DSHUFFLE_METHOD=g++11
  cmake --build build -j 3 --target mgmtd_main meta_main storage_main hf3fs_fuse_main admin_cli hf3fs_api_shared

  rm -rf "$STAGE_DIR"
  mkdir -p "$STAGE_DIR/bin" "$STAGE_DIR/lib" "$STAGE_DIR/fuse" "$STAGE_DIR/tests"

  bins=(mgmtd_main meta_main storage_main hf3fs_fuse_main admin_cli)
  for bin in "${bins[@]}"; do
    cp -L "$SRC_DIR/build/bin/$bin" "$STAGE_DIR/bin/$bin"
  done
  cp -L "$SRC_DIR/build/src/lib/api/libhf3fs_api_shared.so" "$STAGE_DIR/lib/libhf3fs_api_shared.so"

  # Stage any shared libraries produced inside the 3FS build tree (e.g. jemalloc).
  for elf in "$STAGE_DIR"/bin/* "$STAGE_DIR/lib/libhf3fs_api_shared.so"; do
    while IFS= read -r lib_path; do
      [[ -n "$lib_path" ]] || continue
      dest="$STAGE_DIR/lib/$(basename "$lib_path")"
      [[ -e "$dest" ]] || cp -L "$lib_path" "$dest"
    done < <(ldd "$elf" | awk -v prefix="$SRC_DIR/build/" 'index($3, prefix) == 1 {print $3}')
  done

  # Stage libfuse 3.16.2 runtime libraries and helper binaries.
  while IFS= read -r fuse_lib; do
    cp -a "$fuse_lib" "$STAGE_DIR/lib/"
  done < <(find /usr/local/lib /usr/local/lib/x86_64-linux-gnu -maxdepth 1 -name 'libfuse3.so*' 2>/dev/null || true)
  test -e "$STAGE_DIR/lib/libfuse3.so.3"
  for fuse_bin in /usr/local/bin/fusermount3 /usr/local/sbin/mount.fuse3; do
    if [[ -f "$fuse_bin" ]]; then
      cp -L "$fuse_bin" "$STAGE_DIR/fuse/$(basename "$fuse_bin")"
    fi
  done

  # Stage launcher script and cluster configs.
  cp -a "$SRC_DIR/tests/fuse" "$STAGE_DIR/tests/fuse"

  # Strip staged binaries and shared libraries to keep the cache compact.
  strip --strip-all "$STAGE_DIR"/bin/*
  for fuse_bin in "$STAGE_DIR"/fuse/*; do
    [[ -f "$fuse_bin" ]] && strip --strip-all "$fuse_bin"
  done
  find "$STAGE_DIR/lib" -maxdepth 1 -type f -name '*.so*' -exec strip --strip-unneeded {} +

  # Drop the build tree before validating runtime resolution.
  rm -rf "$SRC_DIR" "$RUNNER_TEMP/libfuse"
  du -sh "$STAGE_DIR"
  ls -lh "$STAGE_DIR/bin" "$STAGE_DIR/lib"
fi

# Install staged libfuse helpers and register staged shared libraries.
if [[ -f "$STAGE_DIR/fuse/fusermount3" ]]; then
  sudo install -m 4755 "$STAGE_DIR/fuse/fusermount3" /usr/local/bin/fusermount3
fi
if [[ -f "$STAGE_DIR/fuse/mount.fuse3" ]]; then
  sudo install -m 755 "$STAGE_DIR/fuse/mount.fuse3" /usr/local/sbin/mount.fuse3
fi
echo "$STAGE_DIR/lib" | sudo tee /etc/ld.so.conf.d/00-hf3fs.conf >/dev/null
sudo ldconfig

# Verify all staged runtime dependencies resolve without the build tree.
for elf in "$STAGE_DIR"/bin/* "$STAGE_DIR/lib/libhf3fs_api_shared.so"; do
  if ldd "$elf" | grep -q 'not found'; then
    echo "Missing runtime shared library for $elf:" >&2
    ldd "$elf" >&2
    exit 1
  fi
done
