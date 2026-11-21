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

FROM node:24-trixie-slim AS node
FROM ghcr.io/astral-sh/uv:0.13.9 AS uv
FROM mcr.microsoft.com/devcontainers/rust:1-trixie

SHELL ["/bin/bash", "-o", "pipefail", "-c"]
USER root

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        cabal-install cmake default-jdk dub ghc just ldc libclang-dev libgtest-dev \
        ninja-build opam php-cli php-dev python3-dev python3-pip python3-venv ruby-dev \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
RUN ln -s default-java /usr/lib/jvm/default

COPY --from=node /usr/local/bin/node /usr/local/bin/
COPY --from=node /usr/local/lib/node_modules/ /usr/local/lib/node_modules/
RUN ln -s ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm \
    && ln -s ../lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx \
    && ln -s ../lib/node_modules/corepack/dist/corepack.js /usr/local/bin/corepack \
    && corepack enable

COPY --from=uv /uv /uvx /usr/local/bin/
# The Python justfile requests a uv-managed interpreter. Match Debian's header minor version.
ENV UV_PYTHON=3.13

# Match the Zig version used by the binding's CI.
RUN arch="$(uname -m)" \
    && curl -fsSL "https://github.com/marler8997/zigup/releases/download/v2025_05_24/zigup-${arch}-linux.tar.gz" \
        | tar -xz -C /usr/local/bin zigup \
    && zigup --install-dir /opt/zig --path-link /usr/local/bin/zig 0.14.0

# DMD publishes Linux binaries for x86. LDC also supports arm64.
RUN if [ "$(uname -m)" = x86_64 ]; then \
        mkdir -p /opt/dmd \
        && curl -fsSL https://github.com/dlang/dmd/releases/download/v2.114.0/dmd.2.114.0.linux.tar.xz \
            | tar -xJ --strip-components=1 -C /opt/dmd dmd2/linux dmd2/src/phobos dmd2/src/druntime; \
    fi

# The Python stub generation recipe also runs the repository's formatters.
RUN arch="$(uname -m)" \
    && curl -fsSL "https://github.com/korandoru/hawkeye/releases/download/v7.2.0/hawkeye-${arch}-unknown-linux-gnu.tar.xz" \
        | tar -xJ --strip-components=1 -C /usr/local/bin "hawkeye-${arch}-unknown-linux-gnu/hawkeye" \
    && curl -fsSL "https://github.com/tamasfe/taplo/releases/download/0.10.0/taplo-linux-${arch}.gz" \
        | gzip -d > /usr/local/bin/taplo \
    && chmod +x /usr/local/bin/taplo

ENV PATH="/home/vscode/.opam/default/bin:/opt/dmd/linux/bin64:${PATH}"

# Initialize home-directory tools as the devcontainer's remote user.
RUN sudo -H -u vscode corepack install --global pnpm@11.8.0
RUN sudo -H -u vscode cabal update
# The OCaml binding requires OCaml 4.x, while Trixie ships 5.x.
RUN sudo -H -u vscode opam init --bare --no-setup --yes --disable-sandboxing \
    && sudo -H -u vscode opam switch create default ocaml-base-compiler.4.14.2 --yes --jobs=4 \
    && sudo -H -u vscode opam install --yes --jobs=4 dune ounit2 ocamlformat
