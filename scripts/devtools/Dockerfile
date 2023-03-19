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

FROM ubuntu:22.04

ENV TERM xterm-256color
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -yq && \
    apt-get install -yq locales sudo unzip file curl clang openjdk-11-jdk-headless libssl-dev make git ripgrep && \
    printf 'en_US.UTF-8 UTF-8\n' > /etc/locale.gen && \
    locale-gen && \
    rm -rf /var/lib/apt/lists/*

ENV LANGUAGE en_US
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8

RUN groupdel dialout && groupdel staff && \
    groupadd -g 20 staff && groupadd -g 121 docker && \
    useradd -u 1000 -g 100 builder && \
    useradd -u 1001 -g 121 runner && \
    useradd -u 501 -g 20 macos && \
    printf "builder,runner,macos ALL=(ALL:ALL) NOPASSWD:ALL\n" > /etc/sudoers.d/databend

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain stable  && \
    rm -rf /root/.cargo/git && \
    rm -rf /root/.cargo/registry && \
    mkdir -p /opt/rust && \
    mv /root/.cargo /opt/rust/cargo && \
    mv /root/.rustup /opt/rust/rustup

RUN ln -s $(readlink -f `which javac` | sed "s:/bin/javac::") /opt/java

ENV JAVA_HOME /opt/java
ENV LD_LIBRARY_PATH /opt/java/lib/server
ENV RUSTUP_HOME /opt/rust/rustup
ENV CARGO_HOME /opt/rust/cargo
ENV PATH /opt/rust/cargo/bin:/opt/java/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

RUN curl -L --output cargo-binstall.tgz https://github.com/cargo-bins/cargo-binstall/releases/download/v0.19.3/cargo-binstall-x86_64-unknown-linux-gnu.tgz && \
    tar xf cargo-binstall*.tgz -C /opt/rust/cargo/bin
RUN cargo binstall taplo-cli --no-confirm

VOLUME [ "/workspace", "/opt/rust/cargo/registry"]
