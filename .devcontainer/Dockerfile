# Copyright 2022 Datafuse Labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM debian:bullseye

ENV TERM xterm-256color

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
    printf "builder ALL=(ALL:ALL) NOPASSWD:ALL\n" > /etc/sudoers.d/opendal && \
    chmod 0440 /etc/sudoers.d/opendal

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

VOLUME [ "/workspace", "/opt/rust/cargo/registry", "/opt/rust/cargo/git" ]

RUN curl -L --output cargo-binstall.tgz https://github.com/cargo-bins/cargo-binstall/releases/download/v0.19.3/cargo-binstall-x86_64-unknown-linux-gnu.tgz && \
    tar xf cargo-binstall*.tgz -C /opt/rust/cargo/bin
RUN cargo binstall taplo-cli mdbook lsd --no-confirm

RUN curl -qL https://github.com/rui314/mold/releases/download/v1.2.1/mold-1.2.1-$(uname -m)-linux.tar.gz -o /tmp/mold.tar.gz && \
    tar --strip-components=1 -xzf /tmp/mold.tar.gz -C /usr/local && \
    rm -rf /tmp/mold.tar.gz && \
    ln -sf /usr/local/bin/mold /usr/bin/$(uname -m)-linux-gnu-ld

RUN mkdir /home/builder && chown -R builder /home/builder && \
    usermod --shell /bin/bash builder

RUN curl -qL https://github.com/starship/starship/releases/latest/download/starship-$(uname -m)-unknown-linux-musl.tar.gz  -o /tmp/starship.tar.gz && \
    tar -xzf /tmp/starship.tar.gz -C /usr/local/bin && \
    rm -rf /tmp/starship.tar.gz && \
    ln -sf /usr/local/bin/starship /usr/bin/starship && \
    echo 'eval "$(starship init bash)"' >> /home/builder/.bashrc

USER builder