FROM golang:1.18 AS go-build
RUN go install github.com/apache/skywalking-eyes/cmd/license-eye@latest

FROM debian:bullseye

ENV TERM xterm-256color
ENV DEBIAN_FRONTEND=noninteractive

COPY --from=go-build /go/bin/license-eye /usr/local/bin/license-eye

RUN apt-get update -yq && \
    apt-get install -yq locales sudo unzip file curl clang openjdk-11-jdk-headless libssl-dev make && \
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
    printf "builder,runner,macos ALL=(ALL:ALL) NOPASSWD:ALL\n" > /etc/sudoers.d/opendal

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain stable && \
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

RUN cargo install taplo-cli mdbook --locked

VOLUME [ "/workspace", "/opt/rust/cargo/registry", "/opt/rust/cargo/git" ]

RUN apt-get update && apt-get install -y git ripgrep && \
    curl -sSLo /tmp/lsd.deb https://github.com/Peltoche/lsd/releases/download/0.21.0/lsd_0.21.0_amd64.deb && \
    dpkg -i /tmp/lsd.deb && \
    rm -rf /tmp/* /var/lib/apt/lists/*

RUN curl -qL https://github.com/rui314/mold/releases/download/v1.2.1/mold-1.2.1-$(uname -m)-linux.tar.gz -o /tmp/mold.tar.gz && \
    tar --strip-components=1 -xzf /tmp/mold.tar.gz -C /usr/local && \
    rm -rf /tmp/mold.tar.gz && \
    ln -sf /usr/local/bin/mold /usr/bin/$(uname -m)-linux-gnu-ld

RUN mkdir /home/builder && chown -R builder /home/builder && \
    usermod --shell /bin/bash builder

USER builder