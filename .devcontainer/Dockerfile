FROM debian:bullseye

ENV TERM xterm-256color

RUN apt-get update -yq && \
    apt-get install -yq locales sudo unzip file curl clang openjdk-11-jdk-headless libssl-dev make git ripgrep && \
    curl -sSLo /tmp/lsd.deb https://github.com/Peltoche/lsd/releases/download/0.21.0/lsd_0.21.0_amd64.deb && \
    dpkg -i /tmp/lsd.deb && \
    printf 'en_US.UTF-8 UTF-8\n' > /etc/locale.gen && \
    locale-gen && \
    rm -rf /var/lib/apt/lists/*

ENV LANGUAGE en_US
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain stable && \
    mkdir -p /opt/rust && \
    rm -f /root/.bashrc && \
    rm -rf /root/.cargo/git && \
    rm -rf /root/.cargo/registry && \
    mv /root/.cargo /opt/rust/cargo && \
    mv /root/.rustup /opt/rust/rustup && \
    echo '. "/opt/rust/cargo/env"' >> /root/.bashrc

RUN ln -s $(readlink -f `which javac` | sed "s:/bin/javac::") /opt/java

ENV JAVA_HOME /opt/java
ENV LD_LIBRARY_PATH /opt/java/lib/server
ENV RUSTUP_HOME /opt/rust/rustup
ENV CARGO_HOME /opt/rust/cargo
ENV PATH /opt/rust/cargo/bin:/opt/java/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

VOLUME [ "/workspace", "/opt/rust/cargo/registry", "/opt/rust/cargo/git" ]

RUN curl -L --output cargo-binstall.tgz https://github.com/cargo-bins/cargo-binstall/releases/download/v0.19.3/cargo-binstall-x86_64-unknown-linux-gnu.tgz && \
    tar xf cargo-binstall*.tgz -C /opt/rust/cargo/bin
RUN cargo binstall taplo-cli mdbook --no-confirm

RUN curl -qL https://github.com/rui314/mold/releases/download/v1.2.1/mold-1.2.1-$(uname -m)-linux.tar.gz -o /tmp/mold.tar.gz && \
    tar --strip-components=1 -xzf /tmp/mold.tar.gz -C /usr/local && \
    rm -rf /tmp/mold.tar.gz && \
    ln -sf /usr/local/bin/mold /usr/bin/$(uname -m)-linux-gnu-ld