FROM rust:1.64-slim-bullseye as builder

# Add Google Protocol Buffers for Libra's metrics library.
ENV PROTOC_VERSION 3.8.0
ENV PROTOC_ZIP protoc-$PROTOC_VERSION-linux-x86_64.zip

RUN set -x \
 && apt update \
 && apt install -y \
      clang \
      cmake \
      libudev-dev \
      make \
      unzip \
      libssl-dev \
      pkg-config \
      zlib1g-dev \
      curl \
 && rustup component add rustfmt \
 && rustup component add clippy \
 && rustc --version \
 && cargo --version \
 && curl -OL https://github.com/google/protobuf/releases/download/v$PROTOC_VERSION/$PROTOC_ZIP \
 && unzip -o $PROTOC_ZIP -d /usr/local bin/protoc \
 && unzip -o $PROTOC_ZIP -d /usr/local include/* \
 && rm -f $PROTOC_ZIP


WORKDIR /solana
COPY . .
RUN mkdir -p docker-output

ARG ci_commit
# NOTE: Keep this here before build since variable is referenced during CI build step.
ENV CI_COMMIT=$ci_commit

ARG debug

# Uses docker buildkit to cache the image.
# /usr/local/cargo/git needed for crossbeam patch
RUN --mount=type=cache,mode=0777,target=/solana/target \
    --mount=type=cache,mode=0777,target=/usr/local/cargo/registry \
    --mount=type=cache,mode=0777,target=/usr/local/cargo/git \
    if [ "$debug" = "false" ] ; then \
      ./cargo stable build --release && cp target/release/solana* ./docker-output; \
    else \
      RUSTFLAGS='-g -C force-frame-pointers=yes' ./cargo stable build --release && cp target/release/solana* ./docker-output; \
    fi
