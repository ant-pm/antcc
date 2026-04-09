FROM --platform=$BUILDPLATFORM rust:1.94-slim AS builder
ARG TARGETARCH
WORKDIR /build
RUN apt-get update && apt-get install -y pkg-config libssl-dev gcc-aarch64-linux-gnu gcc-x86-64-linux-gnu
RUN case "$TARGETARCH" in \
        amd64) echo "x86_64-unknown-linux-gnu" > /target ;; \
        arm64) echo "aarch64-unknown-linux-gnu" > /target ;; \
    esac
RUN rustup target add $(cat /target)
COPY .cargo .cargo
COPY Cargo.toml .
COPY src ./src
RUN cargo build --release --target $(cat /target)
RUN cp target/$(cat /target)/release/antcc-executor /antcc-executor

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*
COPY --from=builder /antcc-executor /usr/local/bin/antcc-executor
CMD ["antcc-executor"]
