FROM rust:1.94-slim AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y pkg-config libssl-dev musl-tools
RUN rustup target add x86_64-unknown-linux-musl
COPY Cargo.toml .
COPY src ./src
RUN cargo build --release --target x86_64-unknown-linux-musl

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/x86_64-unknown-linux-musl/release/antcc-executor /usr/local/bin/antcc-executor
CMD ["antcc-executor"]
