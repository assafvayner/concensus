# Stage 1: Chef - install cargo-chef
FROM rust:1.94 AS chef
RUN cargo install cargo-chef
WORKDIR /app

# Stage 2: Planner - generate recipe.json
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Stage 3: Builder - cache deps, then build
FROM chef AS builder
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json -p daccord-kv-demo
COPY . .
RUN cargo build --release -p daccord-kv-demo

# Stage 4: Runtime
FROM debian:bookworm-slim
COPY --from=builder /app/target/release/daccord-kv-node /usr/local/bin/
COPY --from=builder /app/target/release/daccord-kv-cli /usr/local/bin/
ENTRYPOINT ["daccord-kv-node"]
