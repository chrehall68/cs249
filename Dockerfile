FROM rust:1.98-alpine AS builder
WORKDIR /usr/src/app
COPY . .
RUN cargo build --release
FROM debian:buster-slim
WORKDIR /usr/src/app
COPY --from=builder /usr/src/app/target/release/server .
COPY --from=builder /usr/src/app/target/release/client .

