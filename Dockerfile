FROM rust:1.98-alpine
WORKDIR /usr/src/app
COPY . .
RUN cargo build --release
CMD ["/usr/src/app/target/release/server", "--host", "0.0.0.0"]
