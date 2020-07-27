FROM rust:latest as builder
RUN apt-get update
RUN cd /tmp && USER=root cargo new --bin dt-client
WORKDIR /tmp/dt-client

# Build Rust skeleton project, caching dependencies, before building.
COPY Cargo.toml Cargo.lock ./
RUN touch build.rs && echo "fn main() {println!(\"cargo:rerun-if-changed=\\\"/tmp/dt-client/build.rs\\\"\");}" >> build.rs
RUN cargo build --release

# Force the build.rs script to run by modifying it
RUN echo " " >> build.rs
COPY ./src ./src
RUN cargo build --release

# Push built release to slim container
FROM debian:buster-slim
RUN apt-get update
RUN apt-get install libssl-dev -y
COPY --from=builder /tmp/dt-client/target/release/dt-client /usr/local/bin/dt-client
COPY topic_names.txt topic_names.txt
CMD [ "dt-client" ]