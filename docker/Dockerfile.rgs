FROM rust:1.70

WORKDIR /usr/src/app

COPY Cargo.toml Cargo.toml
COPY src/ src/

RUN cargo install --path .
