FROM rust:1.97.1-slim-bookworm@sha256:2775a09d208ff0d7c1f50490c45b62db929e87ba1dcbc3f2132ac71a704bcdd3 AS build
WORKDIR /build
RUN rustup component add clippy rustfmt
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tests ./tests
RUN cargo fmt --check && cargo test --locked && cargo clippy --locked --all-targets -- -D warnings && cargo build --release --locked

FROM debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251
COPY --from=build /etc/ssl/certs /etc/ssl/certs
COPY --from=build /build/target/release/gdelt-type1 /usr/local/bin/gdelt-type1
COPY --from=build /build/target/release/gdelt-export /usr/local/bin/gdelt-export
RUN mkdir /data && chown 10001:10001 /data
USER 10001:10001
WORKDIR /data
VOLUME ["/data"]
ENTRYPOINT ["/usr/local/bin/gdelt-type1", "--archive", "/data"]
CMD ["--help"]
