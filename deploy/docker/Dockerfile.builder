ARG RUST_IMAGE
FROM ${RUST_IMAGE}
ARG RUST_IMAGE

RUN case "$RUST_IMAGE" in *@sha256:*) ;; *) echo 'RUST_IMAGE must use a verified digest' >&2; exit 2 ;; esac \
    && test "$(rustc --version | cut -d ' ' -f 2)" = '1.99.1' \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential ca-certificates clang cmake curl git libssl-dev perl pkg-config \
    && rustup component add --toolchain 1.99.1 rustfmt clippy llvm-tools-preview \
    && rm -rf /var/lib/apt/lists/*

ENV RUSTUP_TOOLCHAIN=1.99.1 CARGO_BUILD_JOBS=2
WORKDIR /work
CMD ["/bin/bash"]
