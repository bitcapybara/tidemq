ARG UBUNTU_IMAGE
FROM ${UBUNTU_IMAGE}
ARG UBUNTU_IMAGE

RUN case "$UBUNTU_IMAGE" in *@sha256:*) ;; *) echo 'UBUNTU_IMAGE must use a verified digest' >&2; exit 2 ;; esac \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential ca-certificates clang cmake curl git jq libssl-dev passwd perl pkg-config procps python3 python3-yaml util-linux \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 1001 actions \
    && useradd --uid 1001 --gid 1001 --no-create-home --home-dir /home/actions --shell /usr/sbin/nologin actions \
    && install -d -m 0700 -o 1001 -g 1001 /home/actions /work

USER 1001:1001
ENV HOME=/home/actions
WORKDIR /work
CMD ["/usr/bin/bash"]
