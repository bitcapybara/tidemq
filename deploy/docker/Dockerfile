ARG DEBIAN_IMAGE
FROM ${DEBIAN_IMAGE}
ARG DEBIAN_IMAGE

RUN case "$DEBIAN_IMAGE" in *@sha256:*) ;; *) echo 'DEBIAN_IMAGE must use a verified digest' >&2; exit 2 ;; esac \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates curl libatomic1 libgcc-s1 libssl3 libstdc++6 openssl passwd \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 tidemq \
    && useradd --uid 10001 --gid 10001 --no-create-home --home-dir /var/lib/tidemq --shell /usr/sbin/nologin tidemq \
    && install -d -m 0700 -o 10001 -g 10001 /var/lib/tidemq /var/lib/tidemq/objects

COPY --chown=10001:10001 --chmod=0555 bin/tidemq /usr/local/bin/tidemq
COPY --chown=10001:10001 --chmod=0555 bin/deploy_validation /usr/local/bin/deploy_validation

USER 10001:10001
ENV HOME=/var/lib/tidemq
WORKDIR /var/lib/tidemq
CMD ["/usr/local/bin/tidemq", "--help"]
