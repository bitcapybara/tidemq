ARG RUNTIME_IMAGE
FROM ${RUNTIME_IMAGE}
USER 0:0
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl libatomic1 libgcc-s1 libssl3t64 libstdc++6 openssl passwd \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 tidemq \
    && useradd --uid 10001 --gid 10001 --no-create-home --home-dir /var/lib/tidemq --shell /usr/sbin/nologin tidemq \
    && install -d -m 0700 -o 10001 -g 10001 /var/lib/tidemq
COPY ca.pem /usr/local/share/ca-certificates/tidemq-lab.crt
RUN update-ca-certificates
COPY bin/collector /usr/local/bin/otelcol-contrib
RUN chmod 0555 /usr/local/bin/otelcol-contrib
COPY bin/tidemq bin/deploy_validation /usr/local/bin/
RUN chmod 0555 /usr/local/bin/tidemq /usr/local/bin/deploy_validation
USER 10001:10001
WORKDIR /var/lib/tidemq
CMD ["/usr/local/bin/tidemq", "--help"]
