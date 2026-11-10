ARG RUNTIME_IMAGE
FROM ${RUNTIME_IMAGE}
USER 0:0
COPY ubuntu-archive-ca.crt /tmp/ubuntu-archive-ca.crt
RUN chmod 0644 /tmp/ubuntu-archive-ca.crt \
    && sed -i -E 's@https?://(archive|security)\.ubuntu\.com/ubuntu/?@https://mirrors.aliyun.com/ubuntu@g' /etc/apt/sources.list.d/ubuntu.sources \
    && timeout --kill-after=5s 240s apt-get \
        -o APT::Update::Error-Mode=any -o Acquire::Retries=2 -o Acquire::https::Timeout=20 \
        -o Acquire::https::CaInfo=/tmp/ubuntu-archive-ca.crt update \
    && timeout --kill-after=5s 240s apt-get \
        -o Acquire::Retries=2 -o Acquire::https::Timeout=20 \
        -o Acquire::https::CaInfo=/tmp/ubuntu-archive-ca.crt \
        install -y --no-install-recommends ca-certificates curl libatomic1 libgcc-s1 libssl3t64 libstdc++6 openssl passwd \
    && rm -f /tmp/ubuntu-archive-ca.crt \
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
COPY --chown=10001:10001 console/ /opt/tidemq/console/
RUN test -s /opt/tidemq/console/index.html \
    && test -s /opt/tidemq/console/.vite/manifest.json \
    && chmod -R a-w /opt/tidemq/console
USER 10001:10001
WORKDIR /var/lib/tidemq
CMD ["/usr/local/bin/tidemq", "--help"]
