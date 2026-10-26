ARG UBUNTU_IMAGE
FROM $UBUNTU_IMAGE
ENV DEBIAN_FRONTEND=noninteractive
COPY ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
RUN chmod 0644 /etc/ssl/certs/ca-certificates.crt \
    && sed -i -E 's@https?://(archive|security).ubuntu.com/ubuntu/?@https://mirrors.aliyun.com/ubuntu@g' /etc/apt/sources.list.d/ubuntu.sources \
    && apt-get -o APT::Update::Error-Mode=any -o Acquire::Retries=3 -o Acquire::https::Timeout=30 update && apt-get install -y --no-install-recommends \
    build-essential ca-certificates clang cmake curl git libssl-dev perl pkg-config procps util-linux \
    && rm -rf /var/lib/apt/lists/*
COPY runner /opt/actions-runner
COPY installdependencies.sh /opt/installdependencies.sh
RUN bash /opt/installdependencies.sh && chmod -R a+rX /opt/actions-runner \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 1001 actions && useradd --uid 1001 --gid 1001 --no-create-home actions
COPY runner-maintenance.sh runner-maintenance.env hook.sh entrypoint.sh /opt/tidemq-runner/
RUN chmod 0555 /opt/tidemq-runner/*.sh && chmod 0444 /opt/tidemq-runner/runner-maintenance.env \
    && ln -s hook.sh /opt/tidemq-runner/before && ln -s hook.sh /opt/tidemq-runner/after && ln -s hook.sh /opt/tidemq-runner/idle \
    && ln -s /opt/actions-runner/externals/node24/bin/node /usr/local/bin/node
ENV HOME=/home/actions RUSTUP_HOME=/opt/tidemq-rust/rustup CARGO_HOME=/opt/tidemq-rust/cargo \
    RUSTUP_TOOLCHAIN=1.99.1 PATH=/opt/tidemq-rust/cargo/bin:/srv/tidemq/tools:/usr/local/bin:/usr/bin:/bin \
    TMPDIR=/srv/tidemq/tmp CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 \
    CARGO_TARGET_DIR=/srv/tidemq/cargo-target/coverage CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 \
    ACTIONS_RUNNER_HOOK_JOB_STARTED=/opt/tidemq-runner/before \
    ACTIONS_RUNNER_HOOK_JOB_COMPLETED=/opt/tidemq-runner/after
USER 1001:1001
WORKDIR /home/actions
ENTRYPOINT ["/opt/tidemq-runner/entrypoint.sh"]
