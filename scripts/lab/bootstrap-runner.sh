#!/usr/bin/env bash
set -euo pipefail
[[ $EUID == 0 && $# == 1 ]] || { printf 'Usage: sudo bootstrap-runner.sh bundle-directory\n' >&2; exit 2; }
bundle=$1
[[ $(cat /etc/tidemq-lab-role) == runner ]] || exit 1
export DEBIAN_FRONTEND=noninteractive
apt-get install -y cmake
mountpoint -q /srv/tidemq || exit 1
id actions >/dev/null 2>&1 || useradd --create-home --shell /bin/bash --groups docker actions
install -d -m 0755 -o actions -g actions /srv/tidemq/actions-runner /srv/tidemq/work /srv/tidemq/tools /srv/tidemq/cargo-target /srv/tidemq/tmp
install -d -m 0700 -o actions -g actions /home/actions/.config/tidemq-lab /home/actions/.ssh
if [[ ! -x /srv/tidemq/actions-runner/config.sh ]]; then
  tar -xzf "$bundle/runner.archive" -C /srv/tidemq/actions-runner
  chown -R actions:actions /srv/tidemq/actions-runner
fi
/srv/tidemq/actions-runner/bin/installdependencies.sh
for name in rustfs collector collectorTest helm kubectl; do
  install -m 0755 "$bundle/$name" "/srv/tidemq/tools/$name"
done
install -m 0755 "$bundle/nextest" /srv/tidemq/tools/cargo-nextest
install -m 0755 "$bundle/llvmCov" /srv/tidemq/tools/cargo-llvm-cov
ln -sfn /srv/tidemq/actions-runner/externals/node24/bin/node /usr/local/bin/node
if [[ -f "$bundle/rust-toolchain.tar.gz" ]]; then
  [[ ! -e /opt/tidemq-rust || -L /opt/tidemq-rust ]] || { printf 'Migrate the existing toolchain onto the runner data disk first\n' >&2; exit 1; }
  install -d -m 0755 /srv/tidemq/rust
  tar -xzf "$bundle/rust-toolchain.tar.gz" -C /srv/tidemq/rust
  chown -R actions:actions /srv/tidemq/rust
  [[ -L /opt/tidemq-rust ]] || ln -s /srv/tidemq/rust /opt/tidemq-rust
fi
cat > /etc/profile.d/tidemq-runner.sh <<'EOF'
export RUSTUP_HOME=/opt/tidemq-rust/rustup
export CARGO_HOME=/opt/tidemq-rust/cargo
export PATH=/opt/tidemq-rust/cargo/bin:/srv/tidemq/tools:$PATH
EOF
cat > /srv/tidemq/actions-runner/.env <<'EOF'
RUSTUP_HOME=/opt/tidemq-rust/rustup
CARGO_HOME=/opt/tidemq-rust/cargo
CARGO_BUILD_JOBS=2
CARGO_TARGET_DIR=/srv/tidemq/cargo-target/debug
CARGO_INCREMENTAL=0
CARGO_PROFILE_DEV_DEBUG=0
CARGO_PROFILE_TEST_DEBUG=0
TIDEMQ_TEST_RUSTFS_BIN=/srv/tidemq/tools/rustfs
TIDEMQ_OTELCOL_BIN=/srv/tidemq/tools/collectorTest
TIDEMQ_LAB_HOME=/home/actions/.config/tidemq-lab
TMPDIR=/srv/tidemq/tmp
EOF
printf '%s\n' '/opt/tidemq-rust/cargo/bin:/srv/tidemq/tools:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin' > /srv/tidemq/actions-runner/.path
chown actions:actions /srv/tidemq/actions-runner/.env /srv/tidemq/actions-runner/.path
printf 'Installed runner files; register the repository runner with a short-lived token.\n'
