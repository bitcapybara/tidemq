#!/usr/bin/env bash
set -euo pipefail
[[ $EUID == 0 && $# == 1 && -f "$1" ]] || { printf 'Usage: sudo register-runner.sh private-token-file\n' >&2; exit 2; }
token_file=$1
cd /srv/tidemq/actions-runner
if [[ ! -f .runner ]]; then
  runuser -u actions -- ./config.sh --unattended --url https://github.com/bitcapybara/tidemq \
    --token "$(cat "$token_file")" --name tidemq-dev-u126-8 --labels tidemq-dev --work /srv/tidemq/work --disableupdate
fi
rm -f "$token_file"
ln -sf /srv/tidemq/actions-runner/externals/node24/bin/node /usr/local/bin/node
printf '%s\n' '/opt/tidemq-rust/cargo/bin:/srv/tidemq/tools:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin' > .path
chown actions:actions .path
if [[ ! -f .service ]]; then ./svc.sh install actions; fi
./svc.sh start
./svc.sh status
