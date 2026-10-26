#!/usr/bin/env bash
set -euo pipefail
umask 077
cp -R /opt/actions-runner "$HOME/runner"
cd "$HOME/runner"
if [[ "$#" == 1 && "$1" == register ]]; then
  [[ ! -e /state/.runner ]] || { printf 'Runner is already registered\n' >&2; exit 1; }
  [[ -w /state ]] || { printf 'Registration state must be writable during registration\n' >&2; exit 1; }
  ./config.sh --unattended --url https://github.com/bitcapybara/tidemq \
    --token "$(cat /run/registration.token)" --name tidemq-ci-container-u126-8 \
    --labels tidemq-ci-container --work /srv/tidemq/work --disableupdate
  for file in .runner .credentials .credentials_rsaparams; do install -m 0600 "$file" /state/; done
  exit
fi
[[ $# == 0 && -f /state/.runner ]] || { printf 'Register the container runner first\n' >&2; exit 2; }
for file in .runner .credentials .credentials_rsaparams; do ln -s "/state/$file" "$file"; done
ln -s /diag _diag
exec ./run.sh
