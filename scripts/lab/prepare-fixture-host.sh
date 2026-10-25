#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/guest.sh"
lab_copy tidemq-fixtures "$lab_state/tools/rustfs" /tmp/tidemq-rustfs
lab_ssh tidemq-fixtures sudo install -m 0755 /tmp/tidemq-rustfs /usr/local/bin/rustfs
lab_ssh tidemq-fixtures sudo install -d -m 0700 /etc/tidemq-lab/fixture-tls
for suffix in pem key; do
  lab_copy tidemq-fixtures "$lab_state/private/tls/fixtures.$suffix" "/tmp/tidemq-fixtures.$suffix"
  lab_ssh tidemq-fixtures sudo install -m 0600 "/tmp/tidemq-fixtures.$suffix" "/etc/tidemq-lab/fixture-tls/server.$suffix"
  lab_ssh tidemq-fixtures rm "/tmp/tidemq-fixtures.$suffix"
done
lab_copy tidemq-fixtures "$lab_repo/scripts/lab/fixture-runtime.sh" /tmp/tidemq-fixture-runtime.sh
lab_ssh tidemq-fixtures sudo install -m 0755 /tmp/tidemq-fixture-runtime.sh /usr/local/sbin/tidemq-fixtures
lab_ssh tidemq-fixtures rm /tmp/tidemq-rustfs /tmp/tidemq-fixture-runtime.sh
printf 'Prepared the isolated fixture guest; no shared-service credentials were copied.\n'
