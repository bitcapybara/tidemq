#!/usr/bin/env bash
set -euo pipefail
umask 077
[[ $EUID == 0 && $(cat /etc/tidemq-lab-role) == runner && $(id -u actions) == 1001 ]] || exit 2
[[ $(nproc) -ge 8 && $(awk '/MemTotal/{print $2}' /proc/meminfo) -ge 15000000 ]] || { printf 'Resize the idle runner VM to 8 vCPU / 16 GiB first\n' >&2; exit 1; }
if pgrep -u actions -f '[R]unner.Worker' >/dev/null; then printf 'A runner is busy\n' >&2; exit 1; fi
cd /srv/tidemq/container-runner/source
root=/srv/tidemq/container-runner
for directory in state diag work target tmp; do install -d -m 0700 -o actions -g actions "$root/$directory"; done
touch /srv/tidemq/maintenance/cargo-cache.lock
chown actions:actions /srv/tidemq/maintenance/cargo-cache.lock
chmod 0660 /srv/tidemq/maintenance/cargo-cache.lock
compose=(docker compose -f deploy/lab/container-runner/compose.yaml)
case "$1" in
  build)
    context="$root/build"
    rm -rf -- "$context"
    install -d -m 0700 "$context"
    install -d -m 0755 "$context/runner"
    cp deploy/lab/container-runner/{Dockerfile,entrypoint.sh,hook.sh} "$context/"
    cp scripts/lab/runner-maintenance.sh deploy/lab/runner-maintenance.env "$context/"
    [[ $(/srv/tidemq/actions-runner/bin/Runner.Listener --version) == 2.338.1 ]]
    # Only public installation files enter the image; registration state stays separate.
    cp /srv/tidemq/actions-runner/{config.sh,env.sh,run.sh,run-helper.sh.template} "$context/runner/"
    cp /srv/tidemq/actions-runner/bin/installdependencies.sh "$context/"
    cp /etc/ssl/certs/ca-certificates.crt "$context/"
    base=$(jq -er .docker.runtimeBaseSource deploy/lab/inventory.json)
    docker build --build-arg "UBUNTU_IMAGE=$base" -t localhost/tidemq-ci-runner:2.338.1-rust1.99.1 "$context"
    rm -rf -- "$context"
    ;;
  register)
    chown actions:actions "$root/registration.token"
    chmod 0400 "$root/registration.token"
    TIDEMQ_RUNNER_STATE_MODE=rw "${compose[@]}" run --rm --no-deps \
      -v "$root/registration.token:/run/registration.token:ro" runner register
    rm "$root/registration.token"
    ;;
  start)
    service=actions.runner.bitcapybara-tidemq.tidemq-dev-u126-8.service
    install -d -m 0755 "/etc/systemd/system/$service.d"
    printf '[Service]\nCPUQuota=400%%\nMemoryMax=6G\n' > "/etc/systemd/system/$service.d/resources.conf"
    systemctl daemon-reload
    "${compose[@]}" up --detach --force-recreate
    systemctl enable --now "$service"
    docker inspect tidemq-ci-runner --format '{{.Id}} {{.Image}} CPU={{.HostConfig.NanoCpus}} Memory={{.HostConfig.Memory}} ReadOnly={{.HostConfig.ReadonlyRootfs}}'
    ;;
  *) exit 2 ;;
esac
