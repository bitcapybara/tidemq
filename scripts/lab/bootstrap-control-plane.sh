#!/usr/bin/env bash
set -euo pipefail
[[ $EUID == 0 && $# == 3 ]] || { printf 'Usage: sudo bootstrap-control-plane.sh init|join|vip version api-vip\n' >&2; exit 2; }
action=$1 version=$2 vip=$3
[[ "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ && "$vip" == 192.168.241.10 ]] || exit 2
export KUBECONFIG=/etc/kubernetes/admin.conf
image="ghcr.io/kube-vip/kube-vip:$version"
install -d -m 0700 /etc/kubernetes/manifests
if [[ "$action" == init || "$action" == vip ]]; then
  ctr -n k8s.io images pull "$image"
  ctr -n k8s.io run --rm --net-host "$image" tidemq-vip-manifest /kube-vip manifest pod \
    --interface data0 --address "$vip" --controlplane --arp --leaderElection > /etc/kubernetes/kube-vip.yaml
  if [[ "$action" == init && ! -f /etc/kubernetes/admin.conf ]]; then
    sed 's|path: /etc/kubernetes/admin.conf|path: /etc/kubernetes/super-admin.conf|' /etc/kubernetes/kube-vip.yaml > /etc/kubernetes/manifests/kube-vip.yaml
  else
    cp /etc/kubernetes/kube-vip.yaml /etc/kubernetes/manifests/kube-vip.yaml
  fi
fi
case "$action" in
  init)
    if [[ ! -f /etc/kubernetes/admin.conf ]]; then kubeadm init --config /tmp/tidemq-kubeadm.yaml --upload-certs; fi
    kubectl get --raw=/readyz
    cp /etc/kubernetes/kube-vip.yaml /etc/kubernetes/manifests/kube-vip.yaml
    ;;
  join)
    if [[ ! -f /etc/kubernetes/kubelet.conf ]]; then kubeadm join --config /tmp/tidemq-kubeadm.yaml; fi
    ;;
  vip) ;;
  *) exit 2 ;;
esac
