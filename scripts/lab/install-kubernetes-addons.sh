#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/guest.sh"
lab_copy tidemq-cp-a "$lab_state/tools/helm" /tmp/tidemq-helm
lab_ssh tidemq-cp-a sudo install -m 0755 /tmp/tidemq-helm /usr/local/bin/helm
lab_ssh tidemq-cp-a sudo env KUBECONFIG=/etc/kubernetes/admin.conf helm upgrade --install cilium cilium \
  --repo https://helm.cilium.io/ --version "$(jq -er .versions.cilium "$lab_inventory")" \
  --namespace kube-system --set ipam.mode=kubernetes --set kubeProxyReplacement=false \
  --set routingMode=tunnel --set tunnelProtocol=vxlan --set devices=data0 --set operator.replicas=2 \
  --set l7Proxy=false --set envoy.enabled=false --set prometheus.enabled=true --set operator.prometheus.enabled=true \
  --set resources.requests.cpu=100m --set resources.requests.memory=128Mi --set resources.limits.memory=512Mi \
  --wait --timeout 10m
lab_ssh tidemq-cp-a sudo env KUBECONFIG=/etc/kubernetes/admin.conf helm upgrade --install metallb metallb \
  --repo https://metallb.github.io/metallb --version "$(jq -er .versions.metallb "$lab_inventory")" \
  --namespace metallb-system --create-namespace --set speaker.frr.enabled=false --wait --timeout 10m
file="$lab_state/generated/load-balancers.yaml"
cat > "$file" <<EOF
apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata: {name: tidemq, namespace: metallb-system}
spec: {addresses: ['$(jq -er .kubernetes.loadBalancerPool "$lab_inventory")']}
---
apiVersion: metallb.io/v1beta1
kind: L2Advertisement
metadata: {name: tidemq, namespace: metallb-system}
spec:
  ipAddressPools: [tidemq]
  interfaces: [data0]
---
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata: {name: tidemq-local}
provisioner: kubernetes.io/no-provisioner
volumeBindingMode: WaitForFirstConsumer
reclaimPolicy: Retain
EOF
lab_copy tidemq-cp-a "$file" /tmp/tidemq-load-balancers.yaml
lab_ssh tidemq-cp-a sudo kubectl --kubeconfig=/etc/kubernetes/admin.conf apply -f /tmp/tidemq-load-balancers.yaml
lab_copy tidemq-cp-a "$lab_repo/deploy/lab/kube-state-metrics.yaml" /tmp/tidemq-kube-state-metrics.yaml
lab_ssh tidemq-cp-a sudo kubectl --kubeconfig=/etc/kubernetes/admin.conf apply -f /tmp/tidemq-kube-state-metrics.yaml
lab_ssh tidemq-cp-a sudo kubectl --kubeconfig=/etc/kubernetes/admin.conf -n kube-system rollout status deployment/tidemq-kube-state-metrics --timeout=5m
lab_ssh tidemq-cp-a sudo kubectl --kubeconfig=/etc/kubernetes/admin.conf wait --for=condition=Ready nodes --all --timeout=5m
lab_ssh tidemq-cp-a sudo kubectl --kubeconfig=/etc/kubernetes/admin.conf get nodes -o wide
