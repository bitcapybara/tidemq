#!/usr/bin/env bash
set -euo pipefail
umask 077
source "$(dirname -- "${BASH_SOURCE[0]}")/guest.sh"
version=$(jq -er .kubernetes.version "$lab_inventory")
vip=$(jq -er .kubernetes.apiVIP "$lab_inventory")
vip_version=$(jq -er .versions.kubeVip "$lab_inventory")
private="$lab_state/private"
[[ -f "$private/join.token" ]] || printf '%s.%s\n' "$(openssl rand -hex 3)" "$(openssl rand -hex 8)" > "$private/join.token"
[[ -f "$private/certificate.key" ]] || openssl rand -hex 32 > "$private/certificate.key"
token=$(cat "$private/join.token")
certificate_key=$(cat "$private/certificate.key")
cat > "$private/kubeadm-init.yaml" <<EOF
apiVersion: kubeadm.k8s.io/v1beta4
kind: InitConfiguration
localAPIEndpoint: {advertiseAddress: 192.168.241.11, bindPort: 6443}
bootstrapTokens: [{token: '$token', ttl: 24h}]
certificateKey: '$certificate_key'
nodeRegistration:
  name: tidemq-cp-a
  imagePullSerial: false
  criSocket: unix:///run/containerd/containerd.sock
  kubeletExtraArgs: [{name: node-ip, value: 192.168.241.11}]
---
apiVersion: kubeadm.k8s.io/v1beta4
kind: ClusterConfiguration
kubernetesVersion: $version
clusterName: tidemq-dev
controlPlaneEndpoint: $vip:6443
networking: {podSubnet: 10.244.0.0/16, serviceSubnet: 10.96.0.0/16}
apiServer:
  certSANs: [$vip, 192.168.241.11, 192.168.241.12, 192.168.241.13]
---
apiVersion: kubelet.config.k8s.io/v1beta1
kind: KubeletConfiguration
cgroupDriver: systemd
containerLogMaxSize: 10Mi
containerLogMaxFiles: 3
EOF
lab_copy tidemq-cp-a "$private/kubeadm-init.yaml" /tmp/tidemq-kubeadm.yaml
lab_copy tidemq-cp-a "$lab_repo/scripts/lab/bootstrap-control-plane.sh" /tmp/tidemq-control-plane.sh
lab_ssh tidemq-cp-a sudo bash /tmp/tidemq-control-plane.sh init "$vip_version" "$vip" > "$lab_state/logs/kubeadm-init.log" 2>&1
ca_hash=$(lab_ssh tidemq-cp-a sudo bash -c 'openssl x509 -pubkey -in /etc/kubernetes/pki/ca.crt | openssl pkey -pubin -outform DER | openssl dgst -sha256 -hex | cut -d " " -f 2')
[[ "$ca_hash" =~ ^[a-f0-9]{64}$ ]] || { printf 'Invalid Kubernetes CA fingerprint\n' >&2; exit 1; }
while IFS=$'\t' read -r name role address; do
  file="$private/$name-join.yaml"
  cat > "$file" <<EOF
apiVersion: kubeadm.k8s.io/v1beta4
kind: JoinConfiguration
discovery:
  bootstrapToken:
    apiServerEndpoint: $vip:6443
    token: '$token'
    caCertHashes: ['sha256:$ca_hash']
nodeRegistration:
  name: $name
  imagePullSerial: false
  criSocket: unix:///run/containerd/containerd.sock
  kubeletExtraArgs: [{name: node-ip, value: '$address'}]
EOF
  if [[ "$role" == control-plane ]]; then
    cat >> "$file" <<EOF
controlPlane:
  localAPIEndpoint: {advertiseAddress: $address, bindPort: 6443}
  certificateKey: '$certificate_key'
EOF
  fi
  lab_copy "$name" "$file" /tmp/tidemq-kubeadm.yaml
  lab_copy "$name" "$lab_repo/scripts/lab/bootstrap-control-plane.sh" /tmp/tidemq-control-plane.sh
  lab_ssh "$name" sudo bash /tmp/tidemq-control-plane.sh join "$vip_version" "$vip" > "$lab_state/logs/$name-join.log" 2>&1
  if [[ "$role" == control-plane ]]; then
    lab_ssh "$name" sudo bash /tmp/tidemq-control-plane.sh vip "$vip_version" "$vip" >> "$lab_state/logs/$name-join.log" 2>&1
  fi
done < <(jq -r '.nodes[] | select((.role == "control-plane" or .role == "worker") and .name != "tidemq-cp-a") | [.name,.role,.dataIP] | @tsv' "$lab_inventory")
lab_ssh tidemq-cp-a sudo cat /etc/kubernetes/admin.conf > "$private/admin.conf"
while IFS=$'\t' read -r name zone; do
  lab_ssh tidemq-cp-a sudo kubectl --kubeconfig=/etc/kubernetes/admin.conf label node "$name" "topology.kubernetes.io/zone=$zone" --overwrite
done < <(jq -r '.nodes[] | select(.role == "control-plane" or .role == "worker") | [.name,.zone] | @tsv' "$lab_inventory")
printf 'Created the managed six-node Kubernetes control plane; install CNI before readiness checks.\n'
