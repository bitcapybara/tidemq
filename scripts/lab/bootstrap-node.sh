#!/usr/bin/env bash
set -euo pipefail
[[ $EUID == 0 && $# == 2 ]] || { printf 'Usage: sudo bootstrap-node.sh kubernetes-version node-ip\n' >&2; exit 2; }
version=$1 node_ip=$2
[[ "$version" =~ ^v1\.36\.[0-9]+$ && "$node_ip" =~ ^192\.168\.241\.[0-9]+$ ]] || exit 2
export DEBIAN_FRONTEND=noninteractive
swapoff -a
sed -i '/\sswap\s/s/^/#/' /etc/fstab
cat > /etc/modules-load.d/tidemq-kubernetes.conf <<'EOF'
overlay
br_netfilter
EOF
modprobe overlay
modprobe br_netfilter
cat > /etc/sysctl.d/91-tidemq-kubernetes.conf <<'EOF'
net.bridge.bridge-nf-call-iptables=1
net.bridge.bridge-nf-call-ip6tables=1
net.ipv4.ip_forward=1
EOF
sysctl --system >/dev/null
apt-get update -qq
apt-get install -y containerd ca-certificates curl gpg conntrack socat
install -d -m 0755 /etc/containerd /etc/apt/keyrings
if [[ ! -f /etc/containerd/config.toml ]]; then containerd config default > /etc/containerd/config.toml; fi
sed -i 's/SystemdCgroup = false/SystemdCgroup = true/' /etc/containerd/config.toml
curl -fsSL --proto '=https' https://pkgs.k8s.io/core:/stable:/v1.36/deb/Release.key | gpg --dearmor --yes -o /etc/apt/keyrings/kubernetes.gpg
printf '%s\n' 'deb [signed-by=/etc/apt/keyrings/kubernetes.gpg] https://pkgs.k8s.io/core:/stable:/v1.36/deb/ /' > /etc/apt/sources.list.d/kubernetes.list
apt-get update -qq
package_version=$(apt-cache madison kubeadm | awk -v version="${version#v}-" 'index($3,version)==1 && !found {print $3; found=1}')
[[ -n "$package_version" ]] || { printf 'Pinned Kubernetes package is unavailable.\n' >&2; exit 1; }
apt-get install -y "kubelet=$package_version" "kubeadm=$package_version" "kubectl=$package_version"
apt-mark hold kubelet kubeadm kubectl
printf 'KUBELET_EXTRA_ARGS=--node-ip=%s\n' "$node_ip" > /etc/default/kubelet
install -d -m 0755 /etc/systemd/system/kubelet.service.d
cat > /etc/systemd/system/kubelet.service.d/20-tidemq-runtime.conf <<'EOF'
[Unit]
Wants=containerd.service
After=containerd.service
EOF
systemctl daemon-reload
systemctl enable --now containerd kubelet
systemctl restart containerd
printf 'Prepared node %s for Kubernetes %s\n' "$node_ip" "$version"
