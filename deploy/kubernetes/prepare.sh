#!/usr/bin/env bash
set -euo pipefail
umask 077
if [[ $# != 5 ]]; then
  printf 'Usage: %s context namespace immutable-image storage-class new-output-directory\n' "$0" >&2
  exit 2
fi
context=$1 namespace=$2 image=$3 storage_class=$4 output=$5
[[ "$namespace" =~ ^[a-z][a-z0-9-]{0,48}[a-z0-9]$ ]] || { printf 'Invalid namespace\n' >&2; exit 2; }
[[ "$image" =~ ^[a-zA-Z0-9][a-zA-Z0-9./_:@-]+$ && "$image" == *:* && "$image" != *:latest ]] || { printf 'An immutable image reference is required\n' >&2; exit 2; }
[[ "$storage_class" =~ ^[a-z0-9][a-z0-9.-]*$ ]] || { printf 'Invalid storage class\n' >&2; exit 2; }
[[ -n "$context" && ! -e "$output" ]] || { printf 'Context is required and output must not exist\n' >&2; exit 2; }
script_dir=$(cd "$(dirname "$0")" && pwd -P)
mkdir -m 0700 -p "$output"
output=$(cd "$output" && pwd -P)
kube() { kubectl --context "$context" --namespace "$namespace" "$@"; }
kubectl --context "$context" create namespace "$namespace" >/dev/null
cat > "$output/services.yaml" <<EOF
apiVersion: v1
kind: Service
metadata: {name: mq}
spec:
  selector: {app: tidemq}
  ports: [{name: quic, port: 7443, protocol: UDP}]
EOF
for node in 1 2 3 4; do
  cat >> "$output/services.yaml" <<EOF
---
apiVersion: v1
kind: Service
metadata: {name: node-$node}
spec:
  publishNotReadyAddresses: true
  selector: {tidemq-node: node-$node}
  ports: [{name: peer-quic, port: 7444, protocol: UDP}]
EOF
done
kube apply -f "$output/services.yaml" >/dev/null
"$script_dir/../local/generate.sh" rf3-combined "$output/material" >/dev/null
material="$output/material"
cluster=$(sed -n "s/^cluster_id = '\(.*\)'/\1/p" "$material/node-1.toml")
public_ip=$(kube get service mq -o jsonpath='{.spec.clusterIP}')
for node in 1 2 3 4; do
  address=$(kube get service "node-$node" -o jsonpath='{.spec.clusterIP}')
  [[ "$address" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ && "$public_ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || { printf 'This example requires IPv4 Services\n' >&2; exit 1; }
  printf '%s\n' "$address" > "$material/address-$node"
  cat > "$material/node-$node.cnf" <<EOF
[req]
distinguished_name=dn
prompt=no
[dn]
CN=node-$node.local
[extensions]
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=DNS:node-$node.local,DNS:mq.$namespace.svc,IP:$address,IP:$public_ip,IP:127.0.0.1
EOF
  openssl req -new -sha256 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -config "$material/node-$node.cnf" -keyout "$material/node-$node.key" -out "$material/node-$node.csr" >/dev/null 2>&1
  openssl x509 -req -sha256 -in "$material/node-$node.csr" -CA "$material/ca.pem" -CAkey "$material/ca.key" \
    -set_serial "$node" -days 7 -extfile "$material/node-$node.cnf" -extensions extensions -out "$material/node-$node.pem" >/dev/null 2>&1
  openssl x509 -in "$material/node-$node.pem" -outform DER -out "$material/node-$node.der"
done
for node in 1 2 3 4; do
  replicas=1 bootstrap=true
  [[ "$node" != 4 ]] || { replicas=0; bootstrap=false; }
  cat > "$material/node-$node.toml" <<EOF
node_id = $node
cluster_id = '$cluster'
role = 'combined'
management_listen = '127.0.0.1:9419'
data_dir = '/var/lib/tidemq'
replication_factor = 3
[memory]
total_bytes = 201326592
control_reserved_bytes = 33554432
[tls]
certificate = '/private/node-$node.pem'
private_key = '/private/node-$node.key'
trust_roots = '/private/ca.pem'
[authorization]
EOF
  if [[ "$node" != 4 ]]; then
    printf "bootstrap_admin_token_file = '/private/bootstrap.token'\n" >> "$material/node-$node.toml"
  fi
  cat >> "$material/node-$node.toml" <<EOF
[gateway]
listen = '0.0.0.0:7443'
[storage]
archive_interval_ms = 1000
[runtime]
internal_listen = '0.0.0.0:7444'
metadata_group = 1
metadata_voters = [1,2,3]
bootstrap = $bootstrap
[[runtime.groups]]
group_id = 2
voters = [1,2,3]
EOF
  peers='1 2 3'
  [[ "$node" != 4 ]] || peers='1 2 3 4'
  for peer in $peers; do
    address=$(cat "$material/address-$peer")
    zone=$((peer % 3))
    [[ "$peer" != 4 ]] || zone=0
    cat >> "$material/node-$node.toml" <<EOF
[[runtime.peers]]
node_id = $peer
role = 'combined'
zone = 'zone-$zone'
internal_address = '$address:7444'
server_name = 'node-$peer.local'
certificate_der = '/private/node-$peer.der'
EOF
  done
  kube create configmap "node-$node" --from-file="config.toml=$material/node-$node.toml" >/dev/null
  secret=(--from-file="ca.pem=$material/ca.pem" --from-file="node-$node.pem=$material/node-$node.pem" --from-file="node-$node.key=$material/node-$node.key")
  for peer in 1 2 3 4; do secret+=(--from-file="node-$peer.der=$material/node-$peer.der"); done
  [[ "$node" == 4 ]] || secret+=(--from-file="bootstrap.token=$material/bootstrap.token")
  kube create secret generic "node-$node" "${secret[@]}" >/dev/null
  zone=$((node % 3))
  [[ "$node" != 4 ]] || zone=0
  cat > "$output/node-$node.yaml" <<EOF
apiVersion: apps/v1
kind: StatefulSet
metadata: {name: node-$node}
spec:
  serviceName: node-$node
  replicas: $replicas
  updateStrategy: {type: OnDelete}
  persistentVolumeClaimRetentionPolicy: {whenDeleted: Retain, whenScaled: Retain}
  selector: {matchLabels: {tidemq-node: node-$node}}
  template:
    metadata: {labels: {app: tidemq, tidemq-node: node-$node}}
    spec:
      nodeSelector: {topology.kubernetes.io/zone: zone-$zone}
      automountServiceAccountToken: false
      terminationGracePeriodSeconds: 30
      securityContext: {seccompProfile: {type: RuntimeDefault}}
      initContainers:
        - name: initialize-private-files
          image: $image
          imagePullPolicy: IfNotPresent
          command: [/bin/sh, -ec]
          args:
            - 'umask 077; cp /seed/* /private/; chmod 600 /private/*; chown -R 10001:10001 /private; chmod 700 /private; install -d -m 700 -o 10001 -g 10001 /var/lib/tidemq /var/lib/tidemq/objects'
          securityContext:
            runAsUser: 0
            runAsNonRoot: false
            readOnlyRootFilesystem: true
            allowPrivilegeEscalation: false
            capabilities: {drop: [ALL], add: [CHOWN, FOWNER, DAC_OVERRIDE]}
          volumeMounts:
            - {name: seed, mountPath: /seed, readOnly: true}
            - {name: private, mountPath: /private}
            - {name: data, mountPath: /var/lib/tidemq}
      containers:
        - name: tidemq
          image: $image
          imagePullPolicy: IfNotPresent
          command: [/usr/local/bin/tidemq, serve, --config, /config/config.toml]
          env: [{name: TOKIO_WORKER_THREADS, value: '2'}]
          ports:
            - {name: quic, containerPort: 7443, protocol: UDP}
            - {name: peer-quic, containerPort: 7444, protocol: UDP}
          resources:
            requests: {cpu: 250m, memory: 384Mi}
            limits: {cpu: '2', memory: 768Mi}
          securityContext:
            runAsUser: 10001
            runAsGroup: 10001
            runAsNonRoot: true
            readOnlyRootFilesystem: true
            allowPrivilegeEscalation: false
            capabilities: {drop: [ALL]}
          readinessProbe:
            exec: {command: [curl, --fail, --silent, --max-time, '1', 'http://127.0.0.1:9419/readyz']}
            periodSeconds: 2
            timeoutSeconds: 2
          volumeMounts:
            - {name: config, mountPath: /config, readOnly: true}
            - {name: private, mountPath: /private, readOnly: true}
            - {name: data, mountPath: /var/lib/tidemq}
            - {name: temporary, mountPath: /tmp}
      volumes:
        - {name: config, configMap: {name: node-$node}}
        - {name: seed, secret: {secretName: node-$node, defaultMode: 256}}
        - {name: private, emptyDir: {medium: Memory, sizeLimit: 1Mi}}
        - {name: temporary, emptyDir: {medium: Memory, sizeLimit: 32Mi}}
  volumeClaimTemplates:
    - metadata: {name: data}
      spec:
        accessModes: [ReadWriteOnce]
        storageClassName: $storage_class
        resources: {requests: {storage: 8Gi}}
EOF
  kube apply -f "$output/node-$node.yaml" >/dev/null
done
cat > "$output/availability.yaml" <<EOF
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata: {name: tidemq}
spec:
  minAvailable: 3
  selector: {matchLabels: {app: tidemq}}
EOF
kube apply -f "$output/availability.yaml" >/dev/null
printf 'Created namespace %s; wait for node-{1,2,3} StatefulSets. Standby node-4 has zero replicas.\n' "$namespace"
printf 'Private materials and manifests: %s\n' "$output"
