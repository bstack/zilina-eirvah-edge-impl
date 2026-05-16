#!/usr/bin/env bash
# Create the local k3d cluster, build images, import, and apply the local overlay.
# Idempotent: safe to re-run.

set -euo pipefail

cd "$(dirname "$0")/.."

CLUSTER="eirvah-edge"
NAMESPACE="eirvah-edge"
SERVICES=(opcua-simulator)

# 1. Cluster
if ! k3d cluster list 2>/dev/null | awk 'NR>1{print $1}' | grep -qx "${CLUSTER}"; then
  echo "==> creating k3d cluster '${CLUSTER}'"
  k3d cluster create "${CLUSTER}" --wait
else
  echo "==> k3d cluster '${CLUSTER}' already exists"
fi

# 2. Build + import images
./scripts/build_all.sh local
for svc in "${SERVICES[@]}"; do
  echo "==> importing ${svc}:local into cluster"
  k3d image import "${svc}:local" --cluster "${CLUSTER}"
done

# 3. Apply manifests
echo "==> applying deploy/k3s/overlays/local"
kubectl apply -k deploy/k3s/overlays/local

# 4. Wait for readiness
echo "==> waiting for all deployments to become Available (up to 3 min)"
kubectl -n "${NAMESPACE}" wait --for=condition=Available --timeout=180s deployment --all

# 5. Hints
echo ""
echo "==> stack is up."
echo "    Grafana:     kubectl -n ${NAMESPACE} port-forward svc/grafana 3000:3000"
echo "    Prometheus:  kubectl -n ${NAMESPACE} port-forward svc/prometheus 9090:9090"
echo "    OPC UA:      kubectl -n ${NAMESPACE} port-forward svc/opcua-simulator 4840:4840"
echo "    Mosquitto:   kubectl -n ${NAMESPACE} port-forward svc/mosquitto 1883:1883"
echo "    RabbitMQ:    kubectl -n ${NAMESPACE} port-forward svc/rabbitmq 15672:15672"
echo "    Credentials: admin / eirvah-dev-grafana (Grafana)"
echo ""
echo "    NOTE: Before first run, regenerate the Mosquitto password hash:"
echo "    docker run --rm eclipse-mosquitto:2 mosquitto_passwd -c -b /tmp/p eirvah eirvah-dev-password"
echo "    Then update deploy/k3s/base/mosquitto/secret.yaml with the output."
