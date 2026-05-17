#!/usr/bin/env bash
# Create the local kind cluster, build images, import them,
# and apply the local overlay.
# Idempotent: safe to re-run.

set -euo pipefail

cd "$(dirname "$0")/.."

CLUSTER="eirvah-edge"
NAMESPACE="eirvah-edge"
SERVICES=(opcua-simulator)

# 1. Cluster
if ! kind get clusters 2>/dev/null | grep -qx "${CLUSTER}"; then
  echo "==> creating kind cluster '${CLUSTER}'"
  kind create cluster --name "${CLUSTER}" --wait 60s
else
  echo "==> kind cluster '${CLUSTER}' already exists"
fi

# 2. Build + import images
./scripts/build_all.sh local

for svc in "${SERVICES[@]}"; do
  echo "==> loading ${svc}:local into kind cluster"
  kind load docker-image "${svc}:local" --name "${CLUSTER}"
done

# 3. Apply manifests
echo "==> applying deploy/k3s/overlays/local"
kubectl apply -k deploy/k3s/overlays/local

# 4. Wait for readiness
echo "==> waiting for all deployments to become Available (up to 3 min)"
kubectl -n "${NAMESPACE}" wait \
  --for=condition=Available \
  --timeout=180s \
  deployment --all

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