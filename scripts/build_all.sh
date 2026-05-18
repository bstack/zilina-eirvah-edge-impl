#!/usr/bin/env bash
# Build every service image with the local Docker daemon.
# Usage: scripts/build_all.sh [<tag>]   (default tag: local)

set -euo pipefail

cd "$(dirname "$0")/.."
TAG="${1:-local}"
SERVICES=(
  opcua-simulator
  opcua-data-subscriber
  data-converter
  uns-auto-contextualizer
  mqtt-uns-publisher
  uns-contextualizer-orchestrator
)

for svc in "${SERVICES[@]}"; do
  echo "==> building ${svc}:${TAG}"
  docker build \
    --file "services/${svc}/Dockerfile" \
    --tag "${svc}:${TAG}" \
    .
done
