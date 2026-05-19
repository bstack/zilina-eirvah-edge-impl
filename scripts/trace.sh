#!/usr/bin/env bash
# Search all pod logs for a correlation_id and print matching lines sorted by timestamp.
# Usage: scripts/trace.sh <correlation_id> [namespace]
set -euo pipefail

CORRELATION_ID="${1:?Usage: trace.sh <correlation_id> [namespace]}"
NAMESPACE="${2:-eirvah-edge}"

kubectl -n "${NAMESPACE}" get pods \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | \
while IFS= read -r pod; do
  kubectl -n "${NAMESPACE}" logs "${pod}" \
    --since=1h \
    --all-containers=true \
    --prefix=true \
    2>/dev/null | grep "${CORRELATION_ID}" || true
done | sort
