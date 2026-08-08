#!/usr/bin/env bash
# Delete the local kind cluster created by dev_up.sh.

set -euo pipefail

CLUSTER="eirvah-edge"

if kind get clusters 2>/dev/null | grep -qx "${CLUSTER}"; then
  echo "==> deleting kind cluster '${CLUSTER}'"
  kind delete cluster --name "${CLUSTER}"
else
  echo "==> no cluster named '${CLUSTER}' found — nothing to do"
fi
