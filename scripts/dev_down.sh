#!/usr/bin/env bash
# Delete the local k3d cluster created by dev_up.sh.

set -euo pipefail

CLUSTER="eirvah-edge"

if k3d cluster list 2>/dev/null | awk 'NR>1{print $1}' | grep -qx "${CLUSTER}"; then
  echo "==> deleting k3d cluster '${CLUSTER}'"
  k3d cluster delete "${CLUSTER}"
else
  echo "==> no cluster named '${CLUSTER}' found — nothing to do"
fi
